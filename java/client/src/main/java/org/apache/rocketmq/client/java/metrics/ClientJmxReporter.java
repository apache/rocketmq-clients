/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.java.metrics;

import io.opentelemetry.api.common.Attributes;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers RocketMQ client metrics in the platform MBean server.
 *
 * <p>The reporter does not open a network port or depend on a Prometheus library. When enabled, applications that
 * already run a Prometheus JMX exporter can collect these MBeans. The reporter is disabled by default and can be
 * enabled before creating a client with the {@code rocketmq.client.jmx.enabled} system property. The exporter should
 * include ObjectNames matching {@code org.apache.rocketmq.client:type=message-metrics,*}.
 */
final class ClientJmxReporter {
    static final String DOMAIN = "org.apache.rocketmq.client";
    static final String TYPE = "message-metrics";
    static final String ENABLE_PROPERTY = "rocketmq.client.jmx.enabled";
    private static final Logger log = LoggerFactory.getLogger(ClientJmxReporter.class);
    private static final Pattern SAFE_OBJECT_NAME_VALUE = Pattern.compile("[\\w-%\\. \\t]*");
    private static final MBeanOperationInfo[] NO_OPERATIONS = new MBeanOperationInfo[0];

    private final ClientId clientId;
    private final MBeanServer mBeanServer;
    private final AtomicBoolean enabled;
    private final ReentrantLock registrationLock = new ReentrantLock();
    private final Map<ObjectName, ClientMetricsMBean> mBeans;
    private final Map<HistogramEnum, ConcurrentMap<Attributes, JmxHistogram>> histograms;
    private final Map<GaugeEnum, Set<Attributes>> registeredGaugeAttributes;
    private final Set<Attributes> suppressedRegistrations;
    private final Set<Attributes> warnedGaugeRemovalFailures;
    private volatile GaugeObserver gaugeObserver = GaugeObserver.EMPTY;

    ClientJmxReporter(ClientId clientId) {
        this.clientId = clientId;
        MBeanServer server = null;
        boolean initialized = false;
        try {
            if (Boolean.parseBoolean(System.getProperty(ENABLE_PROPERTY, Boolean.FALSE.toString()))) {
                server = ManagementFactory.getPlatformMBeanServer();
                initialized = true;
            }
        } catch (Throwable t) {
            log.warn("Failed to initialize the client JMX reporter, clientId={}", clientId, t);
        }
        this.mBeanServer = server;
        this.enabled = new AtomicBoolean(initialized);
        if (initialized) {
            this.mBeans = new HashMap<>();
            this.histograms = new ConcurrentHashMap<>();
            this.registeredGaugeAttributes = new EnumMap<>(GaugeEnum.class);
            this.suppressedRegistrations = ConcurrentHashMap.newKeySet();
            this.warnedGaugeRemovalFailures = ConcurrentHashMap.newKeySet();
        } else {
            this.mBeans = Collections.emptyMap();
            this.histograms = Collections.emptyMap();
            this.registeredGaugeAttributes = Collections.emptyMap();
            this.suppressedRegistrations = Collections.emptySet();
            this.warnedGaugeRemovalFailures = Collections.emptySet();
        }
    }

    boolean isEnabled() {
        return enabled.get();
    }

    void setGaugeObserver(GaugeObserver gaugeObserver) {
        this.gaugeObserver = gaugeObserver;
    }

    void record(HistogramEnum histogramType, Attributes attributes, double value) {
        if (!enabled.get() || suppressedRegistrations.contains(attributes)) {
            return;
        }
        ConcurrentMap<Attributes, JmxHistogram> series = histograms.computeIfAbsent(histogramType,
            ignored -> new ConcurrentHashMap<>());
        JmxHistogram histogram = series.get(attributes);
        if (null == histogram) {
            histogram = registerHistogram(histogramType, attributes, series);
        }
        if (null != histogram) {
            histogram.record(value);
        }
    }

    void refreshGauges() {
        if (!enabled.get()) {
            return;
        }
        GaugeObserver observer = gaugeObserver;
        try {
            Map<GaugeEnum, Set<Attributes>> observedGaugeAttributes = new EnumMap<>(GaugeEnum.class);
            for (GaugeEnum gauge : observer.getGauges()) {
                Map<Attributes, Double> values = observer.getValues(gauge);
                observedGaugeAttributes.computeIfAbsent(gauge, ignored -> new HashSet<>())
                    .addAll(values.keySet());
            }
            registrationLock.lock();
            try {
                if (!enabled.get()) {
                    return;
                }
                for (Map.Entry<GaugeEnum, Set<Attributes>> entry : observedGaugeAttributes.entrySet()) {
                    GaugeEnum gauge = entry.getKey();
                    Set<Attributes> registered = registeredGaugeAttributes.computeIfAbsent(gauge,
                        ignored -> new HashSet<>());
                    for (Attributes attributes : entry.getValue()) {
                        if (!registered.contains(attributes) && registerGaugeLocked(gauge, attributes)) {
                            registered.add(attributes);
                        }
                    }
                }
                Iterator<Map.Entry<GaugeEnum, Set<Attributes>>> gaugeIterator =
                    registeredGaugeAttributes.entrySet().iterator();
                while (gaugeIterator.hasNext()) {
                    Map.Entry<GaugeEnum, Set<Attributes>> entry = gaugeIterator.next();
                    Set<Attributes> observed = observedGaugeAttributes.getOrDefault(entry.getKey(),
                        Collections.emptySet());
                    Set<Attributes> registered = entry.getValue();
                    Iterator<Attributes> iterator = registered.iterator();
                    while (iterator.hasNext()) {
                        Attributes attributes = iterator.next();
                        if (!observed.contains(attributes) && removeGaugeLocked(entry.getKey(), attributes)) {
                            iterator.remove();
                        }
                    }
                    if (registered.isEmpty()) {
                        gaugeIterator.remove();
                    }
                }
            } finally {
                registrationLock.unlock();
            }
        } catch (RuntimeException e) {
            log.warn("Failed to refresh client JMX gauges, clientId={}", clientId, e);
        }
    }

    void shutdown() {
        enabled.set(false);
        gaugeObserver = GaugeObserver.EMPTY;
        if (null == mBeanServer) {
            return;
        }
        registrationLock.lock();
        try {
            Iterator<Map.Entry<ObjectName, ClientMetricsMBean>> iterator = mBeans.entrySet().iterator();
            while (iterator.hasNext()) {
                ObjectName objectName = iterator.next().getKey();
                try {
                    if (mBeanServer.isRegistered(objectName)) {
                        mBeanServer.unregisterMBean(objectName);
                    }
                    iterator.remove();
                } catch (JMException | RuntimeException e) {
                    log.warn("Failed to unregister client metrics MBean, objectName={}, clientId={}",
                        objectName, clientId, e);
                }
            }
            histograms.clear();
            for (Set<Attributes> registered : registeredGaugeAttributes.values()) {
                registered.clear();
            }
            suppressedRegistrations.clear();
            warnedGaugeRemovalFailures.clear();
        } finally {
            registrationLock.unlock();
        }
    }

    private JmxHistogram registerHistogram(HistogramEnum histogramType, Attributes attributes,
        ConcurrentMap<Attributes, JmxHistogram> series) {
        registrationLock.lock();
        try {
            if (!enabled.get() || suppressedRegistrations.contains(attributes)) {
                return null;
            }
            JmxHistogram existed = series.get(attributes);
            if (null != existed) {
                return existed;
            }
            JmxHistogram histogram = new JmxHistogram(histogramType.getBoundaries());
            Map<String, MetricValue> metrics = new LinkedHashMap<>();
            String prefix = histogramType.getName();
            metrics.put(prefix + "_count", new LongMetricValue(histogram.count::sum));
            metrics.put(prefix + "_sum", new DoubleMetricValue(histogram.sum::sum));
            for (int i = 0; i < histogram.boundaries.size(); i++) {
                final int bucketIndex = i;
                String boundary = formatBoundary(histogram.boundaries.get(i));
                metrics.put(prefix + "_bucket_le_" + boundary,
                    new LongMetricValue(() -> histogram.cumulativeBucketCount(bucketIndex)));
            }
            metrics.put(prefix + "_bucket_le_inf", new LongMetricValue(histogram.count::sum));
            if (!registerMetricsLocked(attributes, metrics)) {
                return null;
            }
            series.put(attributes, histogram);
            return histogram;
        } finally {
            registrationLock.unlock();
        }
    }

    private boolean registerGaugeLocked(GaugeEnum gauge, Attributes attributes) {
        if (suppressedRegistrations.contains(attributes)) {
            return false;
        }
        Map<String, MetricValue> metrics = Collections.singletonMap(gauge.getName(),
            new DoubleMetricValue(() -> getGaugeValue(gauge, attributes)));
        return registerMetricsLocked(attributes, metrics);
    }

    private boolean removeGaugeLocked(GaugeEnum gauge, Attributes attributes) {
        try {
            ObjectName objectName = objectName(attributes);
            ClientMetricsMBean mBean = mBeans.get(objectName);
            if (null == mBean) {
                warnedGaugeRemovalFailures.remove(attributes);
                return true;
            }
            mBean.remove(gauge.getName());
            if (!mBean.isEmpty()) {
                warnedGaugeRemovalFailures.remove(attributes);
                return true;
            }
            if (mBeanServer.isRegistered(objectName)) {
                mBeanServer.unregisterMBean(objectName);
            }
            mBeans.remove(objectName);
            warnedGaugeRemovalFailures.remove(attributes);
            return true;
        } catch (JMException | RuntimeException e) {
            if (warnedGaugeRemovalFailures.add(attributes)) {
                log.warn("Failed to remove stale client JMX gauge, gauge={}, clientId={}", gauge, clientId, e);
            }
            return false;
        }
    }

    private double getGaugeValue(GaugeEnum gauge, Attributes attributes) {
        try {
            Double value = gaugeObserver.getValues(gauge).get(attributes);
            return null == value ? 0 : value;
        } catch (RuntimeException e) {
            log.warn("Failed to read client JMX gauge, gauge={}, clientId={}", gauge, clientId, e);
            return 0;
        }
    }

    private boolean registerMetricsLocked(Attributes attributes, Map<String, MetricValue> metrics) {
        if (!enabled.get() || suppressedRegistrations.contains(attributes)) {
            return false;
        }
        try {
            ObjectName objectName = objectName(attributes);
            ClientMetricsMBean mBean = mBeans.get(objectName);
            if (null == mBean) {
                mBean = new ClientMetricsMBean();
                mBean.putAllIfAbsent(metrics);
                mBeanServer.registerMBean(mBean, objectName);
                mBeans.put(objectName, mBean);
                return true;
            }
            boolean changed = mBean.putAllIfAbsent(metrics);
            if (!changed && mBeanServer.isRegistered(objectName)) {
                return true;
            }
            if (mBeanServer.isRegistered(objectName)) {
                mBeanServer.unregisterMBean(objectName);
            }
            mBeanServer.registerMBean(mBean, objectName);
            return true;
        } catch (JMException | RuntimeException e) {
            if (suppressedRegistrations.add(attributes)) {
                log.warn("Failed to register client metrics MBean, suppress further attempts, clientId={}",
                    clientId, e);
            }
            return false;
        }
    }

    ObjectName objectName(Attributes attributes) throws JMException {
        SortedMap<String, String> labels = new TreeMap<>();
        attributes.forEach((key, value) -> {
            if (null != value && !String.valueOf(value).isEmpty()) {
                labels.put(key.getKey(), String.valueOf(value));
            }
        });
        labels.put(MetricLabels.CLIENT_ID.getKey(), clientId.toString());
        StringBuilder builder = new StringBuilder(DOMAIN).append(":type=").append(TYPE);
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            builder.append(',').append(entry.getKey()).append('=').append(sanitize(entry.getValue()));
        }
        return new ObjectName(builder.toString());
    }

    private static String sanitize(String value) {
        return SAFE_OBJECT_NAME_VALUE.matcher(value).matches() ? value : ObjectName.quote(value);
    }

    private static String formatBoundary(double value) {
        // Encode signs and decimal points as JMX-friendly suffixes, for example, -1.5 becomes n1_5.
        String boundary = BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
        return boundary.replace('-', 'n').replace('.', '_');
    }

    private interface LongValueSupplier {
        long get();
    }

    private interface DoubleValueSupplier {
        double get();
    }

    private interface MetricValue {
        Number get();

        String getType();
    }

    private static final class LongMetricValue implements MetricValue {
        private final LongValueSupplier supplier;

        private LongMetricValue(LongValueSupplier supplier) {
            this.supplier = supplier;
        }

        @Override
        public Number get() {
            return supplier.get();
        }

        @Override
        public String getType() {
            return Long.class.getName();
        }
    }

    private static final class DoubleMetricValue implements MetricValue {
        private final DoubleValueSupplier supplier;

        private DoubleMetricValue(DoubleValueSupplier supplier) {
            this.supplier = supplier;
        }

        @Override
        public Number get() {
            return supplier.get();
        }

        @Override
        public String getType() {
            return Double.class.getName();
        }
    }

    private static final class JmxHistogram {
        private final List<Double> boundaries;
        private final LongAdder count = new LongAdder();
        private final DoubleAdder sum = new DoubleAdder();
        private final LongAdder[] buckets;

        private JmxHistogram(List<Double> boundaries) {
            this.boundaries = boundaries;
            this.buckets = new LongAdder[boundaries.size() + 1];
            for (int i = 0; i < buckets.length; i++) {
                buckets[i] = new LongAdder();
            }
        }

        private void record(double value) {
            count.increment();
            sum.add(value);
            int bucket = boundaries.size();
            if (!Double.isNaN(value)) {
                for (int i = 0; i < boundaries.size(); i++) {
                    if (value <= boundaries.get(i)) {
                        bucket = i;
                        break;
                    }
                }
            }
            buckets[bucket].increment();
        }

        private long cumulativeBucketCount(int bucketIndex) {
            long result = 0;
            for (int i = 0; i <= bucketIndex; i++) {
                result += buckets[i].sum();
            }
            return result;
        }
    }

    private static final class ClientMetricsMBean implements DynamicMBean {
        private volatile Map<String, MetricValue> metrics = Collections.emptyMap();

        private boolean putAllIfAbsent(Map<String, MetricValue> additions) {
            Map<String, MetricValue> snapshot = metrics;
            boolean hasNewMetric = false;
            for (String metricName : additions.keySet()) {
                if (!snapshot.containsKey(metricName)) {
                    hasNewMetric = true;
                    break;
                }
            }
            if (!hasNewMetric) {
                return false;
            }
            Map<String, MetricValue> updated = new LinkedHashMap<>(snapshot);
            for (Map.Entry<String, MetricValue> entry : additions.entrySet()) {
                if (!updated.containsKey(entry.getKey())) {
                    updated.put(entry.getKey(), entry.getValue());
                }
            }
            metrics = Collections.unmodifiableMap(updated);
            return true;
        }

        private boolean remove(String metricName) {
            Map<String, MetricValue> snapshot = metrics;
            if (!snapshot.containsKey(metricName)) {
                return false;
            }
            Map<String, MetricValue> updated = new LinkedHashMap<>(snapshot);
            updated.remove(metricName);
            metrics = Collections.unmodifiableMap(updated);
            return true;
        }

        private boolean isEmpty() {
            return metrics.isEmpty();
        }

        @Override
        public Object getAttribute(String attribute) throws AttributeNotFoundException {
            MetricValue metric = metrics.get(attribute);
            if (null == metric) {
                throw new AttributeNotFoundException(attribute);
            }
            return metric.get();
        }

        @Override
        public AttributeList getAttributes(String[] attributes) {
            AttributeList result = new AttributeList(attributes.length);
            for (String attribute : attributes) {
                MetricValue metric = metrics.get(attribute);
                if (null != metric) {
                    result.add(new Attribute(attribute, metric.get()));
                }
            }
            return result;
        }

        @Override
        public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
            throw new AttributeNotFoundException(null == attribute ? null : attribute.getName());
        }

        @Override
        public AttributeList setAttributes(AttributeList attributes) {
            return new AttributeList();
        }

        @Override
        public Object invoke(String actionName, Object[] params, String[] signature) throws ReflectionException {
            throw new ReflectionException(new NoSuchMethodException(actionName));
        }

        @Override
        public MBeanInfo getMBeanInfo() {
            Map<String, MetricValue> snapshot = metrics;
            MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[snapshot.size()];
            int index = 0;
            for (Map.Entry<String, MetricValue> entry : snapshot.entrySet()) {
                attributes[index++] = new MBeanAttributeInfo(entry.getKey(), entry.getValue().getType(),
                    "RocketMQ client metric", true, false, false);
            }
            return new MBeanInfo(ClientMetricsMBean.class.getName(), "RocketMQ client metrics", attributes,
                null, NO_OPERATIONS, null);
        }
    }
}
