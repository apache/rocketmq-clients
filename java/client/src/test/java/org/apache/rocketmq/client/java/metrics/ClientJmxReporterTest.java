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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.opentelemetry.api.common.Attributes;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.timer.Timer;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClientJmxReporterTest extends TestBase {
    private ClientJmxReporter reporter;
    private String oldEnablePropertyValue;

    @Before
    public void setUp() {
        oldEnablePropertyValue = System.getProperty(ClientJmxReporter.ENABLE_PROPERTY);
        System.setProperty(ClientJmxReporter.ENABLE_PROPERTY, Boolean.TRUE.toString());
    }

    @After
    public void tearDown() {
        if (null != reporter) {
            reporter.shutdown();
        }
        restoreEnableProperty();
    }

    @Test
    public void testHistogramRegistrationAndShutdown() throws Exception {
        reporter = new ClientJmxReporter(new ClientId());
        assertTrue(reporter.isEnabled());
        Attributes attributes = Attributes.builder()
            .put(MetricLabels.TOPIC, FAKE_TOPIC_0)
            .put(MetricLabels.INVOCATION_STATUS, InvocationStatus.SUCCESS.getName())
            .build();

        reporter.record(HistogramEnum.SEND_COST_TIME, attributes, 0.5);
        reporter.record(HistogramEnum.SEND_COST_TIME, attributes, 5);

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = reporter.objectName(attributes);
        assertTrue(server.isRegistered(objectName));
        assertEquals(2L, server.getAttribute(objectName, "rocketmq_send_cost_time_count"));
        assertEquals(5.5, (Double) server.getAttribute(objectName, "rocketmq_send_cost_time_sum"), 0.001);
        assertEquals(1L, server.getAttribute(objectName, "rocketmq_send_cost_time_bucket_le_1"));
        assertEquals(2L, server.getAttribute(objectName, "rocketmq_send_cost_time_bucket_le_5"));
        assertEquals(2L, server.getAttribute(objectName, "rocketmq_send_cost_time_bucket_le_inf"));

        reporter.shutdown();
        assertFalse(server.isRegistered(objectName));
    }

    @Test
    public void testGaugeValueIsReadFromObserver() throws Exception {
        reporter = new ClientJmxReporter(new ClientId());
        Attributes attributes = Attributes.builder()
            .put(MetricLabels.TOPIC, FAKE_TOPIC_0)
            .put(MetricLabels.CONSUMER_GROUP, FAKE_CONSUMER_GROUP_0)
            .build();
        AtomicReference<Map<Attributes, Double>> values =
            new AtomicReference<>(singletonValue(attributes, 3D));
        reporter.setGaugeObserver(new GaugeObserver() {
            @Override
            public List<GaugeEnum> getGauges() {
                return Collections.singletonList(GaugeEnum.CONSUMER_CACHED_MESSAGES);
            }

            @Override
            public Map<Attributes, Double> getValues(GaugeEnum gauge) {
                return values.get();
            }
        });
        reporter.refreshGauges();

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = reporter.objectName(attributes);
        assertEquals(3D, (Double) server.getAttribute(objectName,
            GaugeEnum.CONSUMER_CACHED_MESSAGES.getName()), 0.001);

        values.set(singletonValue(attributes, 9D));
        assertEquals(9D, (Double) server.getAttribute(objectName,
            GaugeEnum.CONSUMER_CACHED_MESSAGES.getName()), 0.001);

        values.set(Collections.emptyMap());
        assertEquals(0D, (Double) server.getAttribute(objectName,
            GaugeEnum.CONSUMER_CACHED_MESSAGES.getName()), 0.001);
    }

    @Test
    public void testMetricsWithSameLabelsShareMBean() throws Exception {
        reporter = new ClientJmxReporter(new ClientId());
        Attributes attributes = Attributes.builder()
            .put(MetricLabels.TOPIC, FAKE_TOPIC_0)
            .put(MetricLabels.CONSUMER_GROUP, FAKE_CONSUMER_GROUP_0)
            .build();
        reporter.record(HistogramEnum.DELIVERY_LATENCY, attributes, 10);
        reporter.setGaugeObserver(new GaugeObserver() {
            @Override
            public List<GaugeEnum> getGauges() {
                return Collections.singletonList(GaugeEnum.CONSUMER_CACHED_BYTES);
            }

            @Override
            public Map<Attributes, Double> getValues(GaugeEnum gauge) {
                return singletonValue(attributes, 1024D);
            }
        });
        reporter.refreshGauges();

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = reporter.objectName(attributes);
        assertEquals(1L, server.getAttribute(objectName, "rocketmq_delivery_latency_count"));
        assertEquals(1024D, (Double) server.getAttribute(objectName,
            GaugeEnum.CONSUMER_CACHED_BYTES.getName()), 0.001);
    }

    @Test
    public void testExistingMBeanIsNotReplaced() throws Exception {
        reporter = new ClientJmxReporter(new ClientId());
        Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, FAKE_TOPIC_0).build();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = reporter.objectName(attributes);
        server.registerMBean(new Timer(), objectName);
        try {
            reporter.record(HistogramEnum.SEND_COST_TIME, attributes, 1);

            assertTrue(server.isInstanceOf(objectName, Timer.class.getName()));
        } finally {
            server.unregisterMBean(objectName);
        }
    }

    @Test
    public void testDisabledByDefault() throws Exception {
        System.clearProperty(ClientJmxReporter.ENABLE_PROPERTY);
        Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, FAKE_TOPIC_0).build();
        reporter = new ClientJmxReporter(new ClientId());
        reporter.record(HistogramEnum.SEND_COST_TIME, attributes, 1);

        assertFalse(reporter.isEnabled());
        assertFalse(ManagementFactory.getPlatformMBeanServer().isRegistered(reporter.objectName(attributes)));
    }

    private void restoreEnableProperty() {
        if (null == oldEnablePropertyValue) {
            System.clearProperty(ClientJmxReporter.ENABLE_PROPERTY);
        } else {
            System.setProperty(ClientJmxReporter.ENABLE_PROPERTY, oldEnablePropertyValue);
        }
    }

    private static Map<Attributes, Double> singletonValue(Attributes attributes, double value) {
        Map<Attributes, Double> result = new HashMap<>();
        result.put(attributes, value);
        return result;
    }
}
