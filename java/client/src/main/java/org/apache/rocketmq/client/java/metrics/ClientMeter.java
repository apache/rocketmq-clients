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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientMeter {
    private static final Logger log = LoggerFactory.getLogger(ClientMeter.class);

    private final boolean enabled;
    private final Meter meter;
    private final Endpoints endpoints;
    private final SdkMeterProvider provider;
    private final ClientId clientId;
    private final ConcurrentMap<String /* histogram name */, DoubleHistogram> histogramMap;

    public ClientMeter(Meter meter, Endpoints endpoints, SdkMeterProvider provider, ClientId clientId) {
        this.enabled = true;
        this.meter = checkNotNull(meter, "meter should not be null");
        this.endpoints = checkNotNull(endpoints, "endpoints should not be null");
        this.provider = checkNotNull(provider, "provider should not be null");
        this.clientId = checkNotNull(clientId, "clientId should not be null");
        this.histogramMap = new ConcurrentHashMap<>();
    }

    private ClientMeter(ClientId clientId) {
        this.enabled = false;
        this.meter = null;
        this.endpoints = null;
        this.provider = null;
        this.clientId = checkNotNull(clientId, "clientId should not be null");
        this.histogramMap = new ConcurrentHashMap<>();
    }

    static ClientMeter disabledInstance(ClientId clientId) {
        return new ClientMeter(clientId);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void record(HistogramEnum histogramEnum, Attributes attributes, double value) {
        final DoubleHistogram histogram = histogramMap.computeIfAbsent(histogramEnum.getName(), name -> enabled ?
            meter.histogramBuilder(histogramEnum.getName()).build() : null);
        if (null == histogram) {
            return;
        }
        histogram.record(value, attributes);
    }

    public void shutdown() {
        if (!enabled) {
            return;
        }
        log.info("Begin to shutdown client meter, clientId={}, endpoints={}", clientId, endpoints);
        final CountDownLatch latch = new CountDownLatch(1);
        provider.shutdown().whenComplete(latch::countDown);
        try {
            latch.await();
            log.info("Shutdown client meter successfully, clientId={}, endpoints={}", clientId, endpoints);
        } catch (Throwable t) {
            log.error("Failed to shutdown message meter, clientId={}, endpoints={}", clientId, endpoints, t);
        }
    }

    public boolean satisfy(Metric metric) {
        if (enabled && metric.isOn() && endpoints.equals(metric.getEndpoints())) {
            return true;
        }
        return !enabled && !metric.isOn();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("enabled", enabled)
            .add("meter", meter)
            .add("endpoints", endpoints)
            .add("provider", provider)
            .add("histogramMap", histogramMap)
            .toString();
    }
}
