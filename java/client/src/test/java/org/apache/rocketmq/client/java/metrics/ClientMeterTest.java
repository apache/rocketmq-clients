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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import apache.rocketmq.v2.Endpoints;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class ClientMeterTest extends TestBase {

    @Test
    public void testShutdownWithEnabledMeter() {
        final SdkMeterProvider provider = SdkMeterProvider.builder().setResource(Resource.empty()).build();
        final OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(provider).build();
        Meter meter = openTelemetry.getMeter("test");
        final ClientId clientId = new ClientId();
        final ClientMeter clientMeter = new ClientMeter(meter, fakeEndpoints(), provider, clientId);
        assertTrue(clientMeter.isEnabled());
        clientMeter.shutdown();
    }

    @Test
    public void testShutdownWithDisabledMeter() {
        final ClientId clientId = new ClientId();
        final ClientMeter clientMeter = ClientMeter.disabledInstance(clientId);
        assertFalse(clientMeter.isEnabled());
        clientMeter.shutdown();
    }

    @Test
    public void testSatisfy() {
        final ClientId clientId = new ClientId();
        ClientMeter clientMeter = ClientMeter.disabledInstance(clientId);
        Metric metric = new Metric(apache.rocketmq.v2.Metric.newBuilder().setOn(false).build());
        assertTrue(clientMeter.satisfy(metric));

        metric = new Metric(apache.rocketmq.v2.Metric.newBuilder().setOn(true).build());
        assertTrue(clientMeter.satisfy(metric));

        final Endpoints pbEndpoints0 = fakePbEndpoints0();

        metric = new Metric(apache.rocketmq.v2.Metric.newBuilder().setOn(false).setEndpoints(pbEndpoints0).build());
        assertTrue(clientMeter.satisfy(metric));

        metric = new Metric(apache.rocketmq.v2.Metric.newBuilder().setOn(true).setEndpoints(pbEndpoints0).build());
        assertFalse(clientMeter.satisfy(metric));

        final org.apache.rocketmq.client.java.route.Endpoints endpoints =
            new org.apache.rocketmq.client.java.route.Endpoints(pbEndpoints0);

        final SdkMeterProvider provider = SdkMeterProvider.builder().setResource(Resource.empty()).build();
        final OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(provider).build();
        Meter meter = openTelemetry.getMeter("test");
        clientMeter = new ClientMeter(meter, endpoints, provider, clientId);

        metric = new Metric(apache.rocketmq.v2.Metric.newBuilder().setOn(false).build());
        assertFalse(clientMeter.satisfy(metric));

        metric = new Metric(apache.rocketmq.v2.Metric.newBuilder().setOn(true).build());
        assertFalse(clientMeter.satisfy(metric));

        metric = new Metric(apache.rocketmq.v2.Metric.newBuilder().setOn(false).setEndpoints(pbEndpoints0).build());
        assertFalse(clientMeter.satisfy(metric));

        metric = new Metric(apache.rocketmq.v2.Metric.newBuilder().setOn(true).setEndpoints(pbEndpoints0).build());
        assertTrue(clientMeter.satisfy(metric));

        final Endpoints pbEndpoints1 = fakePbEndpoints1();
        metric = new Metric(apache.rocketmq.v2.Metric.newBuilder().setOn(true).setEndpoints(pbEndpoints1).build());
        assertFalse(clientMeter.satisfy(metric));
    }
}