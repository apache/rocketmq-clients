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

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class ClientMeterManagerTest extends TestBase {

    @Test
    public void testResetWithMetricOn() {
        final ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        ClientId clientId = new ClientId();
        final ClientMeterManager meterManager = new ClientMeterManager(clientId, clientConfiguration);
        final Metric metric =
            new Metric(apache.rocketmq.v2.Metric.newBuilder().setOn(true).setEndpoints(fakePbEndpoints0()).build());
        meterManager.reset(metric);
        assertTrue(meterManager.isEnabled());
    }

    @Test
    public void testResetWithMetricOff() {
        final ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        ClientId clientId = new ClientId();
        final ClientMeterManager meterManager = new ClientMeterManager(clientId, clientConfiguration);
        final Metric metric =
            new Metric(apache.rocketmq.v2.Metric.newBuilder().setOn(false).setEndpoints(fakePbEndpoints0()).build());
        meterManager.reset(metric);
        assertFalse(meterManager.isEnabled());
    }
}