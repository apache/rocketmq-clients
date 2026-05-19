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

package org.apache.rocketmq.client.java.impl.producer;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Settings;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Assert;
import org.junit.Test;

public class PublishingSettingsTest extends TestBase {

    @Test
    public void testToProtobufWithClientProperties() {
        final Duration requestTimeout = Duration.ofSeconds(3);
        Map<String, String> clientProperties = new HashMap<>();
        clientProperties.put("key1", "value1");
        clientProperties.put("key2", "value2");
        final PublishingSettings publishingSettings = new PublishingSettings(
            FAKE_NAMESPACE, new ClientId(), fakeEndpoints(), fakeExponentialBackoffRetryPolicy(),
            requestTimeout, new HashSet<>(Collections.singleton(FAKE_TOPIC_0)), clientProperties);

        final Settings settings = publishingSettings.toProtobuf();

        Assert.assertEquals(ClientType.PRODUCER, settings.getClientType());
        Assert.assertEquals(Durations.fromNanos(requestTimeout.toNanos()), settings.getRequestTimeout());
        Assert.assertEquals("value1", settings.getClientPropertiesMap().get("key1"));
        Assert.assertEquals("value2", settings.getClientPropertiesMap().get("key2"));
        Assert.assertTrue(settings.hasPublishing());
    }
}
