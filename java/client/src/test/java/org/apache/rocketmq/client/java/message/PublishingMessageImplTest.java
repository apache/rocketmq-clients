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

package org.apache.rocketmq.client.java.message;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.java.impl.producer.PublishingSettings;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class PublishingMessageImplTest extends TestBase {

    @Test
    public void testGetExtraProperties() throws IOException {
        final PublishingSettings publishingSettings = fakeProducerSettings();
        final String topic = FAKE_TOPIC_0;
        final Message message = fakeMessage(topic);

        final PublishingMessageImpl publishingMessage = new PublishingMessageImpl(message, publishingSettings, true);
        final String propertyKey = "traceparent";
        final String propertyValue = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        publishingMessage.getExtraProperties().put(propertyKey, propertyValue);
        final apache.rocketmq.v2.Message pbMessage = publishingMessage.toProtobuf(fakeMessageQueueImpl(topic));
        final Map<String, String> userPropertiesMap = pbMessage.getUserPropertiesMap();
        assertEquals(propertyValue, userPropertiesMap.get(propertyKey));
    }
}
