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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class GeneralMessageImplTest extends TestBase {
    @Test
    public void testMessage() {
        String topic = "testTopic";
        byte[] body = "foobar".getBytes(StandardCharsets.UTF_8);
        String tag = "tagA";
        List<String> keys = new ArrayList<>();
        keys.add("keyA");
        String messageGroup = "messageGroup0";
        long deliveryTimestamp = System.currentTimeMillis();
        Map<String, String> properties = new HashMap<>();
        properties.put("propertyA", "valueA");

        final MessageImpl message = new MessageImpl(topic, body, tag, keys, messageGroup, deliveryTimestamp,
            properties);
        final GeneralMessageImpl generalMessage = new GeneralMessageImpl(message);
        assertFalse(generalMessage.getMessageId().isPresent());
        assertEquals(topic, generalMessage.getTopic());
        assertEquals(ByteBuffer.wrap(body), generalMessage.getBody());
        assertEquals(properties, generalMessage.getProperties());
        assertTrue(generalMessage.getTag().isPresent());
        assertEquals(tag, generalMessage.getTag().get());
        assertEquals(keys, generalMessage.getKeys());
        assertTrue(generalMessage.getMessageGroup().isPresent());
        assertEquals(messageGroup, generalMessage.getMessageGroup().get());
        assertTrue(generalMessage.getDeliveryTimestamp().isPresent());
        assertEquals(deliveryTimestamp, (long) generalMessage.getDeliveryTimestamp().get());
        assertFalse(generalMessage.getBornHost().isPresent());
        assertFalse(generalMessage.getBornTimestamp().isPresent());
        assertFalse(generalMessage.getDeliveryAttempt().isPresent());
        assertFalse(generalMessage.getDecodeTimestamp().isPresent());
        assertFalse(generalMessage.getTransportDeliveryTimestamp().isPresent());
    }

    @Test
    public void testMessageView() {
        MessageId messageId = MessageIdCodec.getInstance().nextMessageId();
        String topic = "testTopic";
        byte[] body = "foobar".getBytes(StandardCharsets.UTF_8);
        String tag = "tagA";
        String messageGroup = "messageGroup0";
        long deliveryTimestamp = System.currentTimeMillis();
        List<String> keys = new ArrayList<>();
        keys.add("keyA");
        Map<String, String> properties = new HashMap<>();
        properties.put("propertyA", "valueA");
        String bornHost = "bornHost0";
        long bornTimestamp = System.currentTimeMillis();
        int deliveryAttempt = 1;
        final MessageQueueImpl mq = fakeMessageQueueImpl(topic);
        String receiptHandle = "receiptHandle0";
        long offset = 8;
        boolean corrupted = false;
        long transportDeliveryTimestamp = System.currentTimeMillis();

        final MessageViewImpl messageView = new MessageViewImpl(messageId, topic, body, tag, messageGroup,
            deliveryTimestamp, keys, properties, bornHost, bornTimestamp, deliveryAttempt, mq, receiptHandle,
            offset, corrupted, transportDeliveryTimestamp);
        final GeneralMessageImpl generalMessage = new GeneralMessageImpl(messageView);
        assertTrue(generalMessage.getMessageId().isPresent());
        assertEquals(messageId, generalMessage.getMessageId().get());
        assertEquals(topic, generalMessage.getTopic());
        assertEquals(ByteBuffer.wrap(body), generalMessage.getBody());
        assertEquals(properties, generalMessage.getProperties());
        assertTrue(generalMessage.getTag().isPresent());
        assertEquals(tag, generalMessage.getTag().get());
        assertEquals(keys, generalMessage.getKeys());
        assertTrue(generalMessage.getMessageGroup().isPresent());
        assertEquals(messageGroup, generalMessage.getMessageGroup().get());
        assertTrue(generalMessage.getDeliveryTimestamp().isPresent());
        assertEquals(deliveryTimestamp, (long) generalMessage.getDeliveryTimestamp().get());
        assertTrue(generalMessage.getBornHost().isPresent());
        assertEquals(bornHost, generalMessage.getBornHost().get());
        assertTrue(generalMessage.getBornTimestamp().isPresent());
        assertEquals(bornTimestamp, (long) generalMessage.getBornTimestamp().get());
        assertTrue(generalMessage.getDeliveryAttempt().isPresent());
        assertEquals(deliveryAttempt, (int) generalMessage.getDeliveryAttempt().get());
        assertTrue(generalMessage.getDecodeTimestamp().isPresent());
        assertEquals(messageView.getDecodeTimestamp(), (long) generalMessage.getDecodeTimestamp().get());
        assertTrue(generalMessage.getTransportDeliveryTimestamp().isPresent());
        assertEquals(transportDeliveryTimestamp, (long) generalMessage.getTransportDeliveryTimestamp().get());
    }
}