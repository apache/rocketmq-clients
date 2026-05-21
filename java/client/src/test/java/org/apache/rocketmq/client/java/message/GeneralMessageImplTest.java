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
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class GeneralMessageImplTest extends TestBase {

    @Test
    public void testMessageTagKeysProperty() {
        String topic = "testTopic";
        byte[] body = "foobar".getBytes(StandardCharsets.UTF_8);
        String tag = "tagA";
        String key1 = "keyA";
        String key2 = "keyB";
        String propertyKey1 = "propertyKey1";
        String propertyValue1 = "propertyValue1";
        String propertyKey2 = "propertyKey2";
        String propertyValue2 = "propertyValue2";

        List<String> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);

        Map<String, String> properties = new HashMap<>();
        properties.put(propertyKey1, propertyValue1);
        properties.put(propertyKey2, propertyValue2);

        final Message message = new MessageBuilderImpl()
            .setTopic(topic)
            .setBody(body)
            .setTag(tag)
            .setKeys(key1, key2)
            .addProperty(propertyKey1, propertyValue1)
            .addProperty(propertyKey2, propertyValue2)
            .build();

        final GeneralMessageImpl generalMessage = new GeneralMessageImpl(message);
        assertFalse(generalMessage.getMessageId().isPresent());
        assertEquals(topic, generalMessage.getTopic());
        assertEquals(ByteBuffer.wrap(body), generalMessage.getBody());
        assertTrue(generalMessage.getTag().isPresent());
        assertEquals(tag, generalMessage.getTag().get());
        assertEquals(keys, generalMessage.getKeys());
        assertFalse(generalMessage.getBornHost().isPresent());
        assertFalse(generalMessage.getBornTimestamp().isPresent());
        assertFalse(generalMessage.getDeliveryAttempt().isPresent());
        assertFalse(generalMessage.getDecodeTimestamp().isPresent());
        assertFalse(generalMessage.getTransportDeliveryTimestamp().isPresent());

        assertFalse(generalMessage.getMessageGroup().isPresent());
        assertFalse(generalMessage.getLiteTopic().isPresent());
        assertFalse(generalMessage.getDeliveryTimestamp().isPresent());

        // Verify properties
        Map<String, String> messageProperties = generalMessage.getProperties();
        assertEquals(properties.size(), messageProperties.size());
        assertEquals(properties, messageProperties);
    }

    @Test
    public void testMessageGroup() {
        String topic = "testTopic";
        byte[] body = "foobar".getBytes(StandardCharsets.UTF_8);
        String messageGroup = "messageGroup0";

        final Message message = new MessageBuilderImpl()
            .setTopic(topic)
            .setBody(body)
            .setMessageGroup(messageGroup)
            .build();

        final GeneralMessageImpl generalMessage = new GeneralMessageImpl(message);
        assertFalse(generalMessage.getMessageId().isPresent());
        assertEquals(topic, generalMessage.getTopic());
        assertEquals(ByteBuffer.wrap(body), generalMessage.getBody());
        assertFalse(generalMessage.getTag().isPresent());
        assertEquals(0, generalMessage.getKeys().size());
        assertTrue(generalMessage.getMessageGroup().isPresent());
        assertEquals(messageGroup, generalMessage.getMessageGroup().get());
        assertFalse(generalMessage.getBornHost().isPresent());
        assertFalse(generalMessage.getBornTimestamp().isPresent());
        assertFalse(generalMessage.getDeliveryAttempt().isPresent());
        assertFalse(generalMessage.getDecodeTimestamp().isPresent());
        assertFalse(generalMessage.getTransportDeliveryTimestamp().isPresent());

        assertFalse(generalMessage.getLiteTopic().isPresent());
        assertFalse(generalMessage.getDeliveryTimestamp().isPresent());
    }

    @Test
    public void testMessageLiteTopic() {
        String topic = "testTopic";
        byte[] body = "foobar".getBytes(StandardCharsets.UTF_8);
        String liteTopic = "liteTopic0";

        final Message message = new MessageBuilderImpl()
            .setTopic(topic)
            .setBody(body)
            .setLiteTopic(liteTopic)
            .build();

        final GeneralMessageImpl generalMessage = new GeneralMessageImpl(message);
        assertFalse(generalMessage.getMessageId().isPresent());
        assertEquals(topic, generalMessage.getTopic());
        assertEquals(ByteBuffer.wrap(body), generalMessage.getBody());
        assertFalse(generalMessage.getTag().isPresent());
        assertEquals(0, generalMessage.getKeys().size());
        assertFalse(generalMessage.getMessageGroup().isPresent());
        assertTrue(generalMessage.getLiteTopic().isPresent());
        assertEquals(liteTopic, generalMessage.getLiteTopic().get());
        assertFalse(generalMessage.getBornHost().isPresent());
        assertFalse(generalMessage.getBornTimestamp().isPresent());
        assertFalse(generalMessage.getDeliveryAttempt().isPresent());
        assertFalse(generalMessage.getDecodeTimestamp().isPresent());
        assertFalse(generalMessage.getTransportDeliveryTimestamp().isPresent());

        assertFalse(generalMessage.getDeliveryTimestamp().isPresent());
    }

    @Test
    public void testMessageDeliveryTimestamp() {
        String topic = "testTopic";
        byte[] body = "foobar".getBytes(StandardCharsets.UTF_8);
        long deliveryTimestamp = System.currentTimeMillis();

        final Message message = new MessageBuilderImpl()
            .setTopic(topic)
            .setBody(body)
            .setDeliveryTimestamp(deliveryTimestamp)
            .build();

        final GeneralMessageImpl generalMessage = new GeneralMessageImpl(message);
        assertFalse(generalMessage.getMessageId().isPresent());
        assertEquals(topic, generalMessage.getTopic());
        assertEquals(ByteBuffer.wrap(body), generalMessage.getBody());
        assertFalse(generalMessage.getTag().isPresent());
        assertEquals(0, generalMessage.getKeys().size());
        assertFalse(generalMessage.getMessageGroup().isPresent());
        assertFalse(generalMessage.getLiteTopic().isPresent());
        assertTrue(generalMessage.getDeliveryTimestamp().isPresent());
        assertEquals(deliveryTimestamp, (long) generalMessage.getDeliveryTimestamp().get());
        assertFalse(generalMessage.getBornHost().isPresent());
        assertFalse(generalMessage.getBornTimestamp().isPresent());
        assertFalse(generalMessage.getDeliveryAttempt().isPresent());
        assertFalse(generalMessage.getDecodeTimestamp().isPresent());
        assertFalse(generalMessage.getTransportDeliveryTimestamp().isPresent());
    }

    @Test
    public void testMessagePriority() {
        String topic = "testTopic";
        byte[] body = "foobar".getBytes(StandardCharsets.UTF_8);
        int priority = 1;

        final Message message = new MessageBuilderImpl()
            .setTopic(topic)
            .setBody(body)
            .setPriority(priority)
            .build();

        final GeneralMessageImpl generalMessage = new GeneralMessageImpl(message);
        assertFalse(generalMessage.getMessageId().isPresent());
        assertEquals(topic, generalMessage.getTopic());
        assertEquals(ByteBuffer.wrap(body), generalMessage.getBody());
        assertFalse(generalMessage.getTag().isPresent());
        assertEquals(0, generalMessage.getKeys().size());

        assertTrue(generalMessage.getPriority().isPresent());
        assertEquals(priority, (int) generalMessage.getPriority().get());

        assertFalse(generalMessage.getMessageGroup().isPresent());
        assertFalse(generalMessage.getBornHost().isPresent());
        assertFalse(generalMessage.getBornTimestamp().isPresent());
        assertFalse(generalMessage.getDeliveryAttempt().isPresent());
        assertFalse(generalMessage.getLiteTopic().isPresent());
        assertFalse(generalMessage.getDecodeTimestamp().isPresent());
        assertFalse(generalMessage.getTransportDeliveryTimestamp().isPresent());

        assertFalse(generalMessage.getDeliveryTimestamp().isPresent());
    }

    @Test
    public void testMessageView() {
        MessageId messageId = MessageIdCodec.getInstance().nextMessageId();
        String topic = "testTopic";
        byte[] body = "foobar".getBytes(StandardCharsets.UTF_8);
        String tag = "tagA";
        String messageGroup = "messageGroup0";
        String liteTopic = "liteTopic0";
        long deliveryTimestamp = System.currentTimeMillis();
        int priority = 1;
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

        final MessageViewImpl messageView = new MessageViewImpl(messageId, topic, body, tag,
            messageGroup, liteTopic,
            deliveryTimestamp, priority, keys, properties, bornHost, bornTimestamp, deliveryAttempt, mq, receiptHandle,
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
        assertTrue(generalMessage.getLiteTopic().isPresent());
        assertEquals(liteTopic, generalMessage.getLiteTopic().get());
        assertTrue(generalMessage.getDeliveryTimestamp().isPresent());
        assertEquals(deliveryTimestamp, (long) generalMessage.getDeliveryTimestamp().get());
        assertTrue(generalMessage.getPriority().isPresent());
        assertEquals(priority, (int) generalMessage.getPriority().get());
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