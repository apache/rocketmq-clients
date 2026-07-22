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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.java.impl.producer.PublishingSettings;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class PublishingMessageImplTest extends TestBase {

    @Test
    public void testBodyNotCompressedByDefault() throws IOException {
        String topic = "testTopic";
        byte[] body = "foobar".getBytes(StandardCharsets.UTF_8);
        final Message message = new MessageBuilderImpl().setTopic(topic).setBody(body).build();
        final PublishingSettings settings = fakeProducerSettings();
        final PublishingMessageImpl publishingMessage = new PublishingMessageImpl(message, settings, false);
        final MessageQueueImpl mq = fakeMessageQueueImpl(topic);
        final apache.rocketmq.v2.Message pb = publishingMessage.toProtobuf(FAKE_NAMESPACE, mq);
        assertEquals(apache.rocketmq.v2.Encoding.IDENTITY, pb.getSystemProperties().getBodyEncoding());
        assertArrayEquals(body, pb.getBody().toByteArray());
    }

    @Test
    public void testBodyCompressedWhenThresholdReached() throws IOException {
        String topic = "testTopic";
        byte[] body = new byte[4096];
        Arrays.fill(body, (byte) 'x');
        final Message message = new MessageBuilderImpl().setTopic(topic).setBody(body).build();
        final PublishingSettings settings = fakeProducerSettings();
        settings.setCompressBodyThresholdBytes(1024);
        final PublishingMessageImpl publishingMessage = new PublishingMessageImpl(message, settings, false);
        final MessageQueueImpl mq = fakeMessageQueueImpl(topic);
        final apache.rocketmq.v2.Message pb = publishingMessage.toProtobuf(FAKE_NAMESPACE, mq);
        assertEquals(apache.rocketmq.v2.Encoding.GZIP, pb.getSystemProperties().getBodyEncoding());
        final byte[] transportBody = pb.getBody().toByteArray();
        assertTrue(transportBody.length < body.length);
        assertArrayEquals(body, Utilities.decompressBytes(transportBody));
    }

    @Test
    public void testBodyNotCompressedBelowThreshold() throws IOException {
        String topic = "testTopic";
        byte[] body = "foobar".getBytes(StandardCharsets.UTF_8);
        final Message message = new MessageBuilderImpl().setTopic(topic).setBody(body).build();
        final PublishingSettings settings = fakeProducerSettings();
        settings.setCompressBodyThresholdBytes(1024);
        final PublishingMessageImpl publishingMessage = new PublishingMessageImpl(message, settings, false);
        final MessageQueueImpl mq = fakeMessageQueueImpl(topic);
        final apache.rocketmq.v2.Message pb = publishingMessage.toProtobuf(FAKE_NAMESPACE, mq);
        assertEquals(apache.rocketmq.v2.Encoding.IDENTITY, pb.getSystemProperties().getBodyEncoding());
        assertArrayEquals(body, pb.getBody().toByteArray());
    }
}
