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
import java.util.Random;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.java.impl.producer.PublishingSettings;
import org.apache.rocketmq.client.java.impl.producer.PublishingSettingsTestHelper;
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
        PublishingSettingsTestHelper.setCompressBodyThresholdBytes(settings, 1024);
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
        PublishingSettingsTestHelper.setCompressBodyThresholdBytes(settings, 1024);
        final PublishingMessageImpl publishingMessage = new PublishingMessageImpl(message, settings, false);
        final MessageQueueImpl mq = fakeMessageQueueImpl(topic);
        final apache.rocketmq.v2.Message pb = publishingMessage.toProtobuf(FAKE_NAMESPACE, mq);
        assertEquals(apache.rocketmq.v2.Encoding.IDENTITY, pb.getSystemProperties().getBodyEncoding());
        assertArrayEquals(body, pb.getBody().toByteArray());
    }

    @Test
    public void testBodyCompressedAtExactThreshold() throws IOException {
        String topic = "testTopic";
        byte[] body = new byte[1024];
        Arrays.fill(body, (byte) 'x');
        final Message message = new MessageBuilderImpl().setTopic(topic).setBody(body).build();
        final PublishingSettings settings = fakeProducerSettings();
        PublishingSettingsTestHelper.setCompressBodyThresholdBytes(settings, 1024);
        final PublishingMessageImpl publishingMessage = new PublishingMessageImpl(message, settings, false);
        final MessageQueueImpl mq = fakeMessageQueueImpl(topic);
        final apache.rocketmq.v2.Message pb = publishingMessage.toProtobuf(FAKE_NAMESPACE, mq);
        assertEquals(apache.rocketmq.v2.Encoding.GZIP, pb.getSystemProperties().getBodyEncoding());
        assertArrayEquals(body, Utilities.decompressBytes(pb.getBody().toByteArray()));
    }

    @Test
    public void testBodyNotCompressedWhenGzipInflates() throws IOException {
        String topic = "testTopic";
        // Incompressible pseudo-random body: GZIP output is larger than the input,
        // so the implementation must fall back to the identity encoding.
        byte[] body = new byte[4096];
        new Random(42).nextBytes(body);
        final Message message = new MessageBuilderImpl().setTopic(topic).setBody(body).build();
        final PublishingSettings settings = fakeProducerSettings();
        PublishingSettingsTestHelper.setCompressBodyThresholdBytes(settings, 1024);
        final PublishingMessageImpl publishingMessage = new PublishingMessageImpl(message, settings, false);
        final MessageQueueImpl mq = fakeMessageQueueImpl(topic);
        final apache.rocketmq.v2.Message pb = publishingMessage.toProtobuf(FAKE_NAMESPACE, mq);
        assertEquals(apache.rocketmq.v2.Encoding.IDENTITY, pb.getSystemProperties().getBodyEncoding());
        assertArrayEquals(body, pb.getBody().toByteArray());
    }

    @Test(expected = IOException.class)
    public void testIncompressibleBodyExceedingMaxSizeRejected() throws IOException {
        String topic = "testTopic";
        // Larger than the default 4 MiB limit and incompressible: the transport body
        // stays over the limit after the compression fallback, so it must be rejected.
        byte[] body = new byte[5 * 1024 * 1024];
        new Random(42).nextBytes(body);
        final Message message = new MessageBuilderImpl().setTopic(topic).setBody(body).build();
        final PublishingSettings settings = fakeProducerSettings();
        PublishingSettingsTestHelper.setCompressBodyThresholdBytes(settings, 1024);
        new PublishingMessageImpl(message, settings, false);
    }

    @Test
    public void testCompressibleBodyOverMaxSizeAcceptedWhenCompressedFits() throws IOException {
        String topic = "testTopic";
        // Original body exceeds the 4 MiB limit, but the compressed body fits: the
        // limit is validated against the transport (compressed) body by design.
        byte[] body = new byte[5 * 1024 * 1024];
        Arrays.fill(body, (byte) 'x');
        final Message message = new MessageBuilderImpl().setTopic(topic).setBody(body).build();
        final PublishingSettings settings = fakeProducerSettings();
        PublishingSettingsTestHelper.setCompressBodyThresholdBytes(settings, 1024);
        final PublishingMessageImpl publishingMessage = new PublishingMessageImpl(message, settings, false);
        final MessageQueueImpl mq = fakeMessageQueueImpl(topic);
        final apache.rocketmq.v2.Message pb = publishingMessage.toProtobuf(FAKE_NAMESPACE, mq);
        assertEquals(apache.rocketmq.v2.Encoding.GZIP, pb.getSystemProperties().getBodyEncoding());
        assertArrayEquals(body, Utilities.decompressBytes(pb.getBody().toByteArray()));
    }
}
