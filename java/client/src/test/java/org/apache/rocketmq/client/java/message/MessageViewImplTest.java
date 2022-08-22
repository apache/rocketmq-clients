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

import apache.rocketmq.v2.Digest;
import apache.rocketmq.v2.DigestType;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SystemProperties;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class MessageViewImplTest extends TestBase {

    @Test
    public void testFromProtobufWithCrc32() {
        final Digest digest = Digest.newBuilder().setType(DigestType.CRC32).setChecksum("9EF61F95").build();
        SystemProperties systemProperties = SystemProperties.newBuilder().setMessageType(MessageType.NORMAL)
            .setMessageId(MessageIdCodec.getInstance().nextMessageId().toString())
            .setBornHost(FAKE_HOST_0)
            .setBodyDigest(digest)
            .build();
        String topic = FAKE_TOPIC_0;
        Resource resource = Resource.newBuilder().setName(topic).build();
        final ByteString body = ByteString.copyFrom("foobar", StandardCharsets.UTF_8);
        final Message message = Message.newBuilder().setSystemProperties(systemProperties)
            .setTopic(resource).setBody(body)
            .setSystemProperties(systemProperties).build();
        MessageViewImpl messageView = MessageViewImpl.fromProtobuf(message);
        assertEquals(body.asReadOnlyByteBuffer(), messageView.getBody());
        assertEquals(topic, messageView.getTopic());
        assertFalse(messageView.isCorrupted());
    }

    @Test
    public void testFromProtobufWithWrongCrc32() {
        final Digest digest = Digest.newBuilder().setType(DigestType.CRC32).setChecksum("9EF61F96").build();
        SystemProperties systemProperties = SystemProperties.newBuilder().setMessageType(MessageType.NORMAL)
            .setMessageId(MessageIdCodec.getInstance().nextMessageId().toString())
            .setBornHost(FAKE_HOST_0)
            .setBodyDigest(digest)
            .build();
        String topic = FAKE_TOPIC_0;
        Resource resource = Resource.newBuilder().setName(topic).build();
        final ByteString body = ByteString.copyFrom("foobar", StandardCharsets.UTF_8);
        final Message message = Message.newBuilder().setSystemProperties(systemProperties)
            .setTopic(resource).setBody(body)
            .setSystemProperties(systemProperties).build();
        MessageViewImpl messageView = MessageViewImpl.fromProtobuf(message);
        assertEquals(body.asReadOnlyByteBuffer(), messageView.getBody());
        assertEquals(topic, messageView.getTopic());
        assertTrue(messageView.isCorrupted());
    }

    @Test
    public void testFromProtobufWithMd5() {
        final Digest digest = Digest.newBuilder().setType(DigestType.MD5).setChecksum(
            "3858F62230AC3C915F300C664312C63F").build();
        SystemProperties systemProperties = SystemProperties.newBuilder().setMessageType(MessageType.NORMAL)
            .setMessageId(MessageIdCodec.getInstance().nextMessageId().toString())
            .setBornHost(FAKE_HOST_0)
            .setBodyDigest(digest)
            .build();
        String topic = FAKE_TOPIC_0;
        Resource resource = Resource.newBuilder().setName(topic).build();
        final ByteString body = ByteString.copyFrom("foobar", StandardCharsets.UTF_8);
        final Message message = Message.newBuilder().setSystemProperties(systemProperties)
            .setTopic(resource).setBody(body)
            .setSystemProperties(systemProperties).build();
        MessageViewImpl messageView = MessageViewImpl.fromProtobuf(message);
        assertEquals(body.asReadOnlyByteBuffer(), messageView.getBody());
        assertEquals(topic, messageView.getTopic());
        assertFalse(messageView.isCorrupted());
    }

    @Test
    public void testFromProtobufWithWrongMd5() {
        final Digest digest = Digest.newBuilder().setType(DigestType.MD5).setChecksum(
            "3858F62230AC3C915F300C664312C63G").build();
        SystemProperties systemProperties = SystemProperties.newBuilder().setMessageType(MessageType.NORMAL)
            .setMessageId(MessageIdCodec.getInstance().nextMessageId().toString())
            .setBornHost(FAKE_HOST_0)
            .setBodyDigest(digest)
            .build();
        String topic = FAKE_TOPIC_0;
        Resource resource = Resource.newBuilder().setName(topic).build();
        final ByteString body = ByteString.copyFrom("foobar", StandardCharsets.UTF_8);
        final Message message = Message.newBuilder().setSystemProperties(systemProperties)
            .setTopic(resource).setBody(body)
            .setSystemProperties(systemProperties).build();
        MessageViewImpl messageView = MessageViewImpl.fromProtobuf(message);
        assertEquals(body.asReadOnlyByteBuffer(), messageView.getBody());
        assertEquals(topic, messageView.getTopic());
        assertTrue(messageView.isCorrupted());
    }

    @Test
    public void testFromProtobufWithSha1() {
        final Digest digest = Digest.newBuilder().setType(DigestType.SHA1).setChecksum(
            "8843D7F92416211DE9EBB963FF4CE28125932878").build();
        SystemProperties systemProperties = SystemProperties.newBuilder().setMessageType(MessageType.NORMAL)
            .setMessageId(MessageIdCodec.getInstance().nextMessageId().toString())
            .setBornHost(FAKE_HOST_0)
            .setBodyDigest(digest)
            .build();
        String topic = FAKE_TOPIC_0;
        Resource resource = Resource.newBuilder().setName(topic).build();
        final ByteString body = ByteString.copyFrom("foobar", StandardCharsets.UTF_8);
        final Message message = Message.newBuilder().setSystemProperties(systemProperties)
            .setTopic(resource).setBody(body)
            .setSystemProperties(systemProperties).build();
        MessageViewImpl messageView = MessageViewImpl.fromProtobuf(message);
        assertEquals(body.asReadOnlyByteBuffer(), messageView.getBody());
        assertEquals(topic, messageView.getTopic());
        assertFalse(messageView.isCorrupted());
    }

    @Test
    public void testFromProtobufWithWrongSha1() {
        final Digest digest = Digest.newBuilder().setType(DigestType.SHA1).setChecksum(
            "8843D7F92416211DE9EBB963FF4CE28125932879").build();
        SystemProperties systemProperties = SystemProperties.newBuilder().setMessageType(MessageType.NORMAL)
            .setMessageId(MessageIdCodec.getInstance().nextMessageId().toString())
            .setBornHost(FAKE_HOST_0)
            .setBodyDigest(digest)
            .build();
        String topic = FAKE_TOPIC_0;
        Resource resource = Resource.newBuilder().setName(topic).build();
        final ByteString body = ByteString.copyFrom("foobar", StandardCharsets.UTF_8);
        final Message message = Message.newBuilder().setSystemProperties(systemProperties)
            .setTopic(resource).setBody(body)
            .setSystemProperties(systemProperties).build();
        MessageViewImpl messageView = MessageViewImpl.fromProtobuf(message);
        assertEquals(body.asReadOnlyByteBuffer(), messageView.getBody());
        assertEquals(topic, messageView.getTopic());
        assertTrue(messageView.isCorrupted());
    }
}