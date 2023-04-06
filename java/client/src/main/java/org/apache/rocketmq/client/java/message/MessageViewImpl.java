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

import static com.google.common.base.Preconditions.checkNotNull;

import apache.rocketmq.v2.Digest;
import apache.rocketmq.v2.DigestType;
import apache.rocketmq.v2.Encoding;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.SystemProperties;
import com.google.common.base.MoreObjects;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageViewImpl implements MessageView {
    private static final Logger log = LoggerFactory.getLogger(MessageViewImpl.class);

    final byte[] body;
    private final MessageId messageId;
    private final String topic;
    private final String tag;
    private final String messageGroup;
    private final Long deliveryTimestamp;
    private final Collection<String> keys;
    private final Map<String, String> properties;
    private final String bornHost;
    private final long bornTimestamp;
    private int deliveryAttempt;
    private final MessageQueueImpl messageQueue;
    private final Endpoints endpoints;
    private volatile String receiptHandle;
    private final long offset;
    private final boolean corrupted;
    private final long decodeTimestamp;
    private final Long transportDeliveryTimestamp;

    public MessageViewImpl(MessageId messageId, String topic, byte[] body, String tag, String messageGroup,
        Long deliveryTimestamp, Collection<String> keys, Map<String, String> properties,
        String bornHost, long bornTimestamp, int deliveryAttempt, MessageQueueImpl messageQueue,
        String receiptHandle, long offset, boolean corrupted,
        Long transportDeliveryTimestamp) {
        this.messageId = checkNotNull(messageId, "messageId should not be null");
        this.topic = checkNotNull(topic, "topic should not be null");
        this.body = checkNotNull(body, "body should not be null");
        this.tag = tag;
        this.messageGroup = messageGroup;
        this.deliveryTimestamp = deliveryTimestamp;
        this.keys = checkNotNull(keys, "keys should not be null");
        this.properties = checkNotNull(properties, "properties should not be null");
        this.bornHost = checkNotNull(bornHost, "bornHost should not be null");
        this.bornTimestamp = bornTimestamp;
        this.deliveryAttempt = deliveryAttempt;
        this.messageQueue = messageQueue;
        this.endpoints = null == messageQueue ? null : messageQueue.getBroker().getEndpoints();
        this.receiptHandle = checkNotNull(receiptHandle, "receiptHandle should not be null");
        this.offset = offset;
        this.corrupted = corrupted;
        this.decodeTimestamp = System.currentTimeMillis();
        this.transportDeliveryTimestamp = transportDeliveryTimestamp;
    }

    /**
     * @see MessageView#getMessageId()
     */
    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    /**
     * @see MessageView#getTopic()
     */
    @Override
    public String getTopic() {
        return topic;
    }

    /**
     * @see MessageView#getBody()
     */
    @Override
    public ByteBuffer getBody() {
        return ByteBuffer.wrap(body).asReadOnlyBuffer();
    }

    /**
     * @see MessageView#getProperties()
     */
    @Override
    public Map<String, String> getProperties() {
        return new HashMap<>(properties);
    }

    /**
     * @see MessageView#getTag()
     */
    @Override
    public Optional<String> getTag() {
        return Optional.ofNullable(tag);
    }

    /**
     * @see MessageView#getKeys()
     */
    @Override
    public Collection<String> getKeys() {
        return new ArrayList<>(keys);
    }

    /**
     * @see MessageView#getMessageGroup()
     */
    @Override
    public Optional<String> getMessageGroup() {
        return Optional.ofNullable(messageGroup);
    }

    /**
     * @see MessageView#getDeliveryTimestamp()
     */
    @Override
    public Optional<Long> getDeliveryTimestamp() {
        return Optional.ofNullable(deliveryTimestamp);
    }

    /**
     * @see MessageView#getBornHost()
     */
    @Override
    public String getBornHost() {
        return bornHost;
    }

    /**
     * @see MessageView#getBornTimestamp()
     */
    @Override
    public long getBornTimestamp() {
        return bornTimestamp;
    }

    /**
     * @see MessageView#getDeliveryAttempt()
     */
    @Override
    public int getDeliveryAttempt() {
        return deliveryAttempt;
    }

    public int incrementAndGetDeliveryAttempt() {
        return ++deliveryAttempt;
    }

    @SuppressWarnings("unused")
    public MessageQueueImpl getMessageQueue() {
        return messageQueue;
    }

    public Endpoints getEndpoints() {
        return endpoints;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public void setReceiptHandle(String receiptHandle) {
        this.receiptHandle = receiptHandle;
    }

    @SuppressWarnings("unused")
    public long getOffset() {
        return offset;
    }

    public boolean isCorrupted() {
        return corrupted;
    }

    public long getDecodeTimestamp() {
        return decodeTimestamp;
    }

    public Optional<Long> getTransportDeliveryTimestamp() {
        return Optional.ofNullable(transportDeliveryTimestamp);
    }

    public static MessageViewImpl fromProtobuf(Message message) {
        return MessageViewImpl.fromProtobuf(message, null);
    }

    public static MessageViewImpl fromProtobuf(Message message, MessageQueueImpl mq) {
        return MessageViewImpl.fromProtobuf(message, mq, null);
    }

    public static MessageViewImpl fromProtobuf(Message message, MessageQueueImpl mq,
        Long transportDeliveryTimestamp) {
        final SystemProperties systemProperties = message.getSystemProperties();
        final String topic = message.getTopic().getName();
        final MessageId messageId = MessageIdCodec.getInstance().decode(systemProperties.getMessageId());
        final Digest bodyDigest = systemProperties.getBodyDigest();
        byte[] body = message.getBody().toByteArray();
        boolean corrupted = false;
        final String checksum = bodyDigest.getChecksum();
        String expectedChecksum;
        final DigestType digestType = bodyDigest.getType();
        switch (digestType) {
            case CRC32:
                expectedChecksum = Utilities.crc32CheckSum(body);
                if (!expectedChecksum.equals(checksum)) {
                    corrupted = true;
                }
                break;
            case MD5:
                try {
                    expectedChecksum = Utilities.md5CheckSum(body);
                    if (!expectedChecksum.equals(checksum)) {
                        corrupted = true;
                    }
                } catch (NoSuchAlgorithmException e) {
                    corrupted = true;
                    log.error("MD5 is not supported unexpectedly, skip it, topic={}, messageId={}", topic,
                        messageId);
                }
                break;
            case SHA1:
                try {
                    expectedChecksum = Utilities.sha1CheckSum(body);
                    if (!expectedChecksum.equals(checksum)) {
                        corrupted = true;
                    }
                } catch (NoSuchAlgorithmException e) {
                    corrupted = true;
                    log.error("SHA-1 is not supported unexpectedly, skip it, topic={}, messageId={}", topic,
                        messageId);
                }
                break;
            default:
                log.error("Unsupported message body digest algorithm, digestType={}, topic={}, messageId={}",
                    digestType, topic, messageId);
        }
        final Encoding bodyEncoding = systemProperties.getBodyEncoding();
        switch (bodyEncoding) {
            case GZIP:
                try {
                    body = Utilities.uncompressBytesGzip(body);
                } catch (IOException e) {
                    log.error("Failed to uncompress message body, topic={}, messageId={}", topic, messageId);
                    corrupted = true;
                }
                break;
            case IDENTITY:
                break;
            default:
                log.error("Unsupported message encoding algorithm, topic={}, messageId={}, bodyEncoding={}", topic,
                    messageId, bodyEncoding);
        }

        String tag = systemProperties.hasTag() ? systemProperties.getTag() : null;
        String messageGroup = systemProperties.hasMessageGroup() ? systemProperties.getMessageGroup() : null;
        Long deliveryTimestamp = systemProperties.hasDeliveryTimestamp() ?
            Timestamps.toMillis(systemProperties.getDeliveryTimestamp()) : null;
        final ProtocolStringList keys = systemProperties.getKeysList();
        final String bornHost = systemProperties.getBornHost();
        final long bornTimestamp = Timestamps.toMillis(systemProperties.getBornTimestamp());
        final int deliveryAttempt = systemProperties.getDeliveryAttempt();
        final long offset = systemProperties.getQueueOffset();
        final Map<String, String> properties = message.getUserPropertiesMap();
        final String receiptHandle = systemProperties.getReceiptHandle();
        return new MessageViewImpl(messageId, topic, body, tag, messageGroup, deliveryTimestamp, keys, properties,
            bornHost, bornTimestamp, deliveryAttempt, mq, receiptHandle, offset, corrupted, transportDeliveryTimestamp);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("messageId", messageId)
            .add("topic", topic)
            .add("bornHost", bornHost)
            .add("bornTimestamp", bornTimestamp)
            .add("endpoints", endpoints)
            .add("deliveryAttempt", deliveryAttempt)
            .add("tag", tag)
            .add("keys", keys)
            .add("messageGroup", messageGroup)
            .add("deliveryTimestamp", deliveryTimestamp)
            .add("properties", properties)
            .toString();
    }
}
