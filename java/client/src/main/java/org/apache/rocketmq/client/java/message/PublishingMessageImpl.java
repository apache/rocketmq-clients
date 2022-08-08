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

import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SystemProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.Optional;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.java.impl.producer.ProducerSettings;
import org.apache.rocketmq.client.java.message.protocol.Encoding;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;

/**
 * This class is a publishing view for message, which could be considered as an extension of {@link MessageImpl}.
 * Specifically speaking, Some work has been brought forward, e.g. message body compression, message id generation, etc.
 */
public class PublishingMessageImpl extends MessageImpl {
    private final MessageId messageId;
    private final MessageType messageType;
    private volatile String traceContext;

    public PublishingMessageImpl(Message message, ProducerSettings producerSettings, boolean txEnabled)
        throws IOException {
        super(message);
        this.traceContext = null;
        final int length = message.getBody().remaining();
        final int maxBodySizeBytes = producerSettings.getMaxBodySizeBytes();
        if (length > maxBodySizeBytes) {
            throw new IOException("Message body size exceeds the threshold, max size=" + maxBodySizeBytes + " bytes");
        }
        // Generate message id.
        this.messageId = MessageIdCodec.getInstance().nextMessageId();
        // Normal message.
        if (!message.getMessageGroup().isPresent() &&
            !message.getDeliveryTimestamp().isPresent() && !txEnabled) {
            messageType = MessageType.NORMAL;
            return;
        }
        // Fifo message.
        if (message.getMessageGroup().isPresent() && !txEnabled) {
            messageType = MessageType.FIFO;
            return;
        }
        // Delay message.
        if (message.getDeliveryTimestamp().isPresent() && !txEnabled) {
            messageType = MessageType.DELAY;
            return;
        }
        // Transaction message.
        if (!message.getMessageGroup().isPresent() &&
            !message.getDeliveryTimestamp().isPresent() && txEnabled) {
            messageType = MessageType.TRANSACTION;
            return;
        }
        // Transaction semantics is conflicted with fifo/delay.
        throw new IllegalArgumentException("Transactional message should not set messageGroup or deliveryTimestamp");
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setTraceContext(String traceContext) {
        this.traceContext = traceContext;
    }

    public Optional<String> getTraceContext() {
        return null == traceContext ? Optional.empty() : Optional.of(traceContext);
    }

    /**
     * Convert {@link PublishingMessageImpl} to protocol buffer.
     *
     * <p>This method should be invoked before each message sending, because the born time is reset before each
     * invocation, which means that it should not be invoked ahead of time.
     */
    public apache.rocketmq.v2.Message toProtobuf(MessageQueueImpl mq) {
        final apache.rocketmq.v2.SystemProperties.Builder systemPropertiesBuilder =
            apache.rocketmq.v2.SystemProperties.newBuilder()
                // Message keys
                .addAllKeys(keys)
                // Message Id
                .setMessageId(messageId.toString())
                // Born time should be reset before each sending
                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                // Born host
                .setBornHost(Utilities.hostName())
                // Body encoding
                .setBodyEncoding(Encoding.toProtobuf(Encoding.IDENTITY))
                // Queue id
                .setQueueId(mq.getQueueId())
                // Message type
                .setMessageType(MessageType.toProtobuf(messageType));
        // Message tag
        this.getTag().ifPresent(systemPropertiesBuilder::setTag);
        // Trace context
        this.getTraceContext().ifPresent(systemPropertiesBuilder::setTraceContext);
        // Delivery timestamp
        this.getDeliveryTimestamp()
            .ifPresent(millis -> systemPropertiesBuilder.setDeliveryTimestamp(Timestamps.fromMillis(millis)));
        // Message group
        this.getMessageGroup().ifPresent(systemPropertiesBuilder::setMessageGroup);
        final SystemProperties systemProperties = systemPropertiesBuilder.build();
        Resource topicResource = Resource.newBuilder().setName(getTopic()).build();
        return apache.rocketmq.v2.Message.newBuilder()
            // Topic
            .setTopic(topicResource)
            // Message body
            .setBody(ByteString.copyFrom(getBody()))
            // System properties
            .setSystemProperties(systemProperties)
            // User properties
            .putAllUserProperties(getProperties())
            .build();
    }
}
