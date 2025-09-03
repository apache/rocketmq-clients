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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;

public class GeneralMessageImpl implements GeneralMessage {
    private final String topic;
    private final MessageId messageId;
    private final byte[] body;
    private final Map<String, String> properties;
    private final String tag;
    private final Collection<String> keys;
    private final String messageGroup;
    private final Long deliveryTimestamp;
    private final String bornHost;
    private final Long bornTimestamp;
    private final Integer deliveryAttempt;
    private final Long decodeTimestamp;
    private final Long transportDeliveryTimestamp;

    public GeneralMessageImpl(Message message) {
        this.topic = message.getTopic();
        this.messageId = null;
        if (message instanceof MessageImpl) {
            MessageImpl impl = (MessageImpl) message;
            this.body = impl.body;
        } else {
            // Should never reach here.
            final ByteBuffer byteBuffer = message.getBody();
            this.body = new byte[byteBuffer.remaining()];
            byteBuffer.get(body);
        }
        this.properties = message.getProperties();
        this.tag = message.getTag().orElse(null);
        this.keys = message.getKeys();
        this.messageGroup = message.getMessageGroup().orElse(null);
        this.deliveryTimestamp = message.getDeliveryTimestamp().orElse(null);
        this.bornHost = null;
        this.bornTimestamp = null;
        this.deliveryAttempt = null;
        this.decodeTimestamp = null;
        this.transportDeliveryTimestamp = null;
    }

    public GeneralMessageImpl(MessageView message) {
        this.topic = message.getTopic();
        this.messageId = message.getMessageId();
        byte[] messageBody;
        Long messageDecodeTimestamp;
        Long messageTransportDeliveryTimestamp;
        if (message instanceof MessageViewImpl) {
            MessageViewImpl impl = (MessageViewImpl) message;
            messageBody = impl.body;
            messageDecodeTimestamp = impl.getDecodeTimestamp();
            messageTransportDeliveryTimestamp = impl.getTransportDeliveryTimestamp().orElse(null);
        } else {
            // Should never reach here.
            final ByteBuffer byteBuffer = message.getBody();
            messageBody = new byte[byteBuffer.remaining()];
            byteBuffer.get(messageBody);
            // Could not get accurate decode timestamp.
            messageDecodeTimestamp = null;
            // Could not get accurate transport delivery timestamp.
            messageTransportDeliveryTimestamp = null;
        }
        this.body = messageBody;
        this.properties = message.getProperties();
        this.tag = message.getTag().orElse(null);
        this.keys = message.getKeys();
        this.messageGroup = message.getMessageGroup().orElse(null);
        this.deliveryTimestamp = message.getDeliveryTimestamp().orElse(null);
        this.bornHost = message.getBornHost();
        this.bornTimestamp = message.getBornTimestamp();
        this.deliveryAttempt = message.getDeliveryAttempt();
        this.decodeTimestamp = messageDecodeTimestamp;
        this.transportDeliveryTimestamp = messageTransportDeliveryTimestamp;
    }


    @Override
    public Optional<MessageId> getMessageId() {
        return Optional.ofNullable(messageId);
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public ByteBuffer getBody() {
        return ByteBuffer.wrap(body).asReadOnlyBuffer();
    }

    @Override
    public Map<String, String> getProperties() {
        return new HashMap<>(properties);
    }

    @Override
    public Optional<String> getTag() {
        return Optional.ofNullable(tag);
    }

    @Override
    public Collection<String> getKeys() {
        return new ArrayList<>(keys);
    }

    @Override
    public Optional<String> getMessageGroup() {
        return Optional.ofNullable(messageGroup);
    }

    @Override
    public Optional<Long> getDeliveryTimestamp() {
        return Optional.ofNullable(deliveryTimestamp);
    }

    @Override
    public Optional<String> getBornHost() {
        return Optional.ofNullable(bornHost);
    }

    @Override
    public Optional<Long> getBornTimestamp() {
        return Optional.ofNullable(bornTimestamp);
    }

    @Override
    public Optional<Integer> getDeliveryAttempt() {
        return Optional.ofNullable(deliveryAttempt);
    }

    @Override
    public Optional<Long> getDecodeTimestamp() {
        return Optional.ofNullable(decodeTimestamp);
    }

    @Override
    public Optional<Long> getTransportDeliveryTimestamp() {
        return Optional.ofNullable(transportDeliveryTimestamp);
    }
}
