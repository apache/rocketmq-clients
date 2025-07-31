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

import com.google.common.base.MoreObjects;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.apis.message.Message;

/**
 * Default implementation of {@link Message}
 *
 * @see Message
 */
public class MessageImpl implements Message {

    protected final Collection<String> keys;

    protected final Map<String, String> properties;

    final byte[] body;
    private final String topic;

    @Nullable
    private final String tag;
    @Nullable
    private final String messageGroup;
    @Nullable
    private final Long deliveryTimestamp;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     */
    MessageImpl(String topic, byte[] body, @Nullable String tag, Collection<String> keys,
        @Nullable String messageGroup, @Nullable Long deliveryTimestamp,
        Map<String, String> properties) {
        this.topic = topic;
        this.body = body;
        this.tag = tag;
        this.messageGroup = messageGroup;
        this.deliveryTimestamp = deliveryTimestamp;
        this.keys = keys;
        this.properties = properties;
    }

    MessageImpl(Message message) {
        this.topic = message.getTopic();
        if (message instanceof MessageImpl) {
            MessageImpl impl = (MessageImpl) message;
            this.body = impl.body;
        } else {
            // Should never reach here.
            final ByteBuffer body = message.getBody();
            byte[] bytes = new byte[body.remaining()];
            body.get(bytes);
            this.body = bytes;
        }
        this.tag = message.getTag().orElse(null);
        this.messageGroup = message.getMessageGroup().orElse(null);
        this.deliveryTimestamp = message.getDeliveryTimestamp().orElse(null);
        this.keys = message.getKeys();
        this.properties = message.getProperties();
    }

    /**
     * @see Message#getTopic()
     */
    @Override
    public String getTopic() {
        return topic;
    }

    /**
     * @see Message#getBody()
     */
    @Override
    public ByteBuffer getBody() {
        return ByteBuffer.wrap(body).asReadOnlyBuffer();
    }

    /**
     * @see Message#getProperties()
     */
    @Override
    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    /**
     * @see Message#getTag()
     */
    @Override
    public Optional<String> getTag() {
        return Optional.ofNullable(tag);
    }

    /**
     * @see Message#getKeys()
     */
    @Override
    public Collection<String> getKeys() {
        return new ArrayList<>(keys);
    }

    /**
     * @see Message#getDeliveryTimestamp()
     */
    @Override
    public Optional<Long> getDeliveryTimestamp() {
        return Optional.ofNullable(deliveryTimestamp);
    }

    /**
     * @see Message#getMessageGroup()
     */
    @Override
    public Optional<String> getMessageGroup() {
        return Optional.ofNullable(messageGroup);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("topic", topic)
            .add("tag", tag)
            .add("messageGroup", messageGroup)
            .add("deliveryTimestamp", deliveryTimestamp)
            .add("keys", keys)
            .add("properties", properties)
            .toString();
    }
}
