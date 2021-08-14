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

package org.apache.rocketmq.client.message;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;
import org.apache.rocketmq.client.misc.MixAll;


public class Message {
    final MessageImpl impl;
    private final MessageExt messageExt;

    public Message(String topic, String tag, byte[] body) {
        final SystemAttribute systemAttribute = new SystemAttribute();
        final ConcurrentHashMap<String, String> userAttribute = new ConcurrentHashMap<String, String>();
        systemAttribute.setBornTimeMillis(System.currentTimeMillis());
        systemAttribute.setTag(tag);
        this.impl = new MessageImpl(topic, systemAttribute, userAttribute, body);
        this.messageExt = new MessageExt(impl);
    }

    public Message(MessageImpl impl) {
        this.impl = impl;
        this.messageExt = new MessageExt(impl);
    }

    public void setTopic(String topic) {
        this.impl.setTopic(topic);
    }

    public String getTopic() {
        return this.impl.getTopic();
    }

    public String getTag() {
        return this.impl.getSystemAttribute().getTag();
    }

    public void putUserProperty(final String name, final String value) {
        this.impl.getUserAttribute().put(name, value);
    }

    public String getUserProperty(final String name) {
        return this.impl.getUserAttribute().get(name);
    }

    public void setKeys(String keys) {
        final List<String> keyList = this.impl.getSystemAttribute().getKeys();
        keyList.clear();
        keyList.add(keys.trim());
    }

    public void setKeys(Collection<String> keys) {
        final List<String> keyList = this.impl.getSystemAttribute().getKeys();
        keyList.clear();
        for (String key : keys) {
            keyList.add(key.trim());
        }
    }

    public String getKeys() {
        StringBuilder keys = new StringBuilder();
        for (String key : this.impl.getSystemAttribute().getKeys()) {
            keys.append(key).append(MixAll.MESSAGE_KEY_SEPARATOR);
        }
        return keys.toString().trim();
    }

    public List<String> getKeysList() {
        return this.impl.getSystemAttribute().getKeys();
    }

    public int getDelayTimeLevel() {
        return this.impl.getSystemAttribute().getDelayLevel();
    }

    public void setDelayTimeLevel(int level) {
        this.impl.getSystemAttribute().setDelayLevel(level);
    }

    public void setDeliveryTimestamp(long deliveryTimestamp) {
        final SystemAttribute systemAttribute = this.impl.getSystemAttribute();
        systemAttribute.setDeliveryTimeMillis(deliveryTimestamp);
        systemAttribute.setMessageType(MessageType.DELAY);
    }

    public long getDelayTimeMillis() {
        return this.impl.getSystemAttribute().getDeliveryTimeMillis();
    }

    public void setBody(byte[] body) {
        this.impl.setBody(body);
    }

    public byte[] getBody() {
        return this.impl.getBody();
    }

    public Map<String, String> getUserProperties() {
        return this.impl.getUserAttribute();
    }

    public String getMessageGroup() {
        return this.impl.getSystemAttribute().getMessageGroup();
    }

    public String getMsgId() {
        return this.impl.getSystemAttribute().getMessageId();
    }

    public long getBornTimeMillis() {
        return this.impl.getSystemAttribute().getBornTimeMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Message message = (Message) o;
        return Objects.equal(impl, message.impl);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(impl);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("impl", impl)
                          .toString();
    }

    public MessageExt getMessageExt() {
        return this.messageExt;
    }
}
