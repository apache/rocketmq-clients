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
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;

public class MessageImpl {
    private String topic;
    private final SystemAttribute systemAttribute;
    private final ConcurrentMap<String, String> userAttribute;
    private byte[] body;
    private final boolean corrupted;

    public MessageImpl(String topic, SystemAttribute systemAttribute, ConcurrentMap<String, String> userAttribute,
                       byte[] body) {
        this(topic, systemAttribute, userAttribute, body, false);
    }

    public MessageImpl(String topic, SystemAttribute systemAttribute, ConcurrentMap<String, String> userAttribute,
                       byte[] body, boolean corrupted) {
        this.topic = topic;
        this.systemAttribute = systemAttribute;
        this.userAttribute = userAttribute;
        this.body = body;
        this.corrupted = corrupted;
    }

    public void setBody(byte[] body) {
        if (null == body) {
            this.body = null;
            return;
        }
        this.body = body.clone();
    }

    public byte[] getBody() {
        return null == body ? null : body.clone();
    }

    public String getTopic() {
        return this.topic;
    }

    public SystemAttribute getSystemAttribute() {
        return this.systemAttribute;
    }

    public ConcurrentMap<String, String> getUserAttribute() {
        return this.userAttribute;
    }

    public boolean isCorrupted() {
        return this.corrupted;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MessageImpl message = (MessageImpl) o;
        return corrupted == message.corrupted && Objects.equal(topic, message.topic) &&
               Objects.equal(systemAttribute, message.systemAttribute) &&
               Objects.equal(userAttribute, message.userAttribute) &&
               Objects.equal(body, message.body);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topic, systemAttribute, userAttribute, body, corrupted);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("topic", topic)
                          .add("systemAttribute", systemAttribute)
                          .add("userAttribute", userAttribute)
                          .add("body", body)
                          .add("corrupted", corrupted)
                          .toString();
    }
}
