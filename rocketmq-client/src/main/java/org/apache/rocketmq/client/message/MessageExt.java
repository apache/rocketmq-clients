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


import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.Endpoints;

@EqualsAndHashCode
@ToString
public class MessageExt {
    protected final MessageImpl impl;

    public MessageExt(MessageImpl impl) {
        this.impl = impl;
    }

    public byte[] getBody() {
        return this.impl.getBody();
    }

    public String getTopic() {
        return this.impl.getTopic();
    }

    public String getTag() {
        return this.impl.getSystemAttribute().getTag();
    }

    public String getKeys() {
        StringBuilder keys = new StringBuilder();
        for (String key : this.impl.getSystemAttribute().getKeys()) {
            keys.append(key).append(MixAll.MESSAGE_KEY_SEPARATOR);
        }
        return keys.toString().trim();
    }

    public int getDelayTimeLevel() {
        return this.impl.getSystemAttribute().getDelayLevel();
    }

    public long getDeliveryTimestamp() {
        return this.impl.getSystemAttribute().getDeliveryTimeMillis();
    }

    public int getQueueId() {
        return this.impl.getSystemAttribute().getPartitionId();
    }

    public long getBornTimestamp() {
        return this.impl.getSystemAttribute().getBornTimeMillis();
    }

    public String getBornHost() {
        return this.impl.getSystemAttribute().getBornHost();
    }

    public boolean isExpired(long tolerance) {
        throw new UnsupportedOperationException();
    }

    public long getQueueOffset() {
        return this.impl.getSystemAttribute().getPartitionOffset();
    }

    public String getMsgId() {
        return this.impl.getSystemAttribute().getMessageId();
    }

    public long getDecodedTimestamp() {
        return this.impl.getSystemAttribute().getDecodedTimestamp();
    }

    public int getReconsumeTimes() {
        final int deliveryAttempt = this.impl.getSystemAttribute().getDeliveryAttempt();
        if (deliveryAttempt <= 0) {
            return 0;
        }
        return deliveryAttempt - 1;
    }

    public int getDeliveryAttempt() {
        return this.impl.getSystemAttribute().getDeliveryAttempt();
    }

    public String getReceiptHandle() {
        return this.impl.getSystemAttribute().getReceiptHandle();
    }

    public String getMessageGroup() {
        return this.impl.getSystemAttribute().getMessageGroup();
    }

    // TODO: hide targetEndpoint here.
    public Endpoints getAckEndpoints() {
        return this.impl.getSystemAttribute().getAckEndpoints();
    }

    public String getTraceContext() {
        return this.impl.getSystemAttribute().getTraceContext();
    }

    public Map<String, String> getUserProperties() {
        return this.impl.getUserAttribute();
    }

    public MessageType getMsgType() {
        switch (this.impl.getSystemAttribute().getMessageType()) {
            case FIFO:
                return MessageType.FIFO;
            case DELAY:
                return MessageType.DELAY;
            case TRANSACTION:
                return MessageType.TRANSACTION;
            default:
                return MessageType.NORMAL;
        }
    }
}
