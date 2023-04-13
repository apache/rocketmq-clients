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

package org.apache.rocketmq.client.java.route;

import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.java.message.MessageType;
import org.apache.rocketmq.client.java.message.protocol.Resource;

public class MessageQueueImpl {
    private final Resource topicResource;
    private final Broker broker;
    private final int queueId;

    private final Permission permission;
    private final List<MessageType> acceptMessageTypes;

    public MessageQueueImpl(apache.rocketmq.v2.MessageQueue messageQueue) {
        this.topicResource = new Resource(messageQueue.getTopic());
        this.queueId = messageQueue.getId();
        final apache.rocketmq.v2.Permission perm = messageQueue.getPermission();
        this.permission = Permission.fromProtobuf(perm);
        this.acceptMessageTypes = new ArrayList<>();
        final List<apache.rocketmq.v2.MessageType> types = messageQueue.getAcceptMessageTypesList();
        for (apache.rocketmq.v2.MessageType type : types) {
            // 原内容 版本：rocketmq-client-java 5.0.4 及 5.0.6-SNAPSHOT
            // acceptMessageTypes.add(MessageType.fromProtobuf(type));
            // Super Update 20230413 不加此判断，按照官网和其它网上的示例。根本运行不起来
            // 官网：快速开始 https://rocketmq.apache.org/zh/docs/quickStart/01quickstart
            // 提示：Message type is not specified
            if (apache.rocketmq.v2.MessageType.MESSAGE_TYPE_UNSPECIFIED != type) {
                acceptMessageTypes.add(MessageType.fromProtobuf(type));
            }
        }
        this.broker = new Broker(messageQueue.getBroker());
    }

    public Resource getTopicResource() {
        return this.topicResource;
    }

    public String getTopic() {
        return topicResource.getName();
    }

    public Broker getBroker() {
        return this.broker;
    }

    public int getQueueId() {
        return this.queueId;
    }

    public List<MessageType> getAcceptMessageTypes() {
        return acceptMessageTypes;
    }

    public apache.rocketmq.v2.MessageQueue toProtobuf() {
        final List<apache.rocketmq.v2.MessageType> messageTypes = acceptMessageTypes
            .stream().map(MessageType::toProtobuf)
            .collect(Collectors.toList());
        return apache.rocketmq.v2.MessageQueue.newBuilder()
            .setTopic(topicResource.toProtobuf())
            .setId(queueId)
            .setPermission(Permission.toProtobuf(permission))
            .setBroker(broker.toProtobuf())
            .addAllAcceptMessageTypes(messageTypes).build();
    }

    public Permission getPermission() {
        return this.permission;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MessageQueueImpl messageQueueImpl = (MessageQueueImpl) o;
        return queueId == messageQueueImpl.queueId && Objects.equal(topicResource, messageQueueImpl.topicResource) &&
            Objects.equal(broker, messageQueueImpl.broker) && permission == messageQueueImpl.permission;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topicResource, broker, queueId, permission);
    }

    @Override
    public String toString() {
        return broker.getName() + "." + topicResource + "." + queueId;
    }
}
