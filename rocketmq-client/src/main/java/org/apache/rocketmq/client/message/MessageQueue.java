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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.rocketmq.client.route.Partition;

@Getter
@EqualsAndHashCode
public class MessageQueue {
    private final String topic;
    private final String brokerName;
    private final int queueId;

    private final Partition partition;

    public MessageQueue(Partition partition) {
        this.topic = partition.getTopicResource().getName();
        this.brokerName = partition.getBroker().getName();
        this.queueId = partition.getId();
        this.partition = partition;
    }

    @Deprecated
    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
        this.partition = null;
    }

    @Override
    public String toString() {
        return topic + "." + brokerName + "." + queueId;
    }
}
