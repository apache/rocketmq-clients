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

import com.google.common.base.Objects;
import org.apache.rocketmq.client.route.Partition;

public class MessageQueue {
    private final String topic;
    private final String brokerName;
    private final int queueId;

    private final transient Partition partition;

    public MessageQueue(Partition partition) {
        this.topic = partition.getTopicResource().getName();
        this.brokerName = partition.getBroker().getName();
        this.queueId = partition.getId();
        this.partition = partition;
    }

    @Override
    public String toString() {
        return topic + "." + brokerName + "." + queueId;
    }

    public String getTopic() {
        return this.topic;
    }

    public String getBrokerName() {
        return this.brokerName;
    }

    public int getQueueId() {
        return this.queueId;
    }

    public Partition getPartition() {
        return this.partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MessageQueue that = (MessageQueue) o;
        return queueId == that.queueId && Objects.equal(topic, that.topic) &&
               Objects.equal(brokerName, that.brokerName) && Objects.equal(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topic, brokerName, queueId, partition);
    }
}
