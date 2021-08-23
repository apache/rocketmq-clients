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

package org.apache.rocketmq.client.impl.producer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.Broker;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class SendingTopicRouteData {
    private static final Logger log = LoggerFactory.getLogger(SendingTopicRouteData.class);

    private final AtomicInteger index;
    /**
     * Partitions to send message.
     */
    private final List<Partition> partitions;

    public SendingTopicRouteData(TopicRouteData topicRouteData) {
        this.index = new AtomicInteger(RandomUtils.nextInt());
        this.partitions = filterPartition(topicRouteData);
    }

    public List<MessageQueue> getMessageQueues() {
        List<MessageQueue> messageQueues = new ArrayList<MessageQueue>();
        for (Partition partition : partitions) {
            messageQueues.add(new MessageQueue(partition));
        }
        return messageQueues;
    }

    @VisibleForTesting
    public static List<Partition> filterPartition(TopicRouteData topicRouteData) {
        List<Partition> partitions = new ArrayList<Partition>();
        for (Partition partition : topicRouteData.getPartitions()) {
            if (!partition.getPermission().isWritable()) {
                continue;
            }
            if (MixAll.MASTER_BROKER_ID != partition.getBroker().getId()) {
                continue;
            }
            partitions.add(partition);
        }
        if (partitions.isEmpty()) {
            log.warn("No available partition, topicRouteData={}", topicRouteData);
        }
        return partitions;
    }

    public boolean isEmpty() {
        return partitions.isEmpty();
    }

    public List<Partition> takePartitions(Set<Endpoints> isolated, int count) throws ClientException {
        int nextIndex = index.getAndIncrement();
        List<Partition> candidatePartitions = new ArrayList<Partition>();
        Set<String> candidateBrokerNames = new HashSet<String>();
        if (partitions.isEmpty()) {
            throw new ClientException(ErrorCode.NO_PERMISSION);
        }
        for (int i = 0; i < partitions.size(); i++) {
            final Partition partition = partitions.get(UtilAll.positiveMod(nextIndex++, partitions.size()));
            final Broker broker = partition.getBroker();
            final String brokerName = broker.getName();
            if (!isolated.contains(broker.getEndpoints()) && !candidateBrokerNames.contains(brokerName)) {
                candidateBrokerNames.add(brokerName);
                candidatePartitions.add(partition);
            }
            if (candidatePartitions.size() >= count) {
                return candidatePartitions;
            }
        }
        // if all endpoints are isolated.
        if (candidatePartitions.isEmpty()) {
            for (int i = 0; i < partitions.size(); i++) {
                final Partition partition = partitions.get(UtilAll.positiveMod(nextIndex++, partitions.size()));
                final Broker broker = partition.getBroker();
                final String brokerName = broker.getName();
                if (!candidateBrokerNames.contains(brokerName)) {
                    candidateBrokerNames.add(brokerName);
                    candidatePartitions.add(partition);
                }
                if (candidatePartitions.size() >= count) {
                    break;
                }
            }
        }
        return candidatePartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SendingTopicRouteData that = (SendingTopicRouteData) o;
        return Objects.equal(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partitions);
    }
}
