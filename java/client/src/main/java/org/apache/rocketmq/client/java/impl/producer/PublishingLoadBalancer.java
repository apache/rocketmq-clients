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

package org.apache.rocketmq.client.java.impl.producer;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.math.IntMath;
import com.google.common.math.LongMath;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.Broker;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;

@Immutable
public class PublishingLoadBalancer {
    /**
     * Index for round-robin.
     */
    private final AtomicInteger index;
    /**
     * Message queues to send message.
     */
    private final ImmutableList<MessageQueueImpl> messageQueues;

    public PublishingLoadBalancer(TopicRouteData topicRouteData) {
        this.index = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));
        final List<MessageQueueImpl> mqs = topicRouteData.getMessageQueues().stream()
            .filter((Predicate<MessageQueueImpl>) mq -> mq.getPermission().isWritable() &&
                Utilities.MASTER_BROKER_ID == mq.getBroker().getId())
            .collect(Collectors.toList());
        if (mqs.isEmpty()) {
            throw new IllegalArgumentException("No writable message queue found, topiRouteData=" + topicRouteData);
        }
        this.messageQueues = ImmutableList.<MessageQueueImpl>builder().addAll(mqs).build();
    }

    public MessageQueueImpl takeMessageQueueByMessageGroup(String messageGroup) {
        final long hashCode = Hashing.sipHash24().hashBytes(messageGroup.getBytes(StandardCharsets.UTF_8)).asLong();
        final int index = LongMath.mod(hashCode, messageQueues.size());
        return messageQueues.get(index);
    }

    public List<MessageQueueImpl> takeMessageQueues(Set<Endpoints> excluded, int count) {
        int next = index.getAndIncrement();
        List<MessageQueueImpl> candidates = new ArrayList<>();
        Set<String> candidateBrokerNames = new HashSet<>();

        for (int i = 0; i < messageQueues.size(); i++) {
            final MessageQueueImpl messageQueueImpl = messageQueues.get(IntMath.mod(next++, messageQueues.size()));
            final Broker broker = messageQueueImpl.getBroker();
            final String brokerName = broker.getName();
            if (!excluded.contains(broker.getEndpoints()) && !candidateBrokerNames.contains(brokerName)) {
                candidateBrokerNames.add(brokerName);
                candidates.add(messageQueueImpl);
            }
            if (candidates.size() >= count) {
                return candidates;
            }
        }
        // If all endpoints are isolated.
        if (candidates.isEmpty()) {
            for (int i = 0; i < messageQueues.size(); i++) {
                final MessageQueueImpl messageQueueImpl = messageQueues.get(IntMath.mod(next++, messageQueues.size()));
                final Broker broker = messageQueueImpl.getBroker();
                final String brokerName = broker.getName();
                if (!candidateBrokerNames.contains(brokerName)) {
                    candidateBrokerNames.add(brokerName);
                    candidates.add(messageQueueImpl);
                }
                if (candidates.size() >= count) {
                    break;
                }
            }
        }
        return candidates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PublishingLoadBalancer that = (PublishingLoadBalancer) o;
        return Objects.equal(messageQueues, that.messageQueues);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(messageQueues);
    }
}
