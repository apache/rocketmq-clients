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

package org.apache.rocketmq.client.java.impl.consumer;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;

@Immutable
public class SubscriptionLoadBalancer {
    /**
     * Index for round-robin.
     */
    private final AtomicInteger index;
    /**
     * Message queues to receive message.
     */
    private final ImmutableList<MessageQueueImpl> messageQueues;

    public SubscriptionLoadBalancer(TopicRouteData topicRouteData) {
        this.index = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));
        final List<MessageQueueImpl> mqs = topicRouteData.getMessageQueues().stream()
            .filter((Predicate<MessageQueueImpl>) mq -> mq.getPermission().isReadable() &&
                Utilities.MASTER_BROKER_ID == mq.getBroker().getId())
            .collect(Collectors.toList());
        if (mqs.isEmpty()) {
            throw new IllegalArgumentException("No readable message queue found, topiRouteData=" + topicRouteData);
        }
        this.messageQueues = ImmutableList.<MessageQueueImpl>builder().addAll(mqs).build();
    }

    public MessageQueueImpl takeMessageQueue() {
        final int next = index.getAndIncrement();
        return messageQueues.get(IntMath.mod(next, messageQueues.size()));
    }
}
