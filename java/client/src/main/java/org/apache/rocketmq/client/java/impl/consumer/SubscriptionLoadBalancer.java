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

import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.exception.NotFoundException;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteDataResult;

@Immutable
public class SubscriptionLoadBalancer {
    private final TopicRouteDataResult topicRouteDataResult;
    /**
     * Index for round-robin.
     */
    private final AtomicInteger index;
    /**
     * Message queues to receive message.
     */
    private final ImmutableList<MessageQueueImpl> messageQueues;

    public SubscriptionLoadBalancer(TopicRouteDataResult topicRouteDataResult) {
        this.topicRouteDataResult = topicRouteDataResult;
        this.index = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));
        final ImmutableList.Builder<MessageQueueImpl> builder = ImmutableList.builder();
        if (!topicRouteDataResult.ok()) {
            this.messageQueues = builder.build();
            return;
        }
        for (MessageQueueImpl messageQueue : topicRouteDataResult.getTopicRouteData().getMessageQueues()) {
            if (!messageQueue.getPermission().isReadable() ||
                Utilities.MASTER_BROKER_ID != messageQueue.getBroker().getId()) {
                continue;
            }
            builder.add(messageQueue);
        }
        this.messageQueues = builder.build();
    }

    public MessageQueueImpl takeMessageQueue() throws ClientException {
        topicRouteDataResult.checkAndGetTopicRouteData();
        if (messageQueues.isEmpty()) {
            // Should never reach here.
            throw new NotFoundException("Failed to take message queue due to readable message queue doesn't exist");
        }
        final int next = index.getAndIncrement();
        return messageQueues.get(IntMath.mod(next, messageQueues.size()));
    }
}
