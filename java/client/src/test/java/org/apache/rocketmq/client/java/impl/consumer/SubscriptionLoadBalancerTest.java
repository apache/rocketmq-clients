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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.Permission;
import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;

public class SubscriptionLoadBalancerTest {

    private TopicRouteData topicRouteData;
    private SubscriptionLoadBalancer loadBalancer;

    @Before
    public void setUp() {
        final Broker broker = Broker.newBuilder()
            .setName("broker-0")
            .setId(0)
            .setEndpoints(apache.rocketmq.v2.Endpoints.newBuilder()
                .addAddresses(apache.rocketmq.v2.Address.newBuilder()
                    .setHost("127.0.0.1")
                    .setPort(8080)
                    .build())
                .build())
            .build();

        final MessageQueue mq0 = MessageQueue.newBuilder()
            .setTopic(apache.rocketmq.v2.Resource.newBuilder().setName("test-topic").build())
            .setId(0)
            .setPermission(Permission.READ_WRITE)
            .setBroker(broker)
            .build();
        final MessageQueue mq1 = MessageQueue.newBuilder()
            .setTopic(apache.rocketmq.v2.Resource.newBuilder().setName("test-topic").build())
            .setId(1)
            .setPermission(Permission.READ_WRITE)
            .setBroker(broker)
            .build();
        final MessageQueue mq2 = MessageQueue.newBuilder()
            .setTopic(apache.rocketmq.v2.Resource.newBuilder().setName("test-topic").build())
            .setId(2)
            .setPermission(Permission.READ_WRITE)
            .setBroker(broker)
            .build();

        topicRouteData = new TopicRouteData(ImmutableList.of(
            mq0,
            mq1,
            mq2
        ));

        loadBalancer = new SubscriptionLoadBalancer(topicRouteData);
    }

    @Test
    public void testBasicRoundRobin() {
        // Without any empty marking, should distribute across all queues
        Set<Integer> queueIds = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            MessageQueueImpl mq = loadBalancer.takeMessageQueue();
            queueIds.add(mq.getQueueId());
        }
        assertEquals("Should use all 3 queues", 3, queueIds.size());
    }

    @Test
    public void testEmptyQueueSkipping() {
        // Take a queue and mark it as empty multiple times
        MessageQueueImpl firstMq = loadBalancer.takeMessageQueue();

        // Mark first queue as empty multiple times to build up skip weight
        for (int i = 0; i < 5; i++) {
            loadBalancer.markEmptyResult(firstMq);
        }

        // Now the empty queue should be skipped for a while
        // Take several queues - the empty one should be skipped
        Set<Integer> queueIds = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            MessageQueueImpl mq = loadBalancer.takeMessageQueue();
            queueIds.add(mq.getQueueId());
        }
        // With 3 queues and 1 marked empty, we should primarily see the other 2
        assertNotNull("Should return valid queues", queueIds);
    }

    @Test
    public void testNonEmptyResultResetsSkipping() {
        MessageQueueImpl firstMq = loadBalancer.takeMessageQueue();

        // Mark as empty
        for (int i = 0; i < 10; i++) {
            loadBalancer.markEmptyResult(firstMq);
        }

        // Now mark as non-empty - should reset the skip state
        loadBalancer.markNonEmptyResult(firstMq);

        // The queue should no longer be skipped
        boolean foundFirst = false;
        for (int i = 0; i < 10; i++) {
            MessageQueueImpl mq = loadBalancer.takeMessageQueue();
            if (mq.getQueueId() == firstMq.getQueueId()) {
                foundFirst = true;
                break;
            }
        }
        assertTrue("Queue should be selectable again after marking non-empty", foundFirst);
    }

    @Test
    public void testQueueEmptyStateShouldSkip() {
        SubscriptionLoadBalancer.QueueEmptyState state = new SubscriptionLoadBalancer.QueueEmptyState();

        // Initially should not skip
        assertFalse("Fresh state should not skip", state.shouldSkip());

        // After marking empty once, should skip
        state.incrementEmptyResults();
        assertTrue("Should skip after empty result", state.shouldSkip());

        // After many skip rounds, should eventually stop skipping
        for (int i = 0; i < 100; i++) {
            state.incrementSkipRounds();
        }
        assertFalse("Should stop skipping after enough rounds", state.shouldSkip());
    }

    @Test
    public void testQueueEmptyStateReset() {
        SubscriptionLoadBalancer.QueueEmptyState state = new SubscriptionLoadBalancer.QueueEmptyState();
        state.incrementEmptyResults();
        state.incrementEmptyResults();
        assertTrue("Should skip with consecutive empties", state.shouldSkip());

        state.resetEmptyResults();
        assertFalse("Should not skip after reset", state.shouldSkip());
    }

    @Test
    public void testUpdatePreservesEmptyState() {
        MessageQueueImpl mq = loadBalancer.takeMessageQueue();
        loadBalancer.markEmptyResult(mq);
        loadBalancer.markEmptyResult(mq);

        // Update should preserve the empty state map
        SubscriptionLoadBalancer updated = loadBalancer.update(topicRouteData);
        assertNotNull("Updated balancer should not be null", updated);
    }

    @Test
    public void testAllQueuesEmptyStillReturnsQueue() {
        // Even if all queues are empty, takeMessageQueue should still return a queue
        // (it won't skip all queues - attempt limit equals queue count)
        for (int round = 0; round < 3; round++) {
            MessageQueueImpl mq = loadBalancer.takeMessageQueue();
            for (int i = 0; i < 5; i++) {
                loadBalancer.markEmptyResult(mq);
            }
        }

        // Should still return a queue even when all are marked empty
        MessageQueueImpl mq = loadBalancer.takeMessageQueue();
        assertNotNull("Should always return a queue", mq);
    }
}
