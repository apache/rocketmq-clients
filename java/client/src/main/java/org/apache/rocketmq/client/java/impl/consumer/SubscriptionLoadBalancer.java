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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;

/**
 * Subscription load balancer with empty-queue skip scheduling.
 *
 * <p>When using long-polling to receive messages, if some queues are empty, the consumer will be
 * blocked by long-polling on those empty queues, causing delayed message reception on queues that
 * actually have messages. This enhanced load balancer tracks empty queue results and temporarily
 * skips queues that have been returning empty results consecutively, so that queues with messages
 * can be polled more frequently.
 */
@Immutable
public class SubscriptionLoadBalancer {

    /**
     * Maximum number of consecutive skip rounds for an empty queue.
     */
    private static final int MAX_SKIP_ROUNDS = 1000;

    /**
     * Base multiplier: each consecutive empty result increases skip rounds by this amount.
     */
    private static final int SKIP_ROUNDS_PER_EMPTY = 50;

    /**
     * Maximum consecutive empty results tracked before resetting the counter.
     */
    private static final int MAX_EMPTY_RESULTS = 200;

    /**
     * Message queues to receive message.
     */
    protected final ImmutableList<MessageQueueImpl> messageQueues;

    /**
     * Index for round-robin.
     */
    private final AtomicInteger index;

    /**
     * Tracks empty-result state per queue for skip scheduling.
     */
    private volatile Map<String, QueueEmptyState> emptyStateMap;

    public SubscriptionLoadBalancer(TopicRouteData topicRouteData) {
        this(new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE)), topicRouteData, null);
    }

    private SubscriptionLoadBalancer(AtomicInteger index, TopicRouteData topicRouteData,
        Map<String, QueueEmptyState> emptyStateMap) {
        this.index = index;
        final List<MessageQueueImpl> mqs = topicRouteData.getMessageQueues().stream()
            .filter(SubscriptionLoadBalancer::isReadableMasterQueue)
            .collect(Collectors.toList());
        if (mqs.isEmpty()) {
            throw new IllegalArgumentException("No readable message queue found, topiRouteData=" + topicRouteData);
        }
        this.messageQueues = ImmutableList.<MessageQueueImpl>builder().addAll(mqs).build();
        this.emptyStateMap = emptyStateMap != null ? emptyStateMap : new ConcurrentHashMap<>();
    }

    /**
     * Check if the message queue is readable and belongs to master broker.
     */
    public static boolean isReadableMasterQueue(MessageQueueImpl mq) {
        return mq.getPermission().isReadable() && Utilities.MASTER_BROKER_ID == mq.getBroker().getId();
    }

    SubscriptionLoadBalancer update(TopicRouteData topicRouteData) {
        return new SubscriptionLoadBalancer(index, topicRouteData, emptyStateMap);
    }

    /**
     * Select the next message queue, skipping queues that have been returning empty results.
     *
     * <p>When a queue has been marked as empty, it will be temporarily skipped for a number of
     * rounds proportional to its consecutive empty results count. This prevents long-polling from
     * blocking on empty queues while other queues have messages waiting.
     *
     * @return the next message queue to poll
     */
    public MessageQueueImpl takeMessageQueue() {
        return selectNext(0);
    }

    private MessageQueueImpl selectNext(int attempt) {
        final int next = index.getAndIncrement();
        int idx = IntMath.mod(next, messageQueues.size());
        MessageQueueImpl mq = messageQueues.get(idx);
        if (attempt < messageQueues.size() && shouldSkip(mq)) {
            return selectNext(attempt + 1);
        }
        resetSkipRounds(mq);
        return mq;
    }

    private boolean shouldSkip(MessageQueueImpl mq) {
        QueueEmptyState state = emptyStateMap.get(queueKey(mq));
        if (state != null && state.shouldSkip()) {
            state.incrementSkipRounds();
            return true;
        }
        return false;
    }

    /**
     * Mark a queue as having returned an empty result. This increases the skip weight for the queue.
     *
     * @param mq the message queue that returned no messages
     */
    public void markEmptyResult(MessageQueueImpl mq) {
        QueueEmptyState state = emptyStateMap.computeIfAbsent(queueKey(mq), k -> new QueueEmptyState());
        state.incrementEmptyResults();
    }

    /**
     * Mark a queue as having returned messages. This resets the empty counter for the queue.
     *
     * @param mq the message queue that returned messages
     */
    public void markNonEmptyResult(MessageQueueImpl mq) {
        QueueEmptyState state = emptyStateMap.get(queueKey(mq));
        if (state != null) {
            state.resetEmptyResults();
        }
    }

    private void resetSkipRounds(MessageQueueImpl mq) {
        QueueEmptyState state = emptyStateMap.get(queueKey(mq));
        if (state != null) {
            state.resetSkipRounds();
        }
    }

    private String queueKey(MessageQueueImpl mq) {
        return mq.toString();
    }

    /**
     * Tracks the empty-result state for a single message queue.
     */
    static class QueueEmptyState {
        private static final AtomicIntegerFieldUpdater<QueueEmptyState> SKIP_ROUNDS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(QueueEmptyState.class, "skipRounds");
        private static final AtomicIntegerFieldUpdater<QueueEmptyState> EMPTY_RESULTS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(QueueEmptyState.class, "consecutiveEmptyResults");

        /**
         * Number of rounds this queue has been skipped in the current skip cycle.
         */
        private volatile int skipRounds;
        /**
         * Number of consecutive empty results received from this queue.
         */
        private volatile int consecutiveEmptyResults;

        void incrementSkipRounds() {
            SKIP_ROUNDS_UPDATER.incrementAndGet(this);
        }

        void resetSkipRounds() {
            SKIP_ROUNDS_UPDATER.set(this, 0);
        }

        void incrementEmptyResults() {
            EMPTY_RESULTS_UPDATER.updateAndGet(this, v -> (v + 1) % MAX_EMPTY_RESULTS);
        }

        void resetEmptyResults() {
            EMPTY_RESULTS_UPDATER.set(this, 0);
        }

        /**
         * Determine whether this queue should be skipped in the current scheduling round.
         *
         * <p>A queue is skipped when it has consecutive empty results and the current skip rounds
         * have not exceeded the calculated threshold. The threshold grows linearly with consecutive
         * empty results (capped at {@link #MAX_SKIP_ROUNDS}).
         */
        boolean shouldSkip() {
            int emptyCount = consecutiveEmptyResults;
            return emptyCount > 0
                && skipRounds <= Math.min((long) emptyCount * SKIP_ROUNDS_PER_EMPTY, MAX_SKIP_ROUNDS);
        }

        // Visible for testing
        int getSkipRounds() {
            return skipRounds;
        }

        int getConsecutiveEmptyResults() {
            return consecutiveEmptyResults;
        }
    }
}
