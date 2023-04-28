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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.GetOffsetRequest;
import apache.rocketmq.v2.GetOffsetResponse;
import apache.rocketmq.v2.PullMessageRequest;
import apache.rocketmq.v2.QueryOffsetRequest;
import apache.rocketmq.v2.QueryOffsetResponse;
import apache.rocketmq.v2.Status;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ExcludeFromJacocoGeneratedReport;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"NullableProblems", "UnstableApiUsage"})
public class PullProcessQueueImpl implements PullProcessQueue {
    static final Duration LONG_POLLING_TIMEOUT = Duration.ofSeconds(30);

    private static final Logger log = LoggerFactory.getLogger(PullProcessQueueImpl.class);

    private static final int PULL_BATCH_SIZE = 32;

    private static final Duration PULL_FLOW_CONTROL_BACKOFF_DELAY = Duration.ofMillis(20);

    private static final Duration PULL_FAILURE_BACKOFF_DELAY = Duration.ofSeconds(1);

    private static final Duration QUERY_OFFSET_BACKOFF_DELAY = Duration.ofSeconds(3);
    private static final Duration PAUSE_CHECK_PERIOD = Duration.ofSeconds(1);

    private final PullConsumerImpl consumer;
    private final FilterExpression filterExpression;
    private volatile boolean dropped;
    private final MessageQueueImpl mq;

    private final AtomicLong pullTimes;

    private final AtomicLong pulledMessagesQuantity;

    private volatile long activityNanoTime = System.nanoTime();
    private volatile long cacheFullNanoTime = Long.MIN_VALUE;

    private final AtomicBoolean paused;

    private final long initialOffset;
    private volatile long consumedOffset;

    public PullProcessQueueImpl(PullConsumerImpl consumer, MessageQueueImpl mq, FilterExpression filterExpression) {
        this(consumer, mq, filterExpression, -1);
    }

    public PullProcessQueueImpl(PullConsumerImpl consumer, MessageQueueImpl mq, FilterExpression filterExpression,
        long initialOffset) {
        this.consumer = consumer;
        this.dropped = false;
        this.mq = mq;
        this.filterExpression = filterExpression;
        this.pullTimes = new AtomicLong();
        this.paused = new AtomicBoolean(false);
        this.initialOffset = initialOffset;
        this.pulledMessagesQuantity = new AtomicLong();
    }

    public void pause() {
        paused.compareAndSet(false, true);
    }

    public void resume() {
        paused.compareAndSet(true, false);
    }

    @Override
    public MessageQueueImpl getMessageQueue() {
        return mq;
    }

    @Override
    public void drop() {
        this.dropped = true;
        log.info("Process queue was dropped, pq={}, clientId={}", this, consumer.getClientId());
    }

    @Override
    public boolean expired() {
        final Duration requestTimeout = consumer.getClientConfiguration().getRequestTimeout();
        final Duration maxIdleDuration = LONG_POLLING_TIMEOUT.plus(requestTimeout).multipliedBy(3);
        final Duration idleDuration = Duration.ofNanos(System.nanoTime() - activityNanoTime);
        if (idleDuration.compareTo(maxIdleDuration) < 0) {
            return false;
        }
        final Duration afterCacheFullDuration = Duration.ofNanos(System.nanoTime() - cacheFullNanoTime);
        if (afterCacheFullDuration.compareTo(maxIdleDuration) < 0) {
            return false;
        }
        log.warn("Process queue is idle, idleDuration={}, maxIdleDuration={}, afterCacheFullDuration={}, mq={}, " +
            "clientId={}", idleDuration, maxIdleDuration, afterCacheFullDuration, mq, consumer.getClientId());
        return true;
    }

    @Override
    public long getCachedMessageCount() {
        return consumer.peekCachedMessages(this).size();
    }

    @Override
    public long getCachedMessageBytes() {
        final List<MessageViewImpl> messages = consumer.peekCachedMessages(this);
        long bodyBytes = 0;
        for (MessageViewImpl message : messages) {
            bodyBytes += message.getBody().remaining();
        }
        return bodyBytes;
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    public void doStats() {
        final long pullTimes = this.pullTimes.getAndSet(0);
        final long pulledMessagesQuantity = this.pulledMessagesQuantity.getAndSet(0);
        log.info("Process queue stats: clientId={}, mq={}, pullTimes={}, pulledMessagesQuantity={}, " +
                "cachedMessageCount={}, cachedMessageBytes={}", consumer.getClientId(), mq, pullTimes,
            pulledMessagesQuantity, this.getCachedMessageCount(), this.getCachedMessageBytes());
    }

    public boolean isCacheFull() {
        final int cachedMessageCountThreshold = consumer.getMaxCacheMessageCountEachQueue();
        final long actualMessageQuantity = this.getCachedMessageCount();
        final ClientId clientId = consumer.getClientId();
        if (cachedMessageCountThreshold < actualMessageQuantity) {
            log.warn("Process queue total cached messages quantity exceeds the threshold, threshold={}, actual={}, " +
                "mq={}, clientId={}", cachedMessageCountThreshold, actualMessageQuantity, mq, clientId);
            cacheFullNanoTime = System.nanoTime();
            return true;
        }
        final int cachedMessageSizeInBytesThreshold = consumer.getMaxCacheMessageSizeInBytesEachQueue();
        final long actualMessageBytes = this.getCachedMessageBytes();
        if (cachedMessageSizeInBytesThreshold < actualMessageBytes) {
            log.warn("Process queue total cached messages memory exceeds the threshold, threshold={} bytes, " +
                    "actual={} bytes, mq={}, clientId={}", cachedMessageSizeInBytesThreshold, actualMessageBytes, mq,
                clientId);
            cacheFullNanoTime = System.nanoTime();
            return true;
        }
        return false;
    }

    public void pullMessageImmediately(long offset) {
        final ClientId clientId = consumer.getClientId();
        if (!consumer.isRunning()) {
            log.info("Stop to pull message because consumer is not running, mq={}, offset={}, clientId={}",
                mq, offset, clientId);
            return;
        }
        if (dropped) {
            log.info("Process queue has been dropped, no longer pull message, mq={}, pq={}, clientId={}", mq,
                this, clientId);
            return;
        }
        if (this.isCacheFull()) {
            log.warn("Process queue cache is full, would receive message later, mq={}, clientId={}", mq, clientId);
            return;
        }
        if (paused.get()) {
            log.debug("Process queue pulling is paused, mq={}, clientId={}", mq, clientId);
            final ScheduledExecutorService scheduler = consumer.getScheduler();
            scheduler.schedule(() -> pullMessageImmediately(offset), PAUSE_CHECK_PERIOD.toNanos(),
                TimeUnit.NANOSECONDS);
            return;
        }
        try {
            final PullMessageRequest request = consumer.wrapPullMessageRequest(offset, PULL_BATCH_SIZE, mq,
                filterExpression);
            activityNanoTime = System.nanoTime();
            final ListenableFuture<PullMessageResult> future = consumer.pullMessage(request, mq, LONG_POLLING_TIMEOUT);
            final Endpoints endpoints = mq.getBroker().getEndpoints();
            Futures.addCallback(future, new FutureCallback<PullMessageResult>() {
                @Override
                public void onSuccess(PullMessageResult result) {
                    try {
                        onPullMessageResult(result, offset);
                    } catch (Throwable t) {
                        // Should never reach here.
                        log.error("[Bug] Exception raised while handling pull request, mq={}, endpoints={}, " +
                            "clientId={}", mq, endpoints, clientId, t);
                        onPullMessageException(offset, t);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Exception raised during message pulling, mq={}, endpoints={}, clientId={}", mq,
                        endpoints, clientId, t);
                    onPullMessageException(offset, t);
                }
            }, MoreExecutors.directExecutor());
            pullTimes.getAndIncrement();
            consumer.getPullTimes().getAndIncrement();
        } catch (Throwable t) {
            log.error("Exception raised during message pulling, mq={}, clientId={}", mq, clientId, t);
        }
    }

    private void onPullMessageException(long offset, Throwable t) {
        Duration delay = t instanceof TooManyRequestsException ? PULL_FLOW_CONTROL_BACKOFF_DELAY :
            PULL_FAILURE_BACKOFF_DELAY;
        pullMessageLater(offset, delay);
    }

    private void pullMessageLater(long offset, Duration delay) {
        final ClientId clientId = consumer.getClientId();
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            log.info("Try to pull message later, mq={}, delay={}, clientId={}", mq, delay, clientId);
            scheduler.schedule(() -> pullMessageImmediately(offset), delay.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            log.error("[Bug] Failed to schedule message pulling request, mq={}, clientId={}", mq, clientId, t);
            onPullMessageException(offset, t);
        }
    }

    void onPullMessageResult(PullMessageResult result, long offset) {
        final ClientId clientId = consumer.getClientId();
        if (paused.get()) {
            log.info("Discard pull result because process queue pulling is paused, mq={}, pq={}, offset={}, " +
                    "clientId={}", mq, this, offset, clientId);
            pullMessageImmediately(offset);
            return;
        }
        if (dropped) {
            log.info("Discard pull result because process queue is dropped, mq={}, pq={}, clientId={}", mq, this,
                clientId);
            return;
        }
        final List<MessageViewImpl> messages = result.getMessageViewImpls();
        if (!messages.isEmpty()) {
            pulledMessagesQuantity.getAndAdd(messages.size());
            consumer.getPulledMessagesQuantity().getAndAdd(messages.size());
            consumer.cacheMessages(this, messages);
        }
        pullMessageImmediately(result.getNextOffset());
    }

    @Override
    public void pullMessageImmediately() {
        final ClientId clientId = consumer.getClientId();
        if (initialOffset >= 0) {
            log.info("Start to pull message immediately because offset is appointed already, mq={}, initialOffset={}," +
                " clientId={}", mq, initialOffset, clientId);
            pullMessageImmediately(initialOffset);
            return;
        }
        if (!consumer.isRunning()) {
            log.info("Stop to pull message because consumer is not running, mq={}, initialOffset={}, clientId={}", mq,
                initialOffset, clientId);
            return;
        }
        if (dropped) {
            log.info("Process queue has been dropped, no longer receive message, mq={}, initialOffset={}, " +
                "pq={}, clientId={}", mq, initialOffset, this, clientId);
            return;
        }
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        if (paused.get()) {
            log.debug("Process queue pulling is paused, mq={}, initialOffset={}, clientId={}",
                mq, initialOffset, clientId);
            scheduler.schedule(() -> pullMessageImmediately(), PAUSE_CHECK_PERIOD.toNanos(), TimeUnit.NANOSECONDS);
            return;
        }
        final RpcFuture<GetOffsetRequest, GetOffsetResponse> future0 = consumer.getOffset(mq);
        final ListenableFuture<Long> future = Futures.transformAsync(future0, response -> {
            final Status status = response.getStatus();
            StatusChecker.check(status, future0);
            if (Code.OFFSET_NOT_FOUND.equals(status.getCode())) {
                final RpcFuture<QueryOffsetRequest, QueryOffsetResponse> future1 =
                    consumer.queryOffset(mq, OffsetPolicy.END);
                return Futures.transformAsync(consumer.queryOffset(mq, OffsetPolicy.END), input -> {
                    StatusChecker.check(input.getStatus(), future1);
                    final long offset = input.getOffset();
                    log.info("Query offset using end policy from remote successfully, consumerGroup={}, mq={}," +
                        " offset={}, clientId={}", consumer.getConsumerGroup(), mq, offset, clientId);
                    return Futures.immediateFuture(offset);
                }, MoreExecutors.directExecutor());
            }
            final long offset = response.getOffset();
            log.info("Get offset by consumerGroup from remote successfully, consumerGroup={}, mq={}, offset={}, " +
                "clientId={}", consumer.getConsumerGroup(), mq, offset, clientId);
            return Futures.immediateFuture(offset);
        }, MoreExecutors.directExecutor());
        Futures.addCallback(future, new FutureCallback<Long>() {
            @Override
            public void onSuccess(Long offset) {
                consumedOffset = offset;
                log.info("Start to pull message immediately because offset is fetched from remote successfully," +
                    " mq={}, offset={}, clientId={}", mq, consumedOffset, clientId);
                pullMessageImmediately(consumedOffset);
            }

            @Override
            public void onFailure(Throwable t) {
                log.info("Exception raised while fetching offset from remote, mq={}, clientId={}", mq, clientId, t);
                scheduler.schedule(() -> pullMessageImmediately(), QUERY_OFFSET_BACKOFF_DELAY.toNanos(),
                    TimeUnit.NANOSECONDS);
            }
        }, MoreExecutors.directExecutor());
    }

    @Override
    public void updateConsumedOffset(long offset) {
        this.consumedOffset = offset;
    }

    @Override
    public long getConsumedOffset() {
        return consumedOffset;
    }
}
