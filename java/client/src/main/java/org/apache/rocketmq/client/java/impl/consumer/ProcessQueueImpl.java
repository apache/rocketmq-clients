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

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.Status;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.exception.BadRequestException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContextImpl;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.GeneralMessageImpl;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link ProcessQueue}.
 *
 * <p>Apart from the basic part mentioned in {@link ProcessQueue}, this implementation
 *
 * @see ProcessQueue
 */
@SuppressWarnings({"NullableProblems", "UnstableApiUsage"})
class ProcessQueueImpl implements ProcessQueue {
    static final Duration FORWARD_FIFO_MESSAGE_TO_DLQ_FAILURE_BACKOFF_DELAY = Duration.ofSeconds(1);
    static final Duration ACK_MESSAGE_FAILURE_BACKOFF_DELAY = Duration.ofSeconds(1);
    static final Duration CHANGE_INVISIBLE_DURATION_FAILURE_BACKOFF_DELAY = Duration.ofSeconds(1);

    private static final Logger log = LoggerFactory.getLogger(ProcessQueueImpl.class);

    private static final Duration RECEIVING_FLOW_CONTROL_BACKOFF_DELAY = Duration.ofMillis(20);
    private static final Duration RECEIVING_FAILURE_BACKOFF_DELAY = Duration.ofSeconds(1);
    private static final Duration RECEIVING_BACKOFF_DELAY_WHEN_CACHE_IS_FULL = Duration.ofSeconds(1);

    private final PushConsumerImpl consumer;

    /**
     * Dropped means {@link ProcessQueue} is deprecated, which means no message would be fetched from remote anymore.
     */
    private volatile boolean dropped;
    private final MessageQueueImpl mq;
    private final FilterExpression filterExpression;

    /**
     * Messages which is pending means have been cached, but are not taken by consumer dispatcher yet.
     */
    @GuardedBy("cachedMessageLock")
    private final List<MessageViewImpl> cachedMessages;
    private final ReadWriteLock cachedMessageLock;

    private final AtomicLong cachedMessagesBytes;

    private final AtomicLong receptionTimes;
    private final AtomicLong receivedMessagesQuantity;

    private volatile long activityNanoTime = System.nanoTime();
    private volatile long cacheFullNanoTime = Long.MIN_VALUE;

    public ProcessQueueImpl(PushConsumerImpl consumer, MessageQueueImpl mq, FilterExpression filterExpression) {
        this.consumer = consumer;
        this.dropped = false;
        this.mq = mq;
        this.filterExpression = filterExpression;
        this.cachedMessages = new ArrayList<>();
        this.cachedMessageLock = new ReentrantReadWriteLock();
        this.cachedMessagesBytes = new AtomicLong();
        this.receptionTimes = new AtomicLong(0);
        this.receivedMessagesQuantity = new AtomicLong(0);
    }

    @Override
    public MessageQueueImpl getMessageQueue() {
        return mq;
    }

    @Override
    public void drop() {
        this.dropped = true;
    }

    @Override
    public boolean expired() {
        final Duration longPollingTimeout = consumer.getPushConsumerSettings().getLongPollingTimeout();
        final Duration requestTimeout = consumer.getClientConfiguration().getRequestTimeout();
        final Duration maxIdleDuration = longPollingTimeout.plus(requestTimeout).multipliedBy(3);
        final Duration idleDuration = Duration.ofNanos(System.nanoTime() - activityNanoTime);
        if (idleDuration.compareTo(maxIdleDuration) < 0) {
            return false;
        }
        final Duration afterCacheFullDuration = Duration.ofNanos(System.nanoTime() - cacheFullNanoTime);
        if (afterCacheFullDuration.compareTo(maxIdleDuration) < 0) {
            return false;
        }
        log.warn("Process queue is idle, idleDuration={}, maxIdleDuration={}, afterCacheFullDuration={}, mq={}, "
            + "clientId={}", idleDuration, maxIdleDuration, afterCacheFullDuration, mq, consumer.getClientId());
        return true;
    }

    void cacheMessages(List<MessageViewImpl> messageList) {
        cachedMessageLock.writeLock().lock();
        try {
            for (MessageViewImpl messageView : messageList) {
                cachedMessages.add(messageView);
                cachedMessagesBytes.addAndGet(messageView.getBody().remaining());
            }
        } finally {
            cachedMessageLock.writeLock().unlock();
        }
    }

    private int getReceptionBatchSize() {
        int bufferSize = consumer.cacheMessageCountThresholdPerQueue() - this.cachedMessagesCount();
        bufferSize = Math.max(bufferSize, 1);
        return Math.min(bufferSize, consumer.getPushConsumerSettings().getReceiveBatchSize());
    }

    @Override
    public void fetchMessageImmediately() {
        receiveMessageImmediately();
    }

    /**
     * Receive message later by message queue.
     *
     * <p> Make sure that no exception will be thrown.
     */
    public void onReceiveMessageException(Throwable t) {
        Duration delay = t instanceof TooManyRequestsException ? RECEIVING_FLOW_CONTROL_BACKOFF_DELAY :
            RECEIVING_FAILURE_BACKOFF_DELAY;
        receiveMessageLater(delay);
    }

    private void receiveMessageLater(Duration delay) {
        final ClientId clientId = consumer.getClientId();
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            log.info("Try to receive message later, mq={}, delay={}, clientId={}", mq, delay, clientId);
            scheduler.schedule(this::receiveMessage, delay.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            log.error("[Bug] Failed to schedule message receiving request, mq={}, clientId={}", mq, clientId, t);
            onReceiveMessageException(t);
        }
    }

    public void receiveMessage() {
        final ClientId clientId = consumer.getClientId();
        if (dropped) {
            log.info("Process queue has been dropped, no longer receive message, mq={}, clientId={}", mq, clientId);
            return;
        }
        if (this.isCacheFull()) {
            log.warn("Process queue cache is full, would receive message later, mq={}, clientId={}", mq, clientId);
            receiveMessageLater(RECEIVING_BACKOFF_DELAY_WHEN_CACHE_IS_FULL);
            return;
        }
        receiveMessageImmediately();
    }

    private void receiveMessageImmediately() {
        final ClientId clientId = consumer.getClientId();
        if (!consumer.isRunning()) {
            log.info("Stop to receive message because consumer is not running, mq={}, clientId={}", mq, clientId);
            return;
        }
        try {
            final Endpoints endpoints = mq.getBroker().getEndpoints();
            final int batchSize = this.getReceptionBatchSize();
            final ReceiveMessageRequest request = consumer.wrapReceiveMessageRequest(batchSize, mq, filterExpression);
            activityNanoTime = System.nanoTime();

            // Intercept before message reception.
            final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE);
            consumer.doBefore(context, Collections.emptyList());

            final ListenableFuture<ReceiveMessageResult> future = consumer.receiveMessage(request, mq,
                consumer.getPushConsumerSettings().getLongPollingTimeout());
            Futures.addCallback(future, new FutureCallback<ReceiveMessageResult>() {
                @Override
                public void onSuccess(ReceiveMessageResult result) {
                    // Intercept after message reception.
                    final List<GeneralMessage> generalMessages = result.getMessageViewImpls().stream()
                        .map((Function<MessageView, GeneralMessage>) GeneralMessageImpl::new)
                        .collect(Collectors.toList());
                    final MessageInterceptorContextImpl context0 =
                        new MessageInterceptorContextImpl(context, MessageHookPointsStatus.OK);
                    consumer.doAfter(context0, generalMessages);

                    try {
                        onReceiveMessageResult(result);
                    } catch (Throwable t) {
                        // Should never reach here.
                        log.error("[Bug] Exception raised while handling receive result, mq={}, endpoints={}, "
                            + "clientId={}", mq, endpoints, clientId, t);
                        onReceiveMessageException(t);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    // Intercept after message reception.
                    final MessageInterceptorContextImpl context0 =
                        new MessageInterceptorContextImpl(context, MessageHookPointsStatus.ERROR);
                    consumer.doAfter(context0, Collections.emptyList());

                    log.error("Exception raised during message reception, mq={}, endpoints={}, clientId={}", mq,
                        endpoints, clientId, t);
                    onReceiveMessageException(t);
                }
            }, MoreExecutors.directExecutor());
            receptionTimes.getAndIncrement();
            consumer.getReceptionTimes().getAndIncrement();
        } catch (Throwable t) {
            log.error("Exception raised during message reception, mq={}, clientId={}", mq, clientId, t);
            onReceiveMessageException(t);
        }
    }

    public boolean isCacheFull() {
        final int cacheMessageCountThresholdPerQueue = consumer.cacheMessageCountThresholdPerQueue();
        final long actualMessagesQuantity = this.cachedMessagesCount();
        final ClientId clientId = consumer.getClientId();
        if (cacheMessageCountThresholdPerQueue <= actualMessagesQuantity) {
            log.warn("Process queue total cached messages quantity exceeds the threshold, threshold={}, actual={}," +
                " mq={}, clientId={}", cacheMessageCountThresholdPerQueue, actualMessagesQuantity, mq, clientId);
            cacheFullNanoTime = System.nanoTime();
            return true;
        }
        final int cacheMessageBytesThresholdPerQueue = consumer.cacheMessageBytesThresholdPerQueue();
        final long actualCachedMessagesBytes = this.cachedMessageBytes();
        if (cacheMessageBytesThresholdPerQueue <= actualCachedMessagesBytes) {
            log.warn("Process queue total cached messages memory exceeds the threshold, threshold={} bytes," +
                    " actual={} bytes, mq={}, clientId={}", cacheMessageBytesThresholdPerQueue,
                actualCachedMessagesBytes, mq, clientId);
            cacheFullNanoTime = System.nanoTime();
            return true;
        }
        return false;
    }

    @Override
    public void discardMessage(MessageViewImpl messageView) {
        log.info("Discard message, mq={}, messageId={}, clientId={}", mq, messageView.getMessageId(),
            consumer.getClientId());
        final ListenableFuture<Void> future = nackMessage(messageView);
        future.addListener(() -> evictCache(messageView), MoreExecutors.directExecutor());
    }

    @Override
    public void discardFifoMessage(MessageViewImpl messageView) {
        log.info("Discard fifo message, mq={}, messageId={}, clientId={}", mq, messageView.getMessageId(),
            consumer.getClientId());
        final ListenableFuture<Void> future = forwardToDeadLetterQueue(messageView);
        future.addListener(() -> evictCache(messageView), MoreExecutors.directExecutor());
    }

    public int cachedMessagesCount() {
        cachedMessageLock.readLock().lock();
        try {
            return cachedMessages.size();
        } finally {
            cachedMessageLock.readLock().unlock();
        }
    }

    public long cachedMessageBytes() {
        return cachedMessagesBytes.get();
    }

    private void onReceiveMessageResult(ReceiveMessageResult result) {
        final List<MessageViewImpl> messages = result.getMessageViewImpls();
        if (!messages.isEmpty()) {
            cacheMessages(messages);
            receivedMessagesQuantity.getAndAdd(messages.size());
            consumer.getReceivedMessagesQuantity().getAndAdd(messages.size());
            consumer.getConsumeService().consume(this, messages);
        }
        receiveMessage();
    }

    private void evictCache(MessageViewImpl messageView) {
        cachedMessageLock.writeLock().lock();
        try {
            if (cachedMessages.remove(messageView)) {
                cachedMessagesBytes.addAndGet(-messageView.getBody().remaining());
            }
        } finally {
            cachedMessageLock.writeLock().unlock();
        }
    }

    private void statsConsumptionResult(ConsumeResult consumeResult) {
        if (ConsumeResult.SUCCESS.equals(consumeResult)) {
            consumer.consumptionOkQuantity.incrementAndGet();
            return;
        }
        consumer.consumptionErrorQuantity.incrementAndGet();
    }

    @Override
    public void eraseMessage(MessageViewImpl messageView, ConsumeResult consumeResult) {
        statsConsumptionResult(consumeResult);
        ListenableFuture<Void> future = ConsumeResult.SUCCESS.equals(consumeResult) ? ackMessage(messageView) :
            nackMessage(messageView);
        future.addListener(() -> evictCache(messageView), MoreExecutors.directExecutor());
    }

    private ListenableFuture<Void> nackMessage(final MessageViewImpl messageView) {
        final int deliveryAttempt = messageView.getDeliveryAttempt();
        final Duration duration = consumer.getRetryPolicy().getNextAttemptDelay(deliveryAttempt);
        final SettableFuture<Void> future0 = SettableFuture.create();
        changeInvisibleDuration(messageView, duration, 1, future0);
        return future0;
    }

    private void changeInvisibleDuration(final MessageViewImpl messageView, final Duration duration,
        final int attempt, final SettableFuture<Void> future0) {
        final ClientId clientId = consumer.getClientId();
        final String consumerGroup = consumer.getConsumerGroup();
        final MessageId messageId = messageView.getMessageId();
        final Endpoints endpoints = messageView.getEndpoints();
        final RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> future =
            consumer.changeInvisibleDuration(messageView, duration);
        Futures.addCallback(future, new FutureCallback<ChangeInvisibleDurationResponse>() {
            @Override
            public void onSuccess(ChangeInvisibleDurationResponse response) {
                final String requestId = future.getContext().getRequestId();
                final Status status = response.getStatus();
                final Code code = status.getCode();
                if (Code.INVALID_RECEIPT_HANDLE.equals(code)) {
                    log.error("Failed to change invisible duration due to the invalid receipt handle, forgive to "
                            + "retry, clientId={}, consumerGroup={}, messageId={}, attempt={}, mq={}, endpoints={}, "
                            + "requestId={}, status message=[{}]", clientId, consumerGroup, messageId, attempt, mq,
                        endpoints, requestId, status.getMessage());
                    future0.setException(new BadRequestException(code.getNumber(), requestId, status.getMessage()));
                    return;
                }
                // Log failure and retry later.
                if (!Code.OK.equals(code)) {
                    log.error("Failed to change invisible duration, would retry later, clientId={}, "
                            + "consumerGroup={}, messageId={}, attempt={}, mq={}, endpoints={}, requestId={}, "
                            + "status message=[{}]", clientId, consumerGroup, messageId, attempt, mq, endpoints,
                        requestId, status.getMessage());
                    changeInvisibleDurationLater(messageView, duration, 1 + attempt, future0);
                    return;
                }
                // Set result if succeed in changing invisible time.
                future0.setFuture(Futures.immediateVoidFuture());
                // Log retries.
                if (1 < attempt) {
                    log.info("Finally, change invisible duration successfully, clientId={}, consumerGroup={} "
                            + "messageId={}, attempt={}, mq={}, endpoints={}, requestId={}", clientId, consumerGroup,
                        messageId, attempt, mq, endpoints, requestId);
                    return;
                }
                log.debug("Change invisible duration successfully, clientId={}, consumerGroup={}, messageId={}, "
                        + "mq={}, endpoints={}, requestId={}", clientId, consumerGroup, messageId, mq, endpoints,
                    requestId);
            }

            @Override
            public void onFailure(Throwable t) {
                // Log failure and retry later.
                log.error("Exception raised while changing invisible duration, would retry later, clientId={}, "
                        + "consumerGroup={}, messageId={}, mq={}, endpoints={}", clientId, consumerGroup,
                    messageId, mq, endpoints, t);
                changeInvisibleDurationLater(messageView, duration, 1 + attempt, future0);
            }
        }, MoreExecutors.directExecutor());
    }

    private void changeInvisibleDurationLater(final MessageViewImpl messageView, final Duration duration,
        final int attempt, SettableFuture<Void> future) {
        final MessageId messageId = messageView.getMessageId();
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            scheduler.schedule(() -> changeInvisibleDuration(messageView, duration, attempt, future),
                CHANGE_INVISIBLE_DURATION_FAILURE_BACKOFF_DELAY.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            log.error("[Bug] Failed to schedule message change invisible duration request, mq={}, messageId={}, "
                + "clientId={}", mq, messageId, consumer.getClientId());
            changeInvisibleDurationLater(messageView, duration, 1 + attempt, future);
        }
    }

    @Override
    public ListenableFuture<Void> eraseFifoMessage(MessageViewImpl messageView, ConsumeResult consumeResult) {
        statsConsumptionResult(consumeResult);
        final RetryPolicy retryPolicy = consumer.getRetryPolicy();
        final int maxAttempts = retryPolicy.getMaxAttempts();
        int attempt = messageView.getDeliveryAttempt();
        final MessageId messageId = messageView.getMessageId();
        final ConsumeService service = consumer.getConsumeService();
        final ClientId clientId = consumer.getClientId();
        if (ConsumeResult.FAILURE.equals(consumeResult) && attempt < maxAttempts) {
            final Duration nextAttemptDelay = retryPolicy.getNextAttemptDelay(attempt);
            attempt = messageView.incrementAndGetDeliveryAttempt();
            log.debug("Prepare to redeliver the fifo message because of the consumption failure, maxAttempt={}," +
                    " attempt={}, mq={}, messageId={}, nextAttemptDelay={}, clientId={}", maxAttempts, attempt, mq,
                messageId, nextAttemptDelay, clientId);
            final ListenableFuture<ConsumeResult> future = service.consume(messageView, nextAttemptDelay);
            return Futures.transformAsync(future, result -> eraseFifoMessage(messageView, result),
                MoreExecutors.directExecutor());
        }
        boolean ok = ConsumeResult.SUCCESS.equals(consumeResult);
        if (!ok) {
            log.info("Failed to consume fifo message finally, run out of attempt times, maxAttempts={}, "
                + "attempt={}, mq={}, messageId={}, clientId={}", maxAttempts, attempt, mq, messageId, clientId);
        }
        // Ack message or forward it to DLQ depends on consumption result.
        ListenableFuture<Void> future = ok ? ackMessage(messageView) : forwardToDeadLetterQueue(messageView);
        future.addListener(() -> evictCache(messageView), consumer.getConsumptionExecutor());
        return future;
    }


    private ListenableFuture<Void> forwardToDeadLetterQueue(final MessageViewImpl messageView) {
        final SettableFuture<Void> future = SettableFuture.create();
        forwardToDeadLetterQueue(messageView, 1, future);
        return future;
    }

    private void forwardToDeadLetterQueue(final MessageViewImpl messageView, final int attempt,
        final SettableFuture<Void> future0) {
        final RpcFuture<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse> future =
            consumer.forwardMessageToDeadLetterQueue(messageView);
        final ClientId clientId = consumer.getClientId();
        final String consumerGroup = consumer.getConsumerGroup();
        final MessageId messageId = messageView.getMessageId();
        final Endpoints endpoints = messageView.getEndpoints();
        Futures.addCallback(future, new FutureCallback<ForwardMessageToDeadLetterQueueResponse>() {
            @Override
            public void onSuccess(ForwardMessageToDeadLetterQueueResponse response) {
                final String requestId = future.getContext().getRequestId();
                final Status status = response.getStatus();
                final Code code = status.getCode();
                // Log failure and retry later.
                if (!Code.OK.equals(code)) {
                    log.error("Failed to forward message to dead letter queue, would attempt to re-forward later," +
                            " clientId={}, consumerGroup={}, messageId={}, attempt={}, mq={}, endpoints={}, "
                            + "requestId={}, code={}, status message={}", clientId, consumerGroup, messageId, attempt,
                        mq, endpoints, requestId, code, status.getMessage());
                    forwardToDeadLetterQueueLater(messageView, 1 + attempt, future0);
                    return;
                }
                // Set result if message is forwarded successfully.
                future0.setFuture(Futures.immediateVoidFuture());
                // Log retries.
                if (1 < attempt) {
                    log.info("Re-forward message to dead letter queue successfully, clientId={}, consumerGroup={}, "
                            + "attempt={}, messageId={}, mq={}, endpoints={}, requestId={}", clientId, consumerGroup,
                        attempt, messageId, mq, endpoints, requestId);
                    return;
                }
                log.info("Forward message to dead letter queue successfully, clientId={}, consumerGroup={}, "
                        + "messageId={}, mq={}, endpoints={}, requestId={}", clientId, consumerGroup, messageId, mq,
                    endpoints, requestId);
            }

            @Override
            public void onFailure(Throwable t) {
                // Log failure and retry later.
                log.error("Exception raised while forward message to DLQ, would attempt to re-forward later, " +
                        "clientId={}, consumerGroup={}, attempt={}, messageId={}, mq={}", clientId, consumerGroup,
                    attempt, messageId, mq, t);
                forwardToDeadLetterQueueLater(messageView, 1 + attempt, future0);
            }
        }, MoreExecutors.directExecutor());
    }

    private void forwardToDeadLetterQueueLater(final MessageViewImpl messageView, final int attempt,
        final SettableFuture<Void> future0) {
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            scheduler.schedule(() -> forwardToDeadLetterQueue(messageView, attempt, future0),
                FORWARD_FIFO_MESSAGE_TO_DLQ_FAILURE_BACKOFF_DELAY.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            log.error("[Bug] Failed to schedule DLQ message request, mq={}, messageId={}, clientId={}", mq,
                messageView.getMessageId(), consumer.getClientId());
            forwardToDeadLetterQueueLater(messageView, 1 + attempt, future0);
        }
    }

    private ListenableFuture<Void> ackMessage(final MessageViewImpl messageView) {
        SettableFuture<Void> future = SettableFuture.create();
        ackMessage(messageView, 1, future);
        return future;
    }

    private void ackMessage(final MessageViewImpl messageView, final int attempt, final SettableFuture<Void> future0) {
        final ClientId clientId = consumer.getClientId();
        final String consumerGroup = consumer.getConsumerGroup();
        final MessageId messageId = messageView.getMessageId();
        final Endpoints endpoints = messageView.getEndpoints();
        final RpcFuture<AckMessageRequest, AckMessageResponse> future =
            consumer.ackMessage(messageView);
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                final String requestId = future.getContext().getRequestId();
                final Status status = response.getStatus();
                final Code code = status.getCode();
                if (Code.INVALID_RECEIPT_HANDLE.equals(code)) {
                    log.error("Failed to ack message due to the invalid receipt handle, forgive to retry, "
                            + "clientId={}, consumerGroup={}, messageId={}, attempt={}, mq={}, endpoints={}, "
                            + "requestId={}, status message=[{}]", clientId, consumerGroup, messageId, attempt, mq,
                        endpoints, requestId, status.getMessage());
                    future0.setException(new BadRequestException(code.getNumber(), requestId, status.getMessage()));
                    return;
                }
                // Log failure and retry later.
                if (!Code.OK.equals(code)) {
                    log.error("Failed to ack message, would attempt to re-ack later, clientId={}, "
                            + "consumerGroup={}, attempt={}, messageId={}, mq={}, code={}, requestId={}, endpoints={}, "
                            + "status message=[{}]", clientId, consumerGroup, attempt, messageId, mq, code, requestId,
                        endpoints, status.getMessage());
                    ackMessageLater(messageView, 1 + attempt, future0);
                    return;
                }
                // Set result if FIFO message is acknowledged successfully.
                future0.setFuture(Futures.immediateVoidFuture());
                // Log retries.
                if (1 < attempt) {
                    log.info("Finally, ack message successfully, clientId={}, consumerGroup={}, attempt={}, "
                            + "messageId={}, mq={}, endpoints={}, requestId={}", clientId, consumerGroup, attempt,
                        messageId, mq, endpoints, requestId);
                    return;
                }
                log.debug("Ack message successfully, clientId={}, consumerGroup={}, messageId={}, mq={}, "
                    + "endpoints={}, requestId={}", clientId, consumerGroup, messageId, mq, endpoints, requestId);
            }

            @Override
            public void onFailure(Throwable t) {
                // Log failure and retry later.
                log.error("Exception raised while acknowledging message, clientId={}, consumerGroup={}, "
                        + "would attempt to re-ack later, attempt={}, messageId={}, mq={}, endpoints={}", clientId,
                    consumerGroup, attempt, messageId, mq, endpoints, t);
                ackMessageLater(messageView, 1 + attempt, future0);
            }
        }, MoreExecutors.directExecutor());
    }

    private void ackMessageLater(final MessageViewImpl messageView, final int attempt,
        final SettableFuture<Void> future) {
        final MessageId messageId = messageView.getMessageId();
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            scheduler.schedule(() -> ackMessage(messageView, attempt, future),
                ACK_MESSAGE_FAILURE_BACKOFF_DELAY.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            log.error("[Bug] Failed to schedule message ack request, mq={}, messageId={}, clientId={}",
                mq, messageId, consumer.getClientId());
            ackMessageLater(messageView, 1 + attempt, future);
        }
    }

    @Override
    public long getCachedMessageCount() {
        cachedMessageLock.readLock().lock();
        try {
            return cachedMessages.size();
        } finally {
            cachedMessageLock.readLock().unlock();
        }
    }

    @Override
    public long getCachedMessageBytes() {
        return cachedMessagesBytes.get();
    }

    public void doStats() {
        final long receptionTimes = this.receptionTimes.getAndSet(0);
        final long receivedMessagesQuantity = this.receivedMessagesQuantity.getAndSet(0);
        log.info("Process queue stats: clientId={}, mq={}, receptionTimes={}, receivedMessageQuantity={}, "
            + "cachedMessageCount={}, cachedMessageBytes={}", consumer.getClientId(), mq, receptionTimes,
            receivedMessagesQuantity, this.getCachedMessageCount(), this.getCachedMessageBytes());
    }
}
