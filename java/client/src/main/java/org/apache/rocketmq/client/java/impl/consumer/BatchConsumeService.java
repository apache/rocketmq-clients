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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.client.apis.consumer.BatchMessageListener;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A consume service that buffers messages from all process queues and dispatches them in batches
 * to a {@link BatchMessageListener}.
 *
 * <p>Messages are accumulated without distinguishing their source process queue. A batch is flushed when any of
 * the following conditions is met:
 * <ul>
 *     <li>The number of buffered messages reaches {@link BatchPolicy#getMaxBatchSize()}</li>
 *     <li>The cumulative byte size reaches {@link BatchPolicy#getMaxBatchBytes()}</li>
 *     <li>The time since the first message entered the buffer reaches {@link BatchPolicy#getMaxWaitTime()}</li>
 * </ul>
 *
 * <p>This implementation supports Standard (non-FIFO) mode where multiple batches can be in-flight concurrently.
 * For FIFO mode, use {@link FifoBatchConsumeService} which extends this class.
 */
@SuppressWarnings({"NullableProblems", "UnstableApiUsage"})
public class BatchConsumeService extends ConsumeService {
    private static final Logger log = LoggerFactory.getLogger(BatchConsumeService.class);

    protected final BatchMessageListener batchMessageListener;
    protected final BatchPolicy batchPolicy;
    protected final PushConsumerImpl consumer;
    protected final ReentrantLock bufferLock = new ReentrantLock();
    protected final ArrayDeque<BufferedMessage> buffer = new ArrayDeque<>();
    protected long bufferBytes = 0;
    protected long firstArrivalNanos = 0;
    protected ScheduledFuture<?> timerFuture;

    /**
     * Creates a new batch consume service for Standard (non-FIFO) mode.
     *
     * @param clientId             the client identifier.
     * @param batchMessageListener the batch message listener.
     * @param batchPolicy          the batching policy.
     * @param consumptionExecutor  the thread pool for executing batch consume tasks.
     * @param consumer             the push consumer instance.
     * @param scheduler            the scheduler for timer-based flush.
     */
    public BatchConsumeService(ClientId clientId, BatchMessageListener batchMessageListener, BatchPolicy batchPolicy,
        ThreadPoolExecutor consumptionExecutor, PushConsumerImpl consumer, ScheduledExecutorService scheduler) {
        super(clientId, consumptionExecutor, consumer, scheduler);
        this.batchMessageListener = batchMessageListener;
        this.batchPolicy = batchPolicy;
        this.consumer = consumer;
    }

    /**
     * Accepts messages from a process queue and adds them to the shared buffer.
     * Corrupted messages are discarded immediately. After buffering, checks if a flush should be triggered.
     *
     * @param pq           the source process queue.
     * @param messageViews the messages to buffer.
     */
    @Override
    public void consume(ProcessQueue pq, List<MessageViewImpl> messageViews) {
        bufferLock.lock();
        try {
            for (MessageViewImpl messageView : messageViews) {
                if (messageView.isCorrupted()) {
                    log.error("Message is corrupted for batch consumption, prepare to discard it, mq={}, "
                        + "messageId={}, clientId={}", pq.getMessageQueue(), messageView.getMessageId(), clientId);
                    discardCorruptedMessage(pq, messageView);
                    continue;
                }
                BufferedMessage bm = new BufferedMessage(pq, messageView);
                buffer.addLast(bm);
                bufferBytes += bm.bodySize;
                if (buffer.size() == 1) {
                    firstArrivalNanos = bm.arrivalNanos;
                    scheduleTimer();
                }
            }
            tryFlush();
        } finally {
            bufferLock.unlock();
        }
    }

    /**
     * Discards a corrupted message. Subclasses may override for FIFO-specific discard behavior.
     *
     * @param pq          the source process queue.
     * @param messageView the corrupted message.
     */
    protected void discardCorruptedMessage(ProcessQueue pq, MessageViewImpl messageView) {
        pq.discardMessage(messageView);
    }

    /**
     * Checks if the buffer should be flushed based on the batch policy conditions.
     * Subclasses may override to add additional constraints (e.g., FIFO single-batch-in-flight check).
     * Must be called while holding the buffer lock.
     */
    protected void tryFlush() {
        while (!buffer.isEmpty() && shouldFlush()) {
            doFlush();
        }
    }

    /**
     * Determines whether the current buffer state warrants a flush.
     * Must be called while holding the buffer lock.
     *
     * @return true if the buffer should be flushed.
     */
    private boolean shouldFlush() {
        return buffer.size() >= batchPolicy.getMaxBatchSize()
            || bufferBytes >= batchPolicy.getMaxBatchBytes()
            || (System.nanoTime() - firstArrivalNanos) >= batchPolicy.getMaxWaitTime().toNanos();
    }

    /**
     * Extracts a batch from the buffer respecting both maxBatchSize and maxBatchBytes constraints.
     * Guarantees forward progress: if the first message alone exceeds maxBatchBytes, it still forms a batch of one.
     * Must be called while holding the buffer lock.
     *
     * @return the extracted batch of messages.
     */
    protected List<BufferedMessage> extractBatch() {
        final List<BufferedMessage> batch = new ArrayList<>();
        long batchBytes = 0;
        int maxSize = batchPolicy.getMaxBatchSize();
        int maxBytes = batchPolicy.getMaxBatchBytes();

        while (!buffer.isEmpty() && batch.size() < maxSize) {
            BufferedMessage candidate = buffer.peekFirst();
            if (!batch.isEmpty() && batchBytes + candidate.bodySize > maxBytes) {
                break;
            }
            buffer.pollFirst();
            batch.add(candidate);
            batchBytes += candidate.bodySize;
            bufferBytes -= candidate.bodySize;
        }

        if (buffer.isEmpty()) {
            firstArrivalNanos = 0;
            cancelTimer();
        } else {
            firstArrivalNanos = buffer.peekFirst().arrivalNanos;
            scheduleTimer();
        }
        return batch;
    }

    /**
     * Flushes a batch from the buffer and submits it for consumption.
     * Must be called while holding the buffer lock.
     */
    private void doFlush() {
        final List<BufferedMessage> batch = extractBatch();
        if (batch.isEmpty()) {
            return;
        }
        submitBatch(batch, 1);
    }

    /**
     * Submits a batch for consumption.
     *
     * @param batch   the batch of buffered messages.
     * @param attempt the current attempt number (starting from 1).
     * @return a future that completes after the batch consumption and result handling are done.
     */
    protected ListenableFuture<ConsumeResult> submitBatch(List<BufferedMessage> batch, int attempt) {
        final List<MessageViewImpl> messageViews = new ArrayList<>(batch.size());
        for (BufferedMessage bm : batch) {
            messageViews.add(bm.messageView);
        }
        final BatchConsumeTask task = new BatchConsumeTask(clientId, batchMessageListener,
            messageViews, getMessageInterceptor());
        final ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(getConsumptionExecutor());
        final ListenableFuture<ConsumeResult> future = executorService.submit(task);
        final SettableFuture<ConsumeResult> resultFuture = SettableFuture.create();
        Futures.addCallback(future, new FutureCallback<ConsumeResult>() {
            @Override
            public void onSuccess(ConsumeResult result) {
                try {
                    handleResult(batch, result, attempt);
                    resultFuture.set(result);
                } catch (Throwable t) {
                    resultFuture.setException(t);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("[Bug] Exception raised while submitting batch consumption task, clientId={}",
                    clientId, t);
                try {
                    handleResult(batch, ConsumeResult.FAILURE, attempt);
                } catch (Throwable ignored) {
                    // Swallow to ensure resultFuture is always completed.
                }
                resultFuture.set(ConsumeResult.FAILURE);
            }
        }, MoreExecutors.directExecutor());
        return resultFuture;
    }

    /**
     * Handles the batch consumption result. In Standard mode, dispatches ack or nack for each message individually.
     * Subclasses may override to implement different behavior (e.g., FIFO retry logic).
     *
     * @param batch   the batch of messages.
     * @param result  the consumption result.
     * @param attempt the current attempt number.
     */
    protected void handleResult(List<BufferedMessage> batch, ConsumeResult result, int attempt) {
        for (BufferedMessage bm : batch) {
            bm.pq.eraseMessage(bm.messageView, result);
        }
        if (ConsumeResult.SUCCESS.equals(result)) {
            consumer.consumptionOkQuantity.addAndGet(batch.size());
        } else {
            consumer.consumptionErrorQuantity.addAndGet(batch.size());
        }
    }

    /**
     * Acquires the buffer lock and invokes {@link #tryFlush()}. This is intended for use by subclasses
     * that need to trigger a flush from outside the normal consume path (e.g., after releasing an in-flight lock).
     */
    protected void lockAndTryFlush() {
        bufferLock.lock();
        try {
            tryFlush();
        } finally {
            bufferLock.unlock();
        }
    }

    /**
     * Schedules a timer to trigger a flush after {@link BatchPolicy#getMaxWaitTime()}.
     * Must be called while holding the buffer lock.
     */
    private void scheduleTimer() {
        cancelTimer();
        long elapsed = System.nanoTime() - firstArrivalNanos;
        long remainingNanos = batchPolicy.getMaxWaitTime().toNanos() - elapsed;
        if (remainingNanos < 0) {
            remainingNanos = 0;
        }
        try {
            timerFuture = getScheduler().schedule(() -> {
                bufferLock.lock();
                try {
                    tryFlush();
                } finally {
                    bufferLock.unlock();
                }
            }, remainingNanos, TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (!getScheduler().isShutdown()) {
                log.error("[Bug] Failed to schedule batch timer, clientId={}", clientId, t);
            }
        }
    }

    /**
     * Cancels the pending timer if one exists. Must be called while holding the buffer lock.
     */
    protected void cancelTimer() {
        if (timerFuture != null) {
            timerFuture.cancel(false);
            timerFuture = null;
        }
    }

    /**
     * Closes this batch consume service by flushing any remaining buffered messages immediately.
     * This should be called before the consumption executor is shut down to ensure no messages are lost.
     * The method waits for all submitted batches to complete before returning.
     */
    @Override
    public void close() {
        List<ListenableFuture<ConsumeResult>> pendingFutures = new ArrayList<>();
        bufferLock.lock();
        try {
            cancelTimer();
            while (!buffer.isEmpty()) {
                log.info("Flushing remaining {} messages in batch buffer during shutdown, clientId={}",
                    buffer.size(), clientId);
                final List<BufferedMessage> batch = extractBatch();
                if (!batch.isEmpty()) {
                    pendingFutures.add(submitBatch(batch, 1));
                }
            }
        } finally {
            bufferLock.unlock();
        }
        // Wait for all flushed batches to complete before returning
        for (ListenableFuture<ConsumeResult> future : pendingFutures) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                log.warn("Timeout waiting for batch consumption to complete during shutdown, clientId={}", clientId);
            } catch (Exception e) {
                log.error("Exception waiting for batch consumption to complete during shutdown, clientId={}",
                    clientId, e);
            }
        }
    }

    /**
     * Returns the batch policy.
     *
     * @return the batch policy.
     */
    protected BatchPolicy getBatchPolicy() {
        return batchPolicy;
    }

    /**
     * Returns the push consumer instance.
     *
     * @return the push consumer.
     */
    protected PushConsumerImpl getConsumer() {
        return consumer;
    }

    /**
     * Returns the batch message listener.
     *
     * @return the batch message listener.
     */
    protected BatchMessageListener getBatchMessageListener() {
        return batchMessageListener;
    }

    /**
     * Returns whether the buffer is currently empty. For use by subclasses to check buffer state.
     *
     * @return true if the buffer is empty.
     */
    protected boolean isBufferEmpty() {
        return buffer.isEmpty();
    }

    /**
     * A buffered message entry that tracks the source process queue, message, body size, and arrival time.
     * The body size is cached at construction to avoid repeated calls to {@code ByteBuffer.remaining()}
     * which could return inconsistent values if the buffer position changes.
     */
    static class BufferedMessage {
        final ProcessQueue pq;
        final MessageViewImpl messageView;
        final int bodySize;
        final long arrivalNanos;

        /**
         * Creates a new buffered message entry.
         *
         * @param pq          the source process queue.
         * @param messageView the message.
         */
        BufferedMessage(ProcessQueue pq, MessageViewImpl messageView) {
            this.pq = pq;
            this.messageView = messageView;
            this.bodySize = messageView.getBody().remaining();
            this.arrivalNanos = System.nanoTime();
        }
    }
}
