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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.consumer.BatchMessageListener;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A FIFO-aware batch consume service that extends {@link BatchConsumeService} with ordering guarantees.
 *
 * <p>Key FIFO semantics:
 * <ul>
 *     <li>Only one batch is in-flight at a time — the next batch is not dispatched until the current one completes
 *     (either successfully acked or forwarded to DLQ).</li>
 *     <li>On failure, the <strong>entire batch</strong> is retried as a whole until max attempts are exhausted.</li>
 *     <li>When max attempts are exhausted, all messages in the batch are forwarded to the dead letter queue.</li>
 * </ul>
 *
 * <p><strong>Warning:</strong> The global buffer mixes messages from different message groups. If a batch fails
 * and exhausts retries, all messages in that batch (including messages from multiple message groups) will be
 * forwarded to DLQ together. This design prioritizes throughput over strict isolation between message groups.
 */
@SuppressWarnings({"NullableProblems", "UnstableApiUsage"})
public class FifoBatchConsumeService extends BatchConsumeService {
    private static final Logger log = LoggerFactory.getLogger(FifoBatchConsumeService.class);

    private volatile boolean batchInFlight = false;

    /**
     * Creates a new FIFO batch consume service.
     *
     * @param clientId             the client identifier.
     * @param batchMessageListener the batch message listener.
     * @param batchPolicy          the batching policy.
     * @param consumptionExecutor  the thread pool for executing batch consume tasks.
     * @param consumer             the push consumer instance.
     * @param scheduler            the scheduler for timer-based flush and retry delays.
     */
    public FifoBatchConsumeService(ClientId clientId, BatchMessageListener batchMessageListener,
        BatchPolicy batchPolicy, ThreadPoolExecutor consumptionExecutor, PushConsumerImpl consumer,
        ScheduledExecutorService scheduler) {
        super(clientId, batchMessageListener, batchPolicy, consumptionExecutor, consumer, scheduler);
    }

    /**
     * Discards a corrupted message using FIFO-specific discard.
     */
    @Override
    protected void discardCorruptedMessage(ProcessQueue pq, MessageViewImpl messageView) {
        pq.discardFifoMessage(messageView);
    }

    /**
     * Overrides the standard flush logic to enforce single-batch-in-flight constraint.
     * Only flushes when no batch is currently being processed. Extracts one batch and submits it.
     */
    @Override
    protected void tryFlush() {
        if (!isBufferEmpty() && !batchInFlight) {
            batchInFlight = true;
            List<BufferedMessage> batch = extractBatch();
            if (batch.isEmpty()) {
                batchInFlight = false;
                return;
            }
            submitBatch(batch, 1);
        }
    }

    /**
     * Handles the batch result with FIFO retry semantics.
     *
     * <p>On success: acks all messages via {@link ProcessQueue#eraseFifoMessage} with SUCCESS.
     * <p>On failure with remaining attempts: schedules a retry of the entire batch after a delay.
     * <p>On failure with attempts exhausted: forwards all messages to DLQ.
     */
    @Override
    protected void handleResult(List<BufferedMessage> batch, ConsumeResult result, int attempt) {
        if (ConsumeResult.SUCCESS.equals(result)) {
            for (BufferedMessage bm : batch) {
                bm.pq.eraseFifoMessage(bm.messageView, ConsumeResult.SUCCESS);
            }
            getConsumer().consumptionOkQuantity.addAndGet(batch.size());
            releaseFifoLock();
            return;
        }
        final RetryPolicy retryPolicy = getConsumer().getRetryPolicy();
        final int maxAttempts = retryPolicy.getMaxAttempts();
        if (attempt < maxAttempts) {
            final Duration delay = retryPolicy.getNextAttemptDelay(attempt);
            log.debug("FIFO batch consumption failed, will retry the whole batch, attempt={}, maxAttempts={}, "
                + "delay={}, batchSize={}, clientId={}", attempt, maxAttempts, delay, batch.size(), clientId);
            try {
                getScheduler().schedule(() -> submitBatch(batch, attempt + 1),
                    delay.toNanos(), TimeUnit.NANOSECONDS);
            } catch (Throwable t) {
                if (getScheduler().isShutdown()) {
                    releaseFifoLock();
                    return;
                }
                log.error("[Bug] Failed to schedule FIFO batch retry, clientId={}", clientId, t);
                discardBatchToDeadLetterQueue(batch);
            }
        } else {
            log.info("FIFO batch consumption failed, run out of attempt times, maxAttempts={}, attempt={}, "
                + "batchSize={}, clientId={}", maxAttempts, attempt, batch.size(), clientId);
            discardBatchToDeadLetterQueue(batch);
        }
    }

    /**
     * Forwards all messages in the batch to the dead letter queue and releases the FIFO lock.
     *
     * @param batch the batch of messages to discard.
     */
    private void discardBatchToDeadLetterQueue(List<BufferedMessage> batch) {
        getConsumer().consumptionErrorQuantity.addAndGet(batch.size());
        for (BufferedMessage bm : batch) {
            bm.pq.discardFifoMessage(bm.messageView);
        }
        releaseFifoLock();
    }

    /**
     * Releases the FIFO in-flight lock and triggers a flush attempt for any pending buffered messages.
     */
    private void releaseFifoLock() {
        batchInFlight = false;
        lockAndTryFlush();
    }

    /**
     * Closes the FIFO batch consume service by draining remaining buffered messages one batch at a time.
     * Unlike the parent's close() which submits all batches concurrently, this override extracts and
     * submits batches sequentially to preserve FIFO ordering during shutdown. Any pending retries
     * scheduled via the scheduler will be awaited by the caller's executor shutdown.
     */
    @Override
    public void close() {
        java.util.List<com.google.common.util.concurrent.ListenableFuture<ConsumeResult>> pendingFutures
            = new java.util.ArrayList<>();
        bufferLock.lock();
        try {
            cancelTimer();
            batchInFlight = true;
            while (!buffer.isEmpty()) {
                log.info("FIFO draining remaining messages during shutdown, clientId={}, remaining={}",
                    clientId, buffer.size());
                List<BufferedMessage> batch = extractBatch();
                if (!batch.isEmpty()) {
                    pendingFutures.add(submitBatch(batch, 1));
                }
            }
        } finally {
            bufferLock.unlock();
        }
        // Wait for all submitted batches to complete
        for (com.google.common.util.concurrent.ListenableFuture<ConsumeResult> future : pendingFutures) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                log.warn("Timeout waiting for FIFO batch during shutdown, clientId={}", clientId);
            } catch (Exception e) {
                log.error("Exception waiting for FIFO batch during shutdown, clientId={}", clientId, e);
            }
        }
    }
}
