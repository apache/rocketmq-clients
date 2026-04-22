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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.consumer.BatchMessageListener;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consume service that accumulates messages locally according to {@link BatchPolicy} before
 * dispatching them as a batch to the {@link BatchMessageListener}.
 *
 * <p>Messages received from the server are buffered in a single consumer-level cache. A batch is
 * flushed to the listener when <em>any</em> of the following conditions is met:
 * <ol>
 *   <li>The buffered message count reaches {@link BatchPolicy#getMaxBatchCount()}.</li>
 *   <li>The accumulated body bytes reach {@link BatchPolicy#getMaxBatchBytes()}.</li>
 *   <li>The elapsed time since the first buffered message reaches
 *       {@link BatchPolicy#getMaxWaitTime()}.</li>
 * </ol>
 *
 * <p>The {@link ConsumeResult} returned by the listener is applied to <strong>all</strong>
 * messages in the batch &mdash; either all succeed or all fail.
 */
@SuppressWarnings("NullableProblems")
public class BatchConsumeService extends ConsumeService {
    private static final Logger log = LoggerFactory.getLogger(BatchConsumeService.class);

    private final BatchMessageListener batchMessageListener;
    private final BatchPolicy batchPolicy;
    private final ThreadPoolExecutor batchConsumptionExecutor;
    private final MessageInterceptor batchMessageInterceptor;
    private final ScheduledExecutorService batchScheduler;

    /**
     * Single consumer-level accumulation buffer shared across all {@link ProcessQueue}s.
     */
    private final BatchBuffer buffer = new BatchBuffer();

    /**
     * Creates a new {@link BatchConsumeService}.
     *
     * @param clientId              the client identifier.
     * @param batchMessageListener  the batch message listener that will receive accumulated batches.
     * @param batchPolicy           the policy that controls when a batch is ready.
     * @param consumptionExecutor   the thread pool used to run {@link BatchConsumeTask}s.
     * @param messageInterceptor    the interceptor for message hooks.
     * @param scheduler             the scheduler used for {@code maxWaitTime} timeout flushes.
     */
    public BatchConsumeService(ClientId clientId, BatchMessageListener batchMessageListener,
        BatchPolicy batchPolicy, ThreadPoolExecutor consumptionExecutor,
        MessageInterceptor messageInterceptor, ScheduledExecutorService scheduler) {
        // Pass a no-op MessageListener to the parent; all batch logic is handled in this class.
        super(clientId, messageView -> ConsumeResult.FAILURE, consumptionExecutor, messageInterceptor, scheduler);
        this.batchMessageListener = batchMessageListener;
        this.batchPolicy = batchPolicy;
        this.batchConsumptionExecutor = consumptionExecutor;
        this.batchMessageInterceptor = messageInterceptor;
        this.batchScheduler = scheduler;
    }

    /**
     * Receives messages from the given {@link ProcessQueue}, buffers them, and flushes complete
     * batches according to the {@link BatchPolicy}.
     *
     * <p>Corrupted messages are discarded immediately and never enter the buffer.
     */
    @Override
    public void consume(ProcessQueue pq, List<MessageViewImpl> messageViews) {
        buffer.lock();
        try {
            for (MessageViewImpl messageView : messageViews) {
                if (messageView.isCorrupted()) {
                    log.error("Message is corrupted for batch consumption, prepare to discard it, mq={}, "
                        + "messageId={}, clientId={}", pq.getMessageQueue(), messageView.getMessageId(), clientId);
                    pq.discardMessage(messageView);
                    continue;
                }
                if (buffer.addMessage(pq, messageView)) {
                    scheduleTimeoutFlush();
                }
            }
            // Flush as many complete batches as possible.
            while (buffer.isBatchReady(batchPolicy.getMaxBatchCount(), batchPolicy.getMaxBatchBytes())) {
                flushBatch();
            }
        } finally {
            buffer.unlock();
        }
    }

    /**
     * Flushes all remaining buffered messages as partial batches.
     *
     * <p>This is called during consumer shutdown (after all inflight receive requests have
     * completed) to ensure cached messages are delivered to the listener rather than lost.
     */
    @Override
    public void close() {
        buffer.lock();
        try {
            while (!buffer.isEmpty()) {
                flushBatch();
            }
        } finally {
            buffer.unlock();
        }
    }

    // ======================== internal helpers ========================

    /**
     * Takes one batch from the buffer (up to maxBatchCount / maxBatchBytes) and submits it
     * for consumption.
     * <p>
     * Make sure the buffer is locked before calling this method.
     */
    private void flushBatch() {
        final List<BatchBuffer.BufferedMessage> batch = buffer.takeBatch(
            batchPolicy.getMaxBatchCount(), batchPolicy.getMaxBatchBytes());
        if (batch.isEmpty()) {
            return;
        }
        submitBatch(batch);
    }

    /**
     * Submits a batch of messages for consumption on the thread pool. After the
     * {@link BatchConsumeTask} completes, every message in the batch is erased from its
     * owning {@link ProcessQueue} with the returned {@link ConsumeResult}.
     */
    private void submitBatch(List<BatchBuffer.BufferedMessage> batch) {
        final List<MessageViewImpl> messageViews = new ArrayList<>(batch.size());
        for (BatchBuffer.BufferedMessage entry : batch) {
            messageViews.add(entry.getMessageView());
        }
        final ListeningExecutorService executor = MoreExecutors.listeningDecorator(batchConsumptionExecutor);
        final BatchConsumeTask task = new BatchConsumeTask(clientId, batchMessageListener, messageViews,
            batchMessageInterceptor);
        final ListenableFuture<ConsumeResult> future = executor.submit(task);
        Futures.addCallback(future, new FutureCallback<ConsumeResult>() {
            @Override
            public void onSuccess(ConsumeResult consumeResult) {
                for (BatchBuffer.BufferedMessage entry : batch) {
                    entry.getProcessQueue().eraseMessage(entry.getMessageView(), consumeResult);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                // Should never reach here.
                log.error("[Bug] Exception raised in batch consumption callback, clientId={}", clientId, t);
            }
        }, MoreExecutors.directExecutor());
    }

    /**
     * Schedules a delayed flush for the buffer after {@link BatchPolicy#getMaxWaitTime()}.
     * When the timeout fires, all remaining messages in the buffer are flushed as partial batches.
     */
    private void scheduleTimeoutFlush() {
        final long delayMillis = batchPolicy.getMaxWaitTime().toMillis();
        buffer.setTimeoutFuture(batchScheduler.schedule(() -> {
            buffer.lock();
            try {
                while (!buffer.isEmpty()) {
                    flushBatch();
                }
            } finally {
                buffer.unlock();
            }
        }, delayMillis, TimeUnit.MILLISECONDS));
    }
}
