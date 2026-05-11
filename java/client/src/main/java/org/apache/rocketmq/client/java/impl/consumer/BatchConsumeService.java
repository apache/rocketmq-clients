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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.client.apis.consumer.BatchMessageListener;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("NullableProblems")
public class BatchConsumeService extends ConsumeService {
    private static final Logger log = LoggerFactory.getLogger(BatchConsumeService.class);

    protected final BatchMessageListener batchMessageListener;
    protected final BatchPolicy batchPolicy;

    protected final ReentrantLock bufferLock = new ReentrantLock();
    protected final List<BufferedMessage> buffer = new ArrayList<>();
    protected long bufferBytes = 0;
    protected long firstMessageNanoTime = 0;
    protected ScheduledFuture<?> scheduledFlush;

    public BatchConsumeService(ClientId clientId, BatchMessageListener batchMessageListener,
        BatchPolicy batchPolicy, ThreadPoolExecutor consumptionExecutor,
        MessageInterceptor messageInterceptor, ScheduledExecutorService scheduler) {
        super(clientId, consumptionExecutor, messageInterceptor, scheduler);
        this.batchMessageListener = batchMessageListener;
        this.batchPolicy = batchPolicy;
    }

    @Override
    public void consume(ProcessQueue pq, List<MessageViewImpl> messageViews) {
        bufferLock.lock();
        try {
            for (MessageViewImpl messageView : messageViews) {
                if (messageView.isCorrupted()) {
                    log.error("Message is corrupted for batch consumption, prepare to discard it, mq={}, "
                        + "messageId={}, clientId={}", pq.getMessageQueue(), messageView.getMessageId(), clientId);
                    discardMessage(pq, messageView);
                    continue;
                }
                buffer.add(new BufferedMessage(pq, messageView));
                bufferBytes += messageView.getBody().remaining();
                if (buffer.size() == 1) {
                    firstMessageNanoTime = System.nanoTime();
                    scheduleTimedFlush();
                }
                if (shouldFlush()) {
                    tryFlush();
                }
            }
        } finally {
            bufferLock.unlock();
        }
    }

    protected void discardMessage(ProcessQueue pq, MessageViewImpl messageView) {
        pq.discardMessage(messageView);
    }

    private boolean shouldFlush() {
        return buffer.size() >= batchPolicy.getMaxBatchCount()
            || bufferBytes >= batchPolicy.getMaxBatchBytes();
    }

    private void scheduleTimedFlush() {
        if (scheduledFlush != null) {
            return;
        }
        long delayNanos = batchPolicy.getMaxWaitTime().toNanos();
        scheduledFlush = getScheduler().schedule(() -> {
            bufferLock.lock();
            try {
                scheduledFlush = null;
                tryFlush();
            } finally {
                bufferLock.unlock();
            }
        }, delayNanos, TimeUnit.NANOSECONDS);
    }

    protected void tryFlush() {
        if (buffer.isEmpty()) {
            return;
        }
        doFlush();
    }

    protected void doFlush() {
        List<BufferedMessage> batch = extractBatch();
        if (batch.isEmpty()) {
            return;
        }
        submitBatch(batch);
    }

    protected List<BufferedMessage> extractBatch() {
        int count = Math.min(buffer.size(), batchPolicy.getMaxBatchCount());
        long bytes = 0;
        int actualCount = 0;
        for (int i = 0; i < count; i++) {
            long msgBytes = buffer.get(i).messageView.getBody().remaining();
            if (actualCount > 0 && bytes + msgBytes > batchPolicy.getMaxBatchBytes()) {
                break;
            }
            bytes += msgBytes;
            actualCount++;
        }
        List<BufferedMessage> batch = new ArrayList<>(buffer.subList(0, actualCount));
        buffer.subList(0, actualCount).clear();
        bufferBytes -= bytes;

        if (scheduledFlush != null) {
            scheduledFlush.cancel(false);
            scheduledFlush = null;
        }
        if (!buffer.isEmpty()) {
            firstMessageNanoTime = System.nanoTime();
            scheduleTimedFlush();
        }
        return batch;
    }

    protected void submitBatch(List<BufferedMessage> batch) {
        List<MessageViewImpl> messageViews = new ArrayList<>(batch.size());
        for (BufferedMessage bm : batch) {
            messageViews.add(bm.messageView);
        }
        final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(getConsumptionExecutor());
        final BatchConsumeTask task = new BatchConsumeTask(clientId, batchMessageListener,
            messageViews, getMessageInterceptor());
        final ListenableFuture<ConsumeResult> future = executorService.submit(task);
        Futures.addCallback(future, new FutureCallback<ConsumeResult>() {
            @Override
            public void onSuccess(ConsumeResult consumeResult) {
                onBatchComplete(batch, consumeResult);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("[Bug] Exception raised while submitting batch consumption task, clientId={}",
                    clientId, t);
            }
        }, MoreExecutors.directExecutor());
    }

    protected void onBatchComplete(List<BufferedMessage> batch, ConsumeResult consumeResult) {
        for (BufferedMessage bm : batch) {
            bm.processQueue.eraseMessage(bm.messageView, consumeResult);
        }
    }

    @Override
    public void close() {
        bufferLock.lock();
        try {
            if (scheduledFlush != null) {
                scheduledFlush.cancel(false);
                scheduledFlush = null;
            }
            while (!buffer.isEmpty()) {
                doFlush();
            }
        } finally {
            bufferLock.unlock();
        }
    }

    static class BufferedMessage {
        final ProcessQueue processQueue;
        final MessageViewImpl messageView;

        BufferedMessage(ProcessQueue processQueue, MessageViewImpl messageView) {
            this.processQueue = processQueue;
            this.messageView = messageView;
        }
    }
}
