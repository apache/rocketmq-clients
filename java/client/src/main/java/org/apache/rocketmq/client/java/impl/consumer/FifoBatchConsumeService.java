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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.rocketmq.client.apis.consumer.BatchMessageListener;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("NullableProblems")
class FifoBatchConsumeService extends BatchConsumeService {
    private static final Logger log = LoggerFactory.getLogger(FifoBatchConsumeService.class);

    private final Supplier<RetryPolicy> retryPolicySupplier;
    private final AtomicBoolean flushing = new AtomicBoolean(false);
    private volatile boolean pendingFlush = false;

    public FifoBatchConsumeService(ClientId clientId, BatchMessageListener batchMessageListener,
        BatchPolicy batchPolicy, ThreadPoolExecutor consumptionExecutor,
        MessageInterceptor messageInterceptor, ScheduledExecutorService scheduler,
        Supplier<RetryPolicy> retryPolicySupplier) {
        super(clientId, batchMessageListener, batchPolicy, consumptionExecutor,
            messageInterceptor, scheduler);
        this.retryPolicySupplier = retryPolicySupplier;
    }

    @Override
    protected void discardMessage(ProcessQueue pq, MessageViewImpl messageView) {
        pq.discardFifoMessage(messageView);
    }

    @Override
    protected void tryFlush() {
        if (buffer.isEmpty()) {
            return;
        }
        if (!flushing.compareAndSet(false, true)) {
            pendingFlush = true;
            return;
        }
        doFlush();
    }

    @Override
    protected void onBatchComplete(List<BufferedMessage> batch, ConsumeResult consumeResult) {
        if (ConsumeResult.SUCCESS.equals(consumeResult)) {
            eraseFifoBatchSuccess(batch);
            return;
        }
        RetryPolicy retryPolicy = retryPolicySupplier.get();
        int maxAttempts = retryPolicy.getMaxAttempts();

        List<BufferedMessage> retryable = new ArrayList<>();
        List<BufferedMessage> exhausted = new ArrayList<>();
        for (BufferedMessage bm : batch) {
            if (bm.messageView.getDeliveryAttempt() < maxAttempts) {
                retryable.add(bm);
            } else {
                exhausted.add(bm);
            }
        }

        if (!exhausted.isEmpty()) {
            log.info("Failed to consume FIFO batch messages finally, run out of attempt times, "
                + "maxAttempts={}, exhaustedCount={}, clientId={}", maxAttempts, exhausted.size(), clientId);
            if (retryable.isEmpty()) {
                eraseFifoBatchFailure(exhausted);
                return;
            }
            eraseFifoBatchFailureWithoutRelease(exhausted);
        }

        for (BufferedMessage bm : retryable) {
            bm.messageView.incrementAndGetDeliveryAttempt();
        }
        int nextAttempt = retryable.get(0).messageView.getDeliveryAttempt();
        Duration nextAttemptDelay = retryPolicy.getNextAttemptDelay(nextAttempt);
        log.debug("Prepare to redeliver the FIFO batch because of consumption failure, maxAttempts={}, "
            + "attempt={}, retryBatchSize={}, nextAttemptDelay={}, clientId={}", maxAttempts, nextAttempt,
            retryable.size(), nextAttemptDelay, clientId);
        getScheduler().schedule(() -> submitBatch(retryable),
            nextAttemptDelay.toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
    }

    private void eraseFifoBatchSuccess(List<BufferedMessage> batch) {
        eraseFifoBatchIteratively(batch.iterator(), ConsumeResult.SUCCESS);
    }

    private void eraseFifoBatchFailure(List<BufferedMessage> batch) {
        eraseFifoBatchIteratively(batch.iterator(), ConsumeResult.FAILURE);
    }

    private void eraseFifoBatchFailureWithoutRelease(List<BufferedMessage> batch) {
        eraseFifoBatchIterativelyNoRelease(batch.iterator());
    }

    private void eraseFifoBatchIterativelyNoRelease(Iterator<BufferedMessage> iterator) {
        if (!iterator.hasNext()) {
            return;
        }
        BufferedMessage bm = iterator.next();
        ListenableFuture<Void> future = bm.processQueue.eraseFifoMessage(bm.messageView, ConsumeResult.FAILURE);
        Futures.addCallback(future, new com.google.common.util.concurrent.FutureCallback<Void>() {
            @Override
            public void onSuccess(Void v) {
                eraseFifoBatchIterativelyNoRelease(iterator);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("[Bug] Exception raised in FIFO batch erase callback, clientId={}", clientId, t);
                eraseFifoBatchIterativelyNoRelease(iterator);
            }
        }, MoreExecutors.directExecutor());
    }

    private void eraseFifoBatchIteratively(Iterator<BufferedMessage> iterator, ConsumeResult result) {
        if (!iterator.hasNext()) {
            releaseFlushing();
            return;
        }
        BufferedMessage bm = iterator.next();
        ListenableFuture<Void> future = bm.processQueue.eraseFifoMessage(bm.messageView, result);
        Futures.addCallback(future, new com.google.common.util.concurrent.FutureCallback<Void>() {
            @Override
            public void onSuccess(Void v) {
                eraseFifoBatchIteratively(iterator, result);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("[Bug] Exception raised in FIFO batch erase callback, clientId={}", clientId, t);
                eraseFifoBatchIteratively(iterator, result);
            }
        }, MoreExecutors.directExecutor());
    }

    private void releaseFlushing() {
        flushing.set(false);
        if (pendingFlush) {
            pendingFlush = false;
            bufferLock.lock();
            try {
                tryFlush();
            } finally {
                bufferLock.unlock();
            }
        }
    }
}
