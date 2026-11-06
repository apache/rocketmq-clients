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
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;

/**
 * Limits unfinished asynchronous consumption without occupying a worker while a stage is pending.
 */
class AsyncConsumeDispatcher {
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private final Queue<PendingConsumption> queue = new ArrayDeque<>();
    private final Queue<PendingConsumption> dispatchQueue = new ArrayDeque<>();
    private boolean dispatching;
    private int concurrency;
    private int active;
    private long outstanding;

    AsyncConsumeDispatcher(ExecutorService executor, ScheduledExecutorService scheduler, int concurrency) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("consumption concurrency should be positive");
        }
        this.executor = executor;
        this.scheduler = scheduler;
        this.concurrency = concurrency;
    }

    ListenableFuture<ConsumeResult> consume(AsyncConsumeTask task, Duration delay) {
        PendingConsumption pending = new PendingConsumption(task);
        synchronized (this) {
            outstanding++;
        }
        try {
            if (delay.isZero() || delay.isNegative()) {
                enqueue(pending);
            } else {
                scheduler.schedule(() -> enqueue(pending), delay.toNanos(), TimeUnit.NANOSECONDS);
            }
        } catch (RuntimeException error) {
            complete(pending, ConsumeResult.FAILURE, false);
        }
        return pending.future;
    }

    private void enqueue(PendingConsumption pending) {
        List<PendingConsumption> ready;
        synchronized (this) {
            queue.add(pending);
            ready = reserveReady();
        }
        dispatch(ready);
    }

    private List<PendingConsumption> reserveReady() {
        List<PendingConsumption> ready = new ArrayList<>();
        while (active < concurrency && !queue.isEmpty()) {
            active++;
            ready.add(queue.remove());
        }
        return ready;
    }

    private void dispatch(List<PendingConsumption> ready) {
        synchronized (this) {
            dispatchQueue.addAll(ready);
            if (dispatching) {
                return;
            }
            dispatching = true;
        }
        while (true) {
            PendingConsumption pending;
            synchronized (this) {
                pending = dispatchQueue.poll();
                if (null == pending) {
                    dispatching = false;
                    return;
                }
            }
            try {
                executor.execute(() -> {
                    try {
                        Futures.addCallback(pending.task.invoke(), new FutureCallback<ConsumeResult>() {
                            @Override
                            public void onSuccess(ConsumeResult result) {
                                complete(pending, result, true);
                            }

                            @Override
                            public void onFailure(Throwable error) {
                                complete(pending, ConsumeResult.FAILURE, true);
                            }
                        }, MoreExecutors.directExecutor());
                    } catch (Throwable error) {
                        complete(pending, ConsumeResult.FAILURE, true);
                    }
                });
            } catch (RuntimeException error) {
                complete(pending, ConsumeResult.FAILURE, true);
            }
        }
    }

    private void complete(PendingConsumption pending, ConsumeResult result, boolean reserved) {
        if (!pending.completed.compareAndSet(false, true)) {
            return;
        }
        // Result callbacks register ACK/NACK or the next FIFO operation before this task leaves the drain ledger.
        pending.future.set(null == result ? ConsumeResult.FAILURE : result);
        List<PendingConsumption> ready;
        synchronized (this) {
            if (reserved) {
                active--;
            }
            outstanding--;
            ready = reserveReady();
            notifyAll();
        }
        dispatch(ready);
    }

    void trackCompletion(ListenableFuture<?> future) {
        synchronized (this) {
            outstanding++;
        }
        future.addListener(() -> {
            synchronized (AsyncConsumeDispatcher.this) {
                outstanding--;
                AsyncConsumeDispatcher.this.notifyAll();
            }
        }, MoreExecutors.directExecutor());
    }

    synchronized void awaitConsumption() throws InterruptedException {
        while (outstanding > 0) {
            wait();
        }
    }

    void updateConcurrency(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("consumption concurrency should be positive");
        }
        List<PendingConsumption> ready;
        synchronized (this) {
            concurrency = count;
            ready = reserveReady();
        }
        dispatch(ready);
    }

    private static class PendingConsumption {
        private final AsyncConsumeTask task;
        private final SettableFuture<ConsumeResult> future = SettableFuture.create();
        private final AtomicBoolean completed = new AtomicBoolean();

        private PendingConsumption(AsyncConsumeTask task) {
            this.task = task;
        }
    }
}
