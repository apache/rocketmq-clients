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

package org.apache.rocketmq.client.misc;

import com.google.common.util.concurrent.AbstractIdleService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.utility.ExecutorServices;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregate multi-dispatch task into one as possible.
 */
@SuppressWarnings("UnstableApiUsage")
public abstract class Dispatcher extends AbstractIdleService {
    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    private final AtomicBoolean dispatchTaskInQueue;

    private final long signalPeriodMillis;

    private final ScheduledExecutorService scheduler;
    private final ThreadPoolExecutor dispatcherExecutor;

    private volatile ScheduledFuture<?> dispatchFuture;

    public Dispatcher(long signalPeriodMillis, ScheduledExecutorService scheduler) {
        this.dispatchTaskInQueue = new AtomicBoolean(false);

        this.signalPeriodMillis = signalPeriodMillis;

        this.scheduler = scheduler;
        this.dispatcherExecutor = new ThreadPoolExecutor(
                1,
                1,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("Dispatcher"));
    }

    public abstract void dispatch();

    @Override
    protected void startUp() {
        dispatchFuture = scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    signal();
                } catch (Throwable t) {
                    log.error("Exception raised while signal dispatcher.", t);
                }
            }
        }, 0, signalPeriodMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutDown() throws InterruptedException {
        if (null != dispatchFuture) {
            dispatchFuture.cancel(false);
        }
        dispatcherExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(dispatcherExecutor)) {
            log.error("[Bug] Failed to shutdown the batch dispatcher.");
        }
    }

    public void signal() {
        if (dispatchTaskInQueue.compareAndSet(false, true)) {
            try {
                dispatcherExecutor.submit(new DispatchTask());
            } catch (Throwable t) {
                if (!dispatcherExecutor.isShutdown()) {
                    log.error("[Bug] Failed to submit dispatch task.", t);
                }
            }
        }
    }

    class DispatchTask implements Runnable {
        @Override
        public void run() {
            dispatchTaskInQueue.compareAndSet(true, false);
            try {
                dispatch();
            } catch (Throwable t) {
                log.error("Exception raised while dispatching task", t);
            }
        }
    }
}