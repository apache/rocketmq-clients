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

package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.impl.ServiceState;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConsumeService {
    private static final Logger log = LoggerFactory.getLogger(ConsumeService.class);

    private static final long CONSUMPTION_DISPATCH_PERIOD_MILLIS = 10;

    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;

    private final MessageListener messageListener;
    private final MessageInterceptor interceptor;
    private final ThreadPoolExecutor consumptionExecutor;
    private final ScheduledExecutorService scheduler;

    private final AtomicReference<ServiceState> state;

    private final Object dispatcherConditionVariable;
    private final ThreadPoolExecutor dispatcherExecutor;


    public ConsumeService(MessageListener messageListener, MessageInterceptor interceptor,
                          ThreadPoolExecutor consumptionExecutor, ScheduledExecutorService scheduler,
                          ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable) {
        this.messageListener = messageListener;
        this.interceptor = interceptor;
        this.consumptionExecutor = consumptionExecutor;
        this.scheduler = scheduler;
        this.processQueueTable = processQueueTable;

        this.state = new AtomicReference<ServiceState>(ServiceState.READY);
        this.dispatcherConditionVariable = new Object();
        this.dispatcherExecutor = new ThreadPoolExecutor(
                1,
                1,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("ConsumptionDispatcher"));
    }

    public void start() {
        synchronized (this) {
            log.info("Begin to start the consume service.");
            if (!state.compareAndSet(ServiceState.READY, ServiceState.STARTING)) {
                log.warn("The consume service has been started before.");
                return;
            }
            try {
                dispatcherExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        while (ServiceState.STARTED == state.get()) {
                            try {
                                dispatch0();
                                synchronized (dispatcherConditionVariable) {
                                    dispatcherConditionVariable.wait(CONSUMPTION_DISPATCH_PERIOD_MILLIS);
                                }
                            } catch (Throwable t) {
                                log.error("Exception raised while schedule message consumption dispatcher", t);
                            }
                        }
                    }
                });
            } catch (Throwable t) {
                log.error("[Bug] Failed to submit task to dispatch message.", t);
                return;
            }
            state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
            log.info("Start the consume service successfully.");
        }
    }

    public void shutdown() throws InterruptedException {
        synchronized (this) {
            log.info("Begin to shutdown the consume service.");
            if (!state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING)) {
                log.warn("The consume service has not been started before.");
                return;
            }
            dispatcherExecutor.shutdown();
            if (!dispatcherExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                log.error("[Bug] Failed to shutdown the dispatcher executor.");
            }
            state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED);
            log.info("Shutdown the consumer service successfully.");
        }
    }

    public abstract void dispatch0();

    public ListenableFuture<ConsumeStatus> consume(MessageExt messageExt) {
        final List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(messageExt);
        return consume(messageExtList);
    }

    public ListenableFuture<ConsumeStatus> consume(MessageExt messageExt, long delay, TimeUnit timeUnit) {
        final List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(messageExt);
        return consume(messageExtList, delay, timeUnit);
    }

    public ListenableFuture<ConsumeStatus> consume(List<MessageExt> messageExtList) {
        return consume(messageExtList, 0, TimeUnit.MILLISECONDS);
    }

    public ListenableFuture<ConsumeStatus> consume(List<MessageExt> messageExtList, long delay, TimeUnit timeUnit) {
        final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(consumptionExecutor);
        final ConsumeTask task = new ConsumeTask(interceptor, messageListener, messageExtList);
        if (delay <= 0) {
            return executorService.submit(task);
        }
        final SettableFuture<ConsumeStatus> future0 = SettableFuture.create();
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                final ListenableFuture<ConsumeStatus> future = executorService.submit(task);
                Futures.addCallback(future, new FutureCallback<ConsumeStatus>() {
                    @Override
                    public void onSuccess(ConsumeStatus consumeStatus) {
                        future0.set(consumeStatus);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.error("[Bug] Exception raised while submitting scheduled consumption task", t);
                    }
                });
            }
        }, delay, timeUnit);
        return future0;
    }

    public void dispatch() {
        synchronized (dispatcherConditionVariable) {
            dispatcherConditionVariable.notifyAll();
        }
    }
}
