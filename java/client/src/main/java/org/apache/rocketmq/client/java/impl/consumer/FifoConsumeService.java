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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FifoConsumeService extends ConsumeService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FifoConsumeService.class);

    public FifoConsumeService(String clientId, ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable,
        MessageListener messageListener, ThreadPoolExecutor consumptionExecutor, MessageInterceptor messageInterceptor,
        ScheduledExecutorService scheduler) {
        super(clientId, processQueueTable, messageListener, consumptionExecutor, messageInterceptor, scheduler);
    }

    @Override
    public void startUp() {
        LOGGER.info("Begin to start the FIFO consume service, clientId={}", clientId);
        super.startUp();
        LOGGER.info("Begin to shutdown the FIFO consume service, clientId={}", clientId);
    }

    @Override
    public void shutDown() throws InterruptedException {
        LOGGER.info("Begin to shutdown the FIFO consume service, clientId={}", clientId);
        super.shutDown();
        LOGGER.info("Shutdown the FIFO consume service successfully, clientId={}", clientId);
    }

    @SuppressWarnings("UnstableApiUsage")
    public void consumeIteratively(ProcessQueue pq, Iterator<MessageViewImpl> iterator) {
        if (!iterator.hasNext()) {
            return;
        }
        final MessageViewImpl next = iterator.next();
        final ListenableFuture<ConsumeResult> future0 = consume(next);
        final ListenableFuture<Void> future = Futures.transformAsync(future0, result -> pq.eraseFifoMessage(next,
            result), MoreExecutors.directExecutor());
        future.addListener(() -> consumeIteratively(pq, iterator), MoreExecutors.directExecutor());
    }

    @Override
    public void dispatch() {
        final List<ProcessQueue> processQueues = new ArrayList<>(processQueueTable.values());
        // Shuffle all process queue in case messages are always consumed firstly in one message queue.
        Collections.shuffle(processQueues);
        // Iterate all process queues to submit consumption task.
        for (final ProcessQueue pq : processQueues) {
            Iterator<MessageViewImpl> iterator = pq.tryTakeFifoMessages();
            consumeIteratively(pq, iterator);
        }
    }
}
