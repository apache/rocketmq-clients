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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnstableApiUsage")
class FifoConsumeService extends ConsumeService {
    private static final Logger log = LoggerFactory.getLogger(FifoConsumeService.class);

    public FifoConsumeService(ClientId clientId, MessageListener messageListener,
        ThreadPoolExecutor consumptionExecutor, MessageInterceptor messageInterceptor,
        ScheduledExecutorService scheduler) {
        super(clientId, messageListener, consumptionExecutor, messageInterceptor, scheduler);
    }

    @Override
    public void consume(ProcessQueue pq, List<MessageViewImpl> messageViews) {
        consumeIteratively(pq, messageViews.iterator());
    }

    public void consumeIteratively(ProcessQueue pq, Iterator<MessageViewImpl> iterator) {
        if (!iterator.hasNext()) {
            return;
        }
        final MessageViewImpl messageView = iterator.next();
        if (messageView.isCorrupted()) {
            // Discard corrupted message.
            log.error("Message is corrupted for FIFO consumption, prepare to discard it, mq={}, messageId={}, "
                + "clientId={}", pq.getMessageQueue(), messageView.getMessageId(), clientId);
            pq.discardFifoMessage(messageView);
            consumeIteratively(pq, iterator);
            return;
        }
        final ListenableFuture<ConsumeResult> future0 = consume(messageView);
        ListenableFuture<Void> future = Futures.transformAsync(future0, result -> pq.eraseFifoMessage(messageView,
            result), MoreExecutors.directExecutor());
        future.addListener(() -> consumeIteratively(pq, iterator), MoreExecutors.directExecutor());
    }
}
