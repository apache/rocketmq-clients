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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeOrderlyService extends ConsumeService {
    private static final Logger log = LoggerFactory.getLogger(ConsumeOrderlyService.class);

    public ConsumeOrderlyService(MessageListener messageListener, MessageInterceptor interceptor,
                                 ThreadPoolExecutor consumptionExecutor, ScheduledExecutorService scheduler,
                                 ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable) {
        super(messageListener, interceptor, consumptionExecutor, scheduler, processQueueTable);
    }

    @Override
    public boolean dispatch0() {
        boolean dispatched = false;
        final List<ProcessQueue> processQueues = new ArrayList<ProcessQueue>(processQueueTable.values());
        Collections.shuffle(processQueues);

        for (final ProcessQueue pq : processQueues) {
            final Optional<MessageExt> messageExt = pq.tryTakeFifoMessage();
            if (!messageExt.isPresent()) {
                continue;
            }
            dispatched = true;
            log.debug("Take fifo message already, messageId={}", messageExt);
            final ListenableFuture<ConsumeStatus> future = consume(messageExt.get());
            Futures.addCallback(future, new FutureCallback<ConsumeStatus>() {
                @Override
                public void onSuccess(ConsumeStatus status) {
                    pq.eraseFifoMessage(messageExt.get(), status);
                }

                @Override
                public void onFailure(Throwable t) {
                    // should never reach here.
                    log.error("[Bug] Exception raised in consumption callback.", t);
                }
            });
        }
        return dispatched;
    }
}
