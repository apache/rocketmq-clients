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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class ConsumeConcurrentlyService extends ConsumeService {
    private static final Logger log = LoggerFactory.getLogger(ConsumeConcurrentlyService.class);
    
    private final int batchMaxSize;

    public ConsumeConcurrentlyService(MessageListener messageListener, MessageInterceptor interceptor,
                                      ThreadPoolExecutor consumptionExecutor, ScheduledExecutorService scheduler,
                                      ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable, int batchMaxSize) {
        super(messageListener, interceptor, consumptionExecutor, scheduler, processQueueTable);
        this.batchMaxSize = batchMaxSize;
    }

    @Override
    public void dispatch0() {
        final List<ProcessQueue> processQueues = new ArrayList<ProcessQueue>(processQueueTable.values());
        // shuffle all process queue in case messages are always consumed firstly in one message queue.
        Collections.shuffle(processQueues);

        int accumulativeSize = 0;

        final Map<MessageQueue, List<MessageExt>> messageExtListTable = new HashMap<MessageQueue, List<MessageExt>>();
        // iterate all process queues to submit consumption task.
        for (ProcessQueue pq : processQueues) {
            List<MessageExt> messageExtList = pq.tryTakeMessages(batchMaxSize - accumulativeSize);
            if (messageExtList.isEmpty()) {
                continue;
            }
            final MessageQueue mq = pq.getMessageQueue();
            // add message list to message table.
            messageExtListTable.put(mq, messageExtList);
            accumulativeSize += messageExtList.size();
            if (accumulativeSize >= batchMaxSize) {
                break;
            }
        }
        // aggregate all messages into list.
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (List<MessageExt> list : messageExtListTable.values()) {
            messageExtList.addAll(list);
        }

        // no new message arrived for current round.
        if (messageExtList.isEmpty()) {
            return;
        }

        final ListenableFuture<ConsumeStatus> future = consume(messageExtList);
        Futures.addCallback(future, new FutureCallback<ConsumeStatus>() {
            @Override
            public void onSuccess(ConsumeStatus status) {
                for (Map.Entry<MessageQueue, List<MessageExt>> entry : messageExtListTable.entrySet()) {
                    final MessageQueue mq = entry.getKey();
                    final List<MessageExt> messageExtList = entry.getValue();
                    final ProcessQueue pq = processQueueTable.get(mq);
                    if (null == pq) {
                        continue;
                    }
                    pq.eraseMessages(messageExtList, status);
                }
                ConsumeConcurrentlyService.this.dispatch();
            }

            @Override
            public void onFailure(Throwable t) {
                // should never reach here.
                log.error("[Bug] Exception raised in consumption callback.", t);
                ConsumeConcurrentlyService.this.dispatch();
            }
        });
    }
}
