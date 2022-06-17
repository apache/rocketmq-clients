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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.tools.TestBase;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumeOrderlyServiceTest extends TestBase {
    @Mock
    private MessageListenerOrderly messageListenerOrderly;
    @Mock
    private PushConsumerImpl consumerImpl;

    private final AtomicLong consumptionErrorCounter = new AtomicLong(0);

    private final ThreadPoolExecutor consumptionExecutor = SINGLE_THREAD_POOL_EXECUTOR;
    private final ScheduledExecutorService scheduler = SCHEDULER;
    private ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;
    private ConsumeOrderlyService consumeOrderlyService;

    @SuppressWarnings("UnstableApiUsage")
    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);
        processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>();

        when(consumerImpl.getMessageListener()).thenReturn(messageListenerOrderly);
        when(consumerImpl.getMessageModel()).thenReturn(MessageModel.CLUSTERING);
        when(consumerImpl.getConsumptionErrorQuantity()).thenReturn(consumptionErrorCounter);

        consumeOrderlyService = new ConsumeOrderlyService(messageListenerOrderly, consumerImpl, consumptionExecutor,
                                                          scheduler, processQueueTable);
        consumeOrderlyService.startAsync().awaitRunning();
    }

    @AfterMethod
    @SuppressWarnings("UnstableApiUsage")
    public void afterMethod() {
        consumeOrderlyService.stopAsync().awaitTerminated();
    }

    @Test
    public void testDispatch0WithEmpty() {
        final FilterExpression filterExpression = new FilterExpression();
        final MessageQueue messageQueue = fakeMessageQueue();
        final ProcessQueueImpl processQueue = new ProcessQueueImpl(consumerImpl, messageQueue, filterExpression);
        processQueueTable.put(messageQueue, processQueue);
        assertFalse(consumeOrderlyService.dispatch0());
    }

    @Test
    public void testDispatch() {
        final FilterExpression filterExpression = new FilterExpression();
        final MessageQueue messageQueue = fakeMessageQueue();
        final ProcessQueueImpl processQueue = new ProcessQueueImpl(consumerImpl, messageQueue, filterExpression);
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(fakeMessageExt(1));
        processQueue.cacheMessages(messageExtList);
        processQueueTable.put(messageQueue, processQueue);
        assertTrue(consumeOrderlyService.dispatch0());
    }
}
