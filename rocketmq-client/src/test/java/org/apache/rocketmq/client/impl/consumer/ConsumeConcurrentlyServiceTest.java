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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.conf.TestBase;
import org.apache.rocketmq.client.consumer.ConsumeContext;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageQueue;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumeConcurrentlyServiceTest extends TestBase {
    private MessageListener mockedListener;
    private MessageInterceptor mockedInterceptor;
    private final ThreadPoolExecutor consumptionExecutor = getSingleThreadPoolExecutor();
    private final ScheduledExecutorService scheduler = getScheduler();
    private ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;
    private ConsumeService consumeService;
    private int batchSize;

    @BeforeMethod
    public void beforeMethod() {
        mockedListener = mock(MessageListener.class);
        mockedInterceptor = mock(MessageInterceptor.class);
        processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>();
        batchSize = 1;
        consumeService = new ConsumeConcurrentlyService(mockedListener, mockedInterceptor, consumptionExecutor,
                                                        scheduler, processQueueTable, batchSize);
        consumeService.start();
    }

    @AfterMethod
    public void afterMethod() {
        consumeService.shutdown();
    }

    @Test
    public void testConsume() throws ExecutionException, InterruptedException {
        final MessageExt messageExt = getDummyMessageExt(1);
        when(mockedListener.consume(ArgumentMatchers.<MessageExt>anyList(),
                                    ArgumentMatchers.any(ConsumeContext.class))).thenReturn(ConsumeStatus.OK);
        final ListenableFuture<ConsumeStatus> future = consumeService.consume(messageExt);
        final ConsumeStatus consumeStatus = future.get();
        assertEquals(consumeStatus, ConsumeStatus.OK);
    }

    @Test
    public void testConsumeWithDelay() throws InterruptedException, ExecutionException {
        final MessageExt messageExt = getDummyMessageExt(1);
        when(mockedListener.consume(ArgumentMatchers.<MessageExt>anyList(),
                                    ArgumentMatchers.any(ConsumeContext.class))).thenReturn(ConsumeStatus.OK);
        final long delayMillis = 1000;
        final ListenableFuture<ConsumeStatus> future = consumeService.consume(messageExt, delayMillis,
                                                                              TimeUnit.MILLISECONDS);
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final long biasMillis = 50;
        final ConsumeStatus consumeStatus = future.get();
        assertEquals(consumeStatus, ConsumeStatus.OK);
        final long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        assertTrue(Math.abs(elapsed - delayMillis) < biasMillis);
    }
}