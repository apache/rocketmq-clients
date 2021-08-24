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
import org.apache.rocketmq.client.consumer.ConsumeContext;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.tools.TestBase;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumeConcurrentlyServiceTest extends TestBase {

    @Mock
    private MessageListener messageListener;
    @Mock
    private MessageInterceptor messageInterceptor;

    private final ThreadPoolExecutor consumptionExecutor = SINGLE_THREAD_POOL_EXECUTOR;
    private final ScheduledExecutorService scheduler = SCHEDULER;
    private ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;
    private ConsumeService consumeService;
    private int batchSize;

    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);

        processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>();
        batchSize = 1;
        consumeService = new ConsumeConcurrentlyService(messageListener, messageInterceptor, consumptionExecutor,
                                                        scheduler, processQueueTable, batchSize);
        consumeService.start();
    }

    @AfterMethod
    public void afterMethod() throws InterruptedException {
        consumeService.shutdown();
    }

    @Test
    public void testConsume() throws ExecutionException, InterruptedException {
        final MessageExt messageExt = fakeMessageExt(1);
        when(messageListener.consume(ArgumentMatchers.<MessageExt>anyList(),
                                     ArgumentMatchers.any(ConsumeContext.class))).thenReturn(ConsumeStatus.OK);
        final ListenableFuture<ConsumeStatus> future = consumeService.consume(messageExt);
        final ConsumeStatus consumeStatus = future.get();
        assertEquals(consumeStatus, ConsumeStatus.OK);
    }

    @Test
    public void testConsumeWithDelay() throws InterruptedException, ExecutionException {
        final MessageExt messageExt = fakeMessageExt(1);
        when(messageListener.consume(ArgumentMatchers.<MessageExt>anyList(),
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