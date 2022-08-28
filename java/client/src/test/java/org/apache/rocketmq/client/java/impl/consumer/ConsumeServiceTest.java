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

import static org.junit.Assert.assertEquals;

import com.google.common.util.concurrent.ListenableFuture;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.mockito.Mockito;

public class ConsumeServiceTest extends TestBase {
    private final ClientId clientId = new ClientId();
    private final MessageInterceptor interceptor = Mockito.mock(MessageInterceptor.class);
    private final ThreadPoolExecutor consumptionExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(), new ThreadFactoryImpl("TestMessageConsumption"));
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl(
        "TestScheduler"));


    @Test
    public void testConsumeSuccess() throws ExecutionException, InterruptedException, TimeoutException {
        final MessageListener messageListener = messageView -> ConsumeResult.SUCCESS;
        final ConsumeService consumeService = new ConsumeService(clientId, messageListener,
            consumptionExecutor, interceptor, scheduler) {
            @Override
            public void consume(ProcessQueue pq, List<MessageViewImpl> messageViews) {
            }
        };
        final MessageViewImpl messageView = fakeMessageViewImpl();
        final ListenableFuture<ConsumeResult> future = consumeService.consume(messageView);
        final ConsumeResult consumeResult = future.get(1000, TimeUnit.MILLISECONDS);
        assertEquals(ConsumeResult.SUCCESS, consumeResult);
    }

    @Test
    public void testConsumeFailure() throws ExecutionException, InterruptedException, TimeoutException {
        final MessageListener messageListener = messageView -> ConsumeResult.FAILURE;
        final ConsumeService consumeService = new ConsumeService(clientId, messageListener,
            consumptionExecutor, interceptor, scheduler) {
            @Override
            public void consume(ProcessQueue pq, List<MessageViewImpl> messageViews) {
            }
        };
        final MessageViewImpl messageView = fakeMessageViewImpl();
        final ListenableFuture<ConsumeResult> future = consumeService.consume(messageView);
        final ConsumeResult consumeResult = future.get(1000, TimeUnit.MILLISECONDS);
        assertEquals(ConsumeResult.FAILURE, consumeResult);
    }

    @Test
    public void testConsumeWithException() throws ExecutionException, InterruptedException, TimeoutException {
        final MessageListener messageListener = messageView -> {
            throw new RuntimeException();
        };
        final ConsumeService consumeService = new ConsumeService(clientId, messageListener,
            consumptionExecutor, interceptor, scheduler) {
            @Override
            public void consume(ProcessQueue pq, List<MessageViewImpl> messageViews) {

            }
        };
        final MessageViewImpl messageView = fakeMessageViewImpl();
        final ListenableFuture<ConsumeResult> future = consumeService.consume(messageView);
        final ConsumeResult consumeResult = future.get(1000, TimeUnit.MILLISECONDS);
        assertEquals(ConsumeResult.FAILURE, consumeResult);
    }

    @Test
    public void testConsumeWithDelay() throws ExecutionException, InterruptedException {
        final MessageListener messageListener = messageView -> ConsumeResult.SUCCESS;
        final ConsumeService consumeService = new ConsumeService(clientId, messageListener,
            consumptionExecutor, interceptor, scheduler) {

            @Override
            public void consume(ProcessQueue pq, List<MessageViewImpl> messageViews) {

            }
        };
        final MessageViewImpl messageView = fakeMessageViewImpl();
        final ListenableFuture<ConsumeResult> future = consumeService.consume(messageView, Duration.ofMillis(500));
        final ConsumeResult consumeResult = future.get();
        assertEquals(ConsumeResult.SUCCESS, consumeResult);
    }
}