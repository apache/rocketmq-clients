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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.apis.consumer.BatchMessageListener;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.retry.ExponentialBackoffRetryPolicy;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BatchConsumeServiceTest extends TestBase {

    private ScheduledExecutorService scheduler;
    private ThreadPoolExecutor consumptionExecutor;
    private PushConsumerImpl consumer;
    private ProcessQueue processQueue;

    @Before
    public void setUp() {
        scheduler = new ScheduledThreadPoolExecutor(4,
            new ThreadFactoryImpl("TestBatchScheduler"));
        consumptionExecutor = new ThreadPoolExecutor(4, 4, 60, TimeUnit.SECONDS,
            new java.util.concurrent.LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("TestBatchConsumption"));

        consumer = mock(PushConsumerImpl.class);
        when(consumer.getClientId()).thenReturn(FAKE_CLIENT_ID);
        doNothing().when(consumer).doBefore(any(), any());
        doNothing().when(consumer).doAfter(any(), any());
        setField(consumer, "consumptionOkQuantity", new AtomicLong(0));
        setField(consumer, "consumptionErrorQuantity", new AtomicLong(0));

        RetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3);
        when(consumer.getRetryPolicy()).thenReturn(retryPolicy);

        processQueue = mock(ProcessQueue.class);
    }

    @After
    public void tearDown() {
        consumptionExecutor.shutdownNow();
        scheduler.shutdownNow();
    }

    @Test
    public void testFlushOnMaxBatchSize() {
        final int maxBatchSize = 3;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE,
            Duration.ofSeconds(60));
        AtomicReference<List<MessageView>> receivedBatch = new AtomicReference<>();
        BatchMessageListener listener = messages -> {
            receivedBatch.set(messages);
            return ConsumeResult.SUCCESS;
        };

        BatchConsumeService service = new BatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        for (int i = 0; i < maxBatchSize; i++) {
            messages.add(fakeMessageViewImpl());
        }
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals(maxBatchSize, receivedBatch.get().size());
            verify(processQueue, times(maxBatchSize)).eraseMessage(any(), eq(ConsumeResult.SUCCESS));
        });
    }

    @Test
    public void testFlushOnMaxBatchBytes() {
        final int maxBatchBytes = 12;
        BatchPolicy policy = new BatchPolicy(Integer.MAX_VALUE, maxBatchBytes,
            Duration.ofSeconds(60));
        AtomicInteger batchSize = new AtomicInteger(0);
        BatchMessageListener listener = messages -> {
            batchSize.set(messages.size());
            return ConsumeResult.SUCCESS;
        };

        BatchConsumeService service = new BatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl(6, false));
        messages.add(fakeMessageViewImpl(6, false));
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals(2, batchSize.get());
            verify(processQueue, times(2)).eraseMessage(any(), eq(ConsumeResult.SUCCESS));
        });
    }

    @Test
    public void testFlushOnMaxWaitTime() {
        BatchPolicy policy = new BatchPolicy(Integer.MAX_VALUE, Integer.MAX_VALUE,
            Duration.ofMillis(100));
        AtomicInteger batchSize = new AtomicInteger(0);
        BatchMessageListener listener = messages -> {
            batchSize.set(messages.size());
            return ConsumeResult.SUCCESS;
        };

        BatchConsumeService service = new BatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = Collections.singletonList(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertEquals(1, batchSize.get())
        );
    }

    @Test
    public void testStandardModeFailure() {
        final int maxBatchSize = 2;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE,
            Duration.ofSeconds(60));
        BatchMessageListener listener = messages -> ConsumeResult.FAILURE;

        BatchConsumeService service = new BatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            verify(processQueue, times(2)).eraseMessage(any(), eq(ConsumeResult.FAILURE))
        );
    }

    @Test
    public void testFifoModeSuccess() {
        final int maxBatchSize = 2;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE,
            Duration.ofSeconds(60));
        BatchMessageListener listener = messages -> ConsumeResult.SUCCESS;

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        when(processQueue.eraseFifoMessage(any(), eq(ConsumeResult.SUCCESS)))
            .thenReturn(com.google.common.util.concurrent.Futures.immediateVoidFuture());

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            verify(processQueue, times(2)).eraseFifoMessage(any(), eq(ConsumeResult.SUCCESS));
            assertEquals(2, getAtomicLongField(consumer, "consumptionOkQuantity").get());
        });
    }

    @Test
    public void testFifoBatchRetryThenDLQ() {
        final int maxBatchSize = 2;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE,
            Duration.ofSeconds(60));
        AtomicInteger callCount = new AtomicInteger(0);
        BatchMessageListener listener = messages -> {
            callCount.incrementAndGet();
            return ConsumeResult.FAILURE;
        };

        RetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3);
        when(consumer.getRetryPolicy()).thenReturn(retryPolicy);
        doNothing().when(processQueue).discardFifoMessage(any());

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals("Should retry up to maxAttempts", 3, callCount.get());
            verify(processQueue, times(2)).discardFifoMessage(any());
            assertEquals(2, getAtomicLongField(consumer, "consumptionErrorQuantity").get());
        });
    }

    @Test
    public void testFifoOnlyOneBatchInFlight() throws InterruptedException {
        final int maxBatchSize = 1;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE,
            Duration.ofSeconds(60));
        CountDownLatch firstBatchStarted = new CountDownLatch(1);
        CountDownLatch firstBatchRelease = new CountDownLatch(1);
        AtomicInteger totalCalls = new AtomicInteger(0);

        BatchMessageListener listener = messages -> {
            int call = totalCalls.incrementAndGet();
            if (call == 1) {
                firstBatchStarted.countDown();
                try {
                    firstBatchRelease.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return ConsumeResult.SUCCESS;
        };

        when(processQueue.eraseFifoMessage(any(), eq(ConsumeResult.SUCCESS)))
            .thenReturn(com.google.common.util.concurrent.Futures.immediateVoidFuture());

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        service.consume(processQueue, Collections.singletonList(fakeMessageViewImpl()));

        assertTrue(firstBatchStarted.await(5, TimeUnit.SECONDS));

        service.consume(processQueue, Collections.singletonList(fakeMessageViewImpl()));

        await().pollDelay(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1)).untilAsserted(() ->
            assertEquals("Second batch should not start while first is in-flight", 1, totalCalls.get())
        );

        firstBatchRelease.countDown();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertEquals("Second batch should start after first completes", 2, totalCalls.get())
        );
    }

    @Test
    public void testGracefulShutdownFlushesBuffer() {
        BatchPolicy policy = new BatchPolicy(100, Integer.MAX_VALUE,
            Duration.ofSeconds(60));
        AtomicInteger batchSize = new AtomicInteger(0);
        BatchMessageListener listener = messages -> {
            batchSize.set(messages.size());
            return ConsumeResult.SUCCESS;
        };

        BatchConsumeService service = new BatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        service.close();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertEquals(3, batchSize.get())
        );
    }

    @Test
    public void testConcurrentProcessQueueSubmission() throws InterruptedException {
        final int maxBatchSize = 10;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE,
            Duration.ofMillis(200));
        final int numThreads = 8;
        final int messagesPerThread = 5;
        final int totalMessages = numThreads * messagesPerThread;

        AtomicInteger totalConsumed = new AtomicInteger(0);
        BatchMessageListener listener = messages -> {
            totalConsumed.addAndGet(messages.size());
            return ConsumeResult.SUCCESS;
        };

        BatchConsumeService service = new BatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        CountDownLatch threadsDone = new CountDownLatch(numThreads);

        for (int t = 0; t < numThreads; t++) {
            final ProcessQueue pq = mock(ProcessQueue.class);
            new Thread(() -> {
                try {
                    barrier.await();
                    List<MessageViewImpl> msgs = new ArrayList<>();
                    for (int i = 0; i < messagesPerThread; i++) {
                        msgs.add(fakeMessageViewImpl());
                    }
                    service.consume(pq, msgs);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    threadsDone.countDown();
                }
            }).start();
        }

        assertTrue("All threads should finish", threadsDone.await(5, TimeUnit.SECONDS));

        service.close();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertEquals("No messages should be lost or duplicated", totalMessages, totalConsumed.get())
        );
    }

    @Test
    public void testCorruptedMessagesAreDiscarded() {
        final int maxBatchSize = 2;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE,
            Duration.ofMillis(100));
        AtomicInteger batchSize = new AtomicInteger(0);
        BatchMessageListener listener = messages -> {
            batchSize.set(messages.size());
            return ConsumeResult.SUCCESS;
        };

        BatchConsumeService service = new BatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl(1, true));
        messages.add(fakeMessageViewImpl(1, false));
        messages.add(fakeMessageViewImpl(1, false));
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals("Corrupted messages should not be in batch", 2, batchSize.get());
            verify(processQueue, times(1)).discardMessage(any());
        });
    }

    @Test
    public void testExtractBatchRespectsMaxBatchSizeAndBytes() {
        BatchPolicy policy = new BatchPolicy(2, 10, Duration.ofSeconds(60));
        AtomicInteger batchCount = new AtomicInteger(0);
        BatchMessageListener listener = messages -> {
            batchCount.incrementAndGet();
            return ConsumeResult.SUCCESS;
        };

        BatchConsumeService service = new BatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl(6, false));
        messages.add(fakeMessageViewImpl(6, false));
        messages.add(fakeMessageViewImpl(6, false));
        messages.add(fakeMessageViewImpl(6, false));
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertTrue("Should have produced multiple batches", batchCount.get() >= 2)
        );
    }

    @Test
    public void testForwardProgressWithOversizedMessage() {
        BatchPolicy policy = new BatchPolicy(10, 5, Duration.ofSeconds(60));
        AtomicInteger batchSize = new AtomicInteger(0);
        BatchMessageListener listener = messages -> {
            batchSize.set(messages.size());
            return ConsumeResult.SUCCESS;
        };

        BatchConsumeService service = new BatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl(20, false));
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertEquals("Oversized message should still be flushed as a batch of one", 1, batchSize.get())
        );
    }

    @Test
    public void testFifoCorruptedMessagesDiscardedWithFifoMethod() {
        final int maxBatchSize = 2;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE,
            Duration.ofMillis(100));
        BatchMessageListener listener = messages -> ConsumeResult.SUCCESS;

        when(processQueue.eraseFifoMessage(any(), eq(ConsumeResult.SUCCESS)))
            .thenReturn(com.google.common.util.concurrent.Futures.immediateVoidFuture());
        doNothing().when(processQueue).discardFifoMessage(any());

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl(1, true));
        messages.add(fakeMessageViewImpl(1, false));
        messages.add(fakeMessageViewImpl(1, false));
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            verify(processQueue, times(1)).discardFifoMessage(any());
            verify(processQueue, times(2)).eraseFifoMessage(any(), eq(ConsumeResult.SUCCESS));
        });
    }

    private static void setField(Object target, String fieldName, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (NoSuchFieldException e) {
            try {
                Field field = target.getClass().getSuperclass().getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(target, value);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static AtomicLong getAtomicLongField(Object target, String fieldName) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (AtomicLong) field.get(target);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
