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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.apis.consumer.BatchMessageListener;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.retry.ExponentialBackoffRetryPolicy;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FifoBatchConsumeServiceTest extends TestBase {

    private ScheduledExecutorService scheduler;
    private ThreadPoolExecutor consumptionExecutor;
    private PushConsumerImpl consumer;
    private ProcessQueue processQueue;

    @Before
    public void setUp() {
        scheduler = new ScheduledThreadPoolExecutor(4,
            new ThreadFactoryImpl("TestFifoBatchScheduler"));
        consumptionExecutor = new ThreadPoolExecutor(4, 4, 60, TimeUnit.SECONDS,
            new java.util.concurrent.LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("TestFifoBatchConsumption"));

        consumer = mock(PushConsumerImpl.class);
        when(consumer.getClientId()).thenReturn(FAKE_CLIENT_ID);
        doNothing().when(consumer).doBefore(any(), any());
        doNothing().when(consumer).doAfter(any(), any());
        setField(consumer, "consumptionOkQuantity", new AtomicLong(0));
        setField(consumer, "consumptionErrorQuantity", new AtomicLong(0));

        RetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3);
        when(consumer.getRetryPolicy()).thenReturn(retryPolicy);

        processQueue = mock(ProcessQueue.class);
        when(processQueue.eraseFifoMessage(any(), any()))
            .thenReturn(Futures.immediateVoidFuture());
        doNothing().when(processQueue).discardFifoMessage(any());
    }

    @After
    public void tearDown() {
        consumptionExecutor.shutdownNow();
        scheduler.shutdownNow();
    }

    // ===== FIFO Ordering Tests =====

    @Test
    public void testFifoOrderingSecondBatchBlockedUntilFirstCompletes() throws InterruptedException {
        final int maxBatchSize = 1;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));
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

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        service.consume(processQueue, Collections.singletonList(fakeMessageViewImpl()));

        assertTrue("First batch should start", firstBatchStarted.await(5, TimeUnit.SECONDS));

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
    public void testFifoOrderingMultipleBatchesSequential() throws InterruptedException {
        final int maxBatchSize = 1;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));
        List<Integer> executionOrder = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger callCount = new AtomicInteger(0);

        BatchMessageListener listener = messages -> {
            int call = callCount.incrementAndGet();
            executionOrder.add(call);
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return ConsumeResult.SUCCESS;
        };

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        for (int i = 0; i < 3; i++) {
            service.consume(processQueue, Collections.singletonList(fakeMessageViewImpl()));
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
            assertEquals("All 3 batches should execute", 3, callCount.get())
        );

        assertEquals("Batches should execute in order", Arrays.asList(1, 2, 3), executionOrder);
    }

    // ===== Retry Tests =====

    @Test
    public void testFifoBatchRetriesOnFailure() {
        final int maxBatchSize = 2;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));
        AtomicInteger callCount = new AtomicInteger(0);

        BatchMessageListener listener = messages -> {
            callCount.incrementAndGet();
            return ConsumeResult.FAILURE;
        };

        RetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3);
        when(consumer.getRetryPolicy()).thenReturn(retryPolicy);

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals("Should retry up to maxAttempts", 3, callCount.get());
        });
    }

    @Test
    public void testFifoBatchSuccessOnSecondAttempt() {
        final int maxBatchSize = 2;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));
        AtomicInteger callCount = new AtomicInteger(0);

        BatchMessageListener listener = messages -> {
            int call = callCount.incrementAndGet();
            return call == 1 ? ConsumeResult.FAILURE : ConsumeResult.SUCCESS;
        };

        RetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3);
        when(consumer.getRetryPolicy()).thenReturn(retryPolicy);

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals("Should succeed on second attempt", 2, callCount.get());
            verify(processQueue, times(2)).eraseFifoMessage(any(), eq(ConsumeResult.SUCCESS));
            assertEquals(2, getAtomicLongField(consumer, "consumptionOkQuantity").get());
        });
    }

    @Test
    public void testFifoBatchRetriesEntireBatchAsWhole() {
        final int maxBatchSize = 3;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));
        List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger callCount = new AtomicInteger(0);

        BatchMessageListener listener = messages -> {
            callCount.incrementAndGet();
            batchSizes.add(messages.size());
            return ConsumeResult.FAILURE;
        };

        RetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3);
        when(consumer.getRetryPolicy()).thenReturn(retryPolicy);

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals("Should retry 3 times", 3, callCount.get());
            assertEquals("All retries should have same batch size", Arrays.asList(3, 3, 3), batchSizes);
        });
    }

    // ===== DLQ Tests =====

    @Test
    public void testFifoBatchDLQAfterMaxRetries() {
        final int maxBatchSize = 2;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));
        AtomicInteger callCount = new AtomicInteger(0);

        BatchMessageListener listener = messages -> {
            callCount.incrementAndGet();
            return ConsumeResult.FAILURE;
        };

        RetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3);
        when(consumer.getRetryPolicy()).thenReturn(retryPolicy);

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals("Should exhaust all attempts", 3, callCount.get());
            verify(processQueue, times(2)).discardFifoMessage(any());
            assertEquals(2, getAtomicLongField(consumer, "consumptionErrorQuantity").get());
        });
    }

    @Test
    public void testFifoBatchDLQAllMessagesInBatch() {
        final int maxBatchSize = 5;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));

        BatchMessageListener listener = messages -> ConsumeResult.FAILURE;

        RetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(2);
        when(consumer.getRetryPolicy()).thenReturn(retryPolicy);

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            messages.add(fakeMessageViewImpl());
        }
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            verify(processQueue, times(5)).discardFifoMessage(any());
            assertEquals(5, getAtomicLongField(consumer, "consumptionErrorQuantity").get());
        });
    }

    @Test
    public void testFifoBatchNoDLQOnSuccess() {
        final int maxBatchSize = 2;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));

        BatchMessageListener listener = messages -> ConsumeResult.SUCCESS;

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            verify(processQueue, times(2)).eraseFifoMessage(any(), eq(ConsumeResult.SUCCESS));
            verify(processQueue, never()).discardFifoMessage(any());
        });
    }

    // ===== FIFO-specific Behavior Tests =====

    @Test
    public void testFifoCorruptedMessageDiscardedWithFifoMethod() {
        final int maxBatchSize = 2;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofMillis(100));

        BatchMessageListener listener = messages -> ConsumeResult.SUCCESS;

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

    @Test
    public void testFifoBatchInFlightFlagReleasedAfterSuccess() throws InterruptedException {
        final int maxBatchSize = 1;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));
        CountDownLatch firstBatchDone = new CountDownLatch(1);
        AtomicInteger callCount = new AtomicInteger(0);

        BatchMessageListener listener = messages -> {
            callCount.incrementAndGet();
            firstBatchDone.countDown();
            return ConsumeResult.SUCCESS;
        };

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        service.consume(processQueue, Collections.singletonList(fakeMessageViewImpl()));
        assertTrue("First batch should complete", firstBatchDone.await(5, TimeUnit.SECONDS));

        service.consume(processQueue, Collections.singletonList(fakeMessageViewImpl()));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertEquals("Second batch should execute after first completes", 2, callCount.get())
        );
    }

    @Test
    public void testFifoBatchInFlightFlagReleasedAfterDLQ() throws InterruptedException {
        final int maxBatchSize = 1;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));
        AtomicInteger callCount = new AtomicInteger(0);

        BatchMessageListener listener = messages -> {
            callCount.incrementAndGet();
            return ConsumeResult.FAILURE;
        };

        RetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(1);
        when(consumer.getRetryPolicy()).thenReturn(retryPolicy);

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        service.consume(processQueue, Collections.singletonList(fakeMessageViewImpl()));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals("First batch should go to DLQ", 1, callCount.get());
            verify(processQueue, times(1)).discardFifoMessage(any());
        });

        service.consume(processQueue, Collections.singletonList(fakeMessageViewImpl()));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertEquals("Second batch should execute after first goes to DLQ", 2, callCount.get())
        );
    }

    @Test
    public void testFifoBatchFlushOnMaxBatchSize() {
        final int maxBatchSize = 3;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));
        AtomicInteger batchSize = new AtomicInteger(0);

        BatchMessageListener listener = messages -> {
            batchSize.set(messages.size());
            return ConsumeResult.SUCCESS;
        };

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        for (int i = 0; i < maxBatchSize; i++) {
            messages.add(fakeMessageViewImpl());
        }
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals(maxBatchSize, batchSize.get());
            verify(processQueue, times(maxBatchSize)).eraseFifoMessage(any(), eq(ConsumeResult.SUCCESS));
        });
    }

    @Test
    public void testFifoBatchFlushOnMaxWaitTime() {
        BatchPolicy policy = new BatchPolicy(Integer.MAX_VALUE, Integer.MAX_VALUE, Duration.ofMillis(100));
        AtomicInteger batchSize = new AtomicInteger(0);

        BatchMessageListener listener = messages -> {
            batchSize.set(messages.size());
            return ConsumeResult.SUCCESS;
        };

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = Collections.singletonList(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertEquals(1, batchSize.get())
        );
    }

    @Test
    public void testFifoGracefulShutdownDrainsBuffer() {
        BatchPolicy policy = new BatchPolicy(100, Integer.MAX_VALUE, Duration.ofSeconds(60));
        AtomicInteger batchSize = new AtomicInteger(0);

        BatchMessageListener listener = messages -> {
            batchSize.set(messages.size());
            return ConsumeResult.SUCCESS;
        };

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
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
    public void testFifoBatchMetricsOnSuccess() {
        final int maxBatchSize = 3;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));

        BatchMessageListener listener = messages -> ConsumeResult.SUCCESS;

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        for (int i = 0; i < maxBatchSize; i++) {
            messages.add(fakeMessageViewImpl());
        }
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals(maxBatchSize, getAtomicLongField(consumer, "consumptionOkQuantity").get());
            assertEquals(0, getAtomicLongField(consumer, "consumptionErrorQuantity").get());
        });
    }

    @Test
    public void testFifoBatchMetricsOnDLQ() {
        final int maxBatchSize = 2;
        BatchPolicy policy = new BatchPolicy(maxBatchSize, Integer.MAX_VALUE, Duration.ofSeconds(60));

        BatchMessageListener listener = messages -> ConsumeResult.FAILURE;

        RetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(1);
        when(consumer.getRetryPolicy()).thenReturn(retryPolicy);

        FifoBatchConsumeService service = new FifoBatchConsumeService(FAKE_CLIENT_ID, listener,
            policy, consumptionExecutor, consumer, scheduler);

        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        service.consume(processQueue, messages);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals(0, getAtomicLongField(consumer, "consumptionOkQuantity").get());
            assertEquals(maxBatchSize, getAtomicLongField(consumer, "consumptionErrorQuantity").get());
        });
    }

    // ===== Helper Methods =====

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
