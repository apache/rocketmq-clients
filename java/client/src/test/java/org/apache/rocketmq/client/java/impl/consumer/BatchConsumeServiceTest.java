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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.Futures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BatchConsumeServiceTest extends TestBase {
    private final ClientId clientId = new ClientId();
    private final MessageInterceptor interceptor = mock(MessageInterceptor.class);
    private ThreadPoolExecutor consumptionExecutor;
    private ScheduledExecutorService scheduler;

    @Before
    public void setUp() {
        consumptionExecutor = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), new ThreadFactoryImpl("TestBatchConsumption"));
        scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("TestBatchScheduler"));
    }

    @After
    public void tearDown() {
        consumptionExecutor.shutdownNow();
        scheduler.shutdownNow();
    }

    private ProcessQueue mockProcessQueue() {
        ProcessQueue pq = mock(ProcessQueue.class);
        Mockito.when(pq.getMessageQueue()).thenReturn(fakeMessageQueueImpl0());
        Mockito.when(pq.eraseFifoMessage(any(), any()))
            .thenReturn(Futures.immediateVoidFuture());
        return pq;
    }

    private List<MessageViewImpl> createMessages(int count) {
        return createMessages(count, 1);
    }

    private List<MessageViewImpl> createMessages(int count, int bodySize) {
        List<MessageViewImpl> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            messages.add(fakeMessageViewImpl(bodySize, false));
        }
        return messages;
    }

    // ==================== Concurrent Mode Tests ====================

    @Test
    public void testConcurrentBatchConsumeSuccess() {
        int batchSize = 4;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        CopyOnWriteArrayList<Integer> receivedBatchSizes = new CopyOnWriteArrayList<>();

        BatchConsumeService service = new BatchConsumeService(clientId,
            messageViews -> {
                receivedBatchSizes.add(messageViews.size());
                return ConsumeResult.SUCCESS;
            }, policy, consumptionExecutor, interceptor, scheduler);

        ProcessQueue pq = mockProcessQueue();
        service.consume(pq, createMessages(batchSize));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, receivedBatchSizes.size());
            assertEquals(batchSize, receivedBatchSizes.get(0).intValue());
        });

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(pq, Mockito.times(batchSize)).eraseMessage(any(), eq(ConsumeResult.SUCCESS)));
    }

    @Test
    public void testConcurrentBytesTriggerFlush() {
        int maxBatchBytes = 10;
        BatchPolicy policy = new BatchPolicy(100, maxBatchBytes, Duration.ofSeconds(30));
        CopyOnWriteArrayList<Integer> receivedBatchSizes = new CopyOnWriteArrayList<>();

        BatchConsumeService service = new BatchConsumeService(clientId,
            messageViews -> {
                receivedBatchSizes.add(messageViews.size());
                return ConsumeResult.SUCCESS;
            }, policy, consumptionExecutor, interceptor, scheduler);

        ProcessQueue pq = mockProcessQueue();
        // Each message has body size 5, so 2 messages (10 bytes) should trigger flush
        service.consume(pq, createMessages(3, 5));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            assertTrue("At least one batch should have been flushed", receivedBatchSizes.size() >= 1));
    }

    @Test
    public void testConcurrentTimeoutTriggerFlush() {
        BatchPolicy policy = new BatchPolicy(100, Duration.ofMillis(200));
        CopyOnWriteArrayList<Integer> receivedBatchSizes = new CopyOnWriteArrayList<>();

        BatchConsumeService service = new BatchConsumeService(clientId,
            messageViews -> {
                receivedBatchSizes.add(messageViews.size());
                return ConsumeResult.SUCCESS;
            }, policy, consumptionExecutor, interceptor, scheduler);

        ProcessQueue pq = mockProcessQueue();
        service.consume(pq, createMessages(2));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, receivedBatchSizes.size());
            assertEquals(2, receivedBatchSizes.get(0).intValue());
        });
    }

    @Test
    public void testConcurrentCrossPqMixing() {
        int batchSize = 4;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        CopyOnWriteArrayList<Integer> receivedBatchSizes = new CopyOnWriteArrayList<>();

        BatchConsumeService service = new BatchConsumeService(clientId,
            messageViews -> {
                receivedBatchSizes.add(messageViews.size());
                return ConsumeResult.SUCCESS;
            }, policy, consumptionExecutor, interceptor, scheduler);

        ProcessQueue pq1 = mockProcessQueue();
        ProcessQueue pq2 = mockProcessQueue();
        service.consume(pq1, createMessages(2));
        service.consume(pq2, createMessages(2));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, receivedBatchSizes.size());
            assertEquals(batchSize, receivedBatchSizes.get(0).intValue());
        });

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(pq1, Mockito.times(2)).eraseMessage(any(), eq(ConsumeResult.SUCCESS));
            verify(pq2, Mockito.times(2)).eraseMessage(any(), eq(ConsumeResult.SUCCESS));
        });
    }

    @Test
    public void testConcurrentCorruptedMessageFiltered() {
        int batchSize = 3;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofMillis(200));
        CopyOnWriteArrayList<Integer> receivedBatchSizes = new CopyOnWriteArrayList<>();

        BatchConsumeService service = new BatchConsumeService(clientId,
            messageViews -> {
                receivedBatchSizes.add(messageViews.size());
                return ConsumeResult.SUCCESS;
            }, policy, consumptionExecutor, interceptor, scheduler);

        ProcessQueue pq = mockProcessQueue();
        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl(false));
        messages.add(fakeMessageViewImpl(true));  // corrupted
        messages.add(fakeMessageViewImpl(false));
        service.consume(pq, messages);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, receivedBatchSizes.size());
            assertEquals(2, receivedBatchSizes.get(0).intValue());
        });

        verify(pq).discardMessage(any());
    }

    @Test
    public void testConcurrentConsumeFailure() {
        int batchSize = 3;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));

        BatchConsumeService service = new BatchConsumeService(clientId,
            messageViews -> ConsumeResult.FAILURE, policy, consumptionExecutor, interceptor, scheduler);

        ProcessQueue pq = mockProcessQueue();
        service.consume(pq, createMessages(batchSize));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(pq, Mockito.times(batchSize)).eraseMessage(any(), eq(ConsumeResult.FAILURE)));
    }

    @Test
    public void testConcurrentConsumeException() {
        int batchSize = 3;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));

        BatchConsumeService service = new BatchConsumeService(clientId,
            messageViews -> {
                throw new RuntimeException("test");
            }, policy, consumptionExecutor, interceptor, scheduler);

        ProcessQueue pq = mockProcessQueue();
        service.consume(pq, createMessages(batchSize));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(pq, Mockito.times(batchSize)).eraseMessage(any(), eq(ConsumeResult.FAILURE)));
    }

    @Test
    public void testConcurrentCloseFlushesRemainingMessages() {
        BatchPolicy policy = new BatchPolicy(100, Duration.ofSeconds(30));
        CopyOnWriteArrayList<Integer> receivedBatchSizes = new CopyOnWriteArrayList<>();

        BatchConsumeService service = new BatchConsumeService(clientId,
            messageViews -> {
                receivedBatchSizes.add(messageViews.size());
                return ConsumeResult.SUCCESS;
            }, policy, consumptionExecutor, interceptor, scheduler);

        ProcessQueue pq = mockProcessQueue();
        service.consume(pq, createMessages(3));
        assertEquals(0, receivedBatchSizes.size());

        service.close();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, receivedBatchSizes.size());
            assertEquals(3, receivedBatchSizes.get(0).intValue());
        });
    }

    // ==================== FIFO Mode Tests ====================

    private RetryPolicy mockRetryPolicy(int maxAttempts) {
        return createCustomizedBackoffRetryPolicy(maxAttempts);
    }

    @Test
    public void testFifoBatchConsumeSuccess() {
        int batchSize = 3;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));

        FifoBatchConsumeService service = new FifoBatchConsumeService(clientId,
            messageViews -> ConsumeResult.SUCCESS, policy, consumptionExecutor, interceptor, scheduler,
            () -> mockRetryPolicy(3));

        ProcessQueue pq = mockProcessQueue();
        service.consume(pq, createMessages(batchSize));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(pq, Mockito.times(batchSize)).eraseFifoMessage(any(), eq(ConsumeResult.SUCCESS)));
    }

    @Test
    public void testFifoFirstFailureTriggerRetry() {
        int batchSize = 2;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        AtomicInteger callCount = new AtomicInteger(0);

        FifoBatchConsumeService service = new FifoBatchConsumeService(clientId,
            messageViews -> {
                int count = callCount.incrementAndGet();
                return count == 1 ? ConsumeResult.FAILURE : ConsumeResult.SUCCESS;
            }, policy, consumptionExecutor, interceptor, scheduler,
            () -> mockRetryPolicy(3));

        ProcessQueue pq = mockProcessQueue();
        service.consume(pq, createMessages(batchSize));

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue("Listener should be called at least twice (1 fail + 1 success)",
                callCount.get() >= 2);
            verify(pq, Mockito.times(batchSize)).eraseFifoMessage(any(), eq(ConsumeResult.SUCCESS));
        });
    }

    @Test
    public void testFifoRetryThenSuccess() {
        int batchSize = 2;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        AtomicInteger callCount = new AtomicInteger(0);

        FifoBatchConsumeService service = new FifoBatchConsumeService(clientId,
            messageViews -> {
                int count = callCount.incrementAndGet();
                return count <= 2 ? ConsumeResult.FAILURE : ConsumeResult.SUCCESS;
            }, policy, consumptionExecutor, interceptor, scheduler,
            () -> mockRetryPolicy(5));

        ProcessQueue pq = mockProcessQueue();
        service.consume(pq, createMessages(batchSize));

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue("Listener should be called 3 times (2 fails + 1 success)", callCount.get() >= 3);
            verify(pq, Mockito.times(batchSize)).eraseFifoMessage(any(), eq(ConsumeResult.SUCCESS));
        });
    }

    @Test
    public void testFifoExhaustedRetryGoesToDlq() {
        int batchSize = 2;
        int maxAttempts = 2;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        AtomicInteger callCount = new AtomicInteger(0);

        FifoBatchConsumeService service = new FifoBatchConsumeService(clientId,
            messageViews -> {
                callCount.incrementAndGet();
                return ConsumeResult.FAILURE;
            }, policy, consumptionExecutor, interceptor, scheduler,
            () -> mockRetryPolicy(maxAttempts));

        ProcessQueue pq = mockProcessQueue();
        service.consume(pq, createMessages(batchSize));

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue("Listener should be called maxAttempts times", callCount.get() >= maxAttempts);
            verify(pq, Mockito.times(batchSize)).eraseFifoMessage(any(), eq(ConsumeResult.FAILURE));
        });
    }

    @Test
    public void testFifoSerialFlush() {
        int batchSize = 2;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        CopyOnWriteArrayList<Integer> receivedBatchSizes = new CopyOnWriteArrayList<>();
        AtomicInteger callCount = new AtomicInteger(0);

        FifoBatchConsumeService service = new FifoBatchConsumeService(clientId,
            messageViews -> {
                receivedBatchSizes.add(messageViews.size());
                callCount.incrementAndGet();
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return ConsumeResult.SUCCESS;
            }, policy, consumptionExecutor, interceptor, scheduler,
            () -> mockRetryPolicy(3));

        ProcessQueue pq = mockProcessQueue();
        // First batch fills immediately
        service.consume(pq, createMessages(batchSize));
        // Second batch should be buffered while first is in-flight
        service.consume(pq, createMessages(batchSize));

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals("Both batches should have been consumed", 2, callCount.get());
            assertEquals(2, receivedBatchSizes.size());
        });
    }

    @Test
    public void testFifoConsumeException() {
        int batchSize = 2;
        int maxAttempts = 2;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        AtomicInteger callCount = new AtomicInteger(0);

        FifoBatchConsumeService service = new FifoBatchConsumeService(clientId,
            messageViews -> {
                callCount.incrementAndGet();
                throw new RuntimeException("test exception");
            }, policy, consumptionExecutor, interceptor, scheduler,
            () -> mockRetryPolicy(maxAttempts));

        ProcessQueue pq = mockProcessQueue();
        service.consume(pq, createMessages(batchSize));

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue("Listener should be called maxAttempts times", callCount.get() >= maxAttempts);
            verify(pq, Mockito.times(batchSize)).eraseFifoMessage(any(), eq(ConsumeResult.FAILURE));
        });
    }

    @Test
    public void testFifoPartialExhaustionSendsToDlq() {
        int batchSize = 3;
        int maxAttempts = 2;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofSeconds(5));
        AtomicInteger callCount = new AtomicInteger(0);

        FifoBatchConsumeService service = new FifoBatchConsumeService(clientId,
            messageViews -> {
                callCount.incrementAndGet();
                return ConsumeResult.FAILURE;
            }, policy, consumptionExecutor, interceptor, scheduler,
            () -> mockRetryPolicy(maxAttempts));

        ProcessQueue pq = mockProcessQueue();

        // Create messages with different delivery attempts:
        // msg0: attempt=1 (already near max), msg1: attempt=0, msg2: attempt=0
        List<MessageViewImpl> messages = createMessages(batchSize);
        messages.get(0).incrementAndGetDeliveryAttempt(); // now attempt=1

        service.consume(pq, messages);

        // After first failure: msg0 (attempt=1 >= maxAttempts=2) goes to DLQ,
        // msg1 and msg2 (attempt=0 < 2) get incremented and retried.
        // After second failure: msg1 and msg2 (attempt=1 >= 2) go to DLQ.
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue("Listener should be called at least twice", callCount.get() >= 2);
            // All 3 messages eventually go to DLQ
            verify(pq, Mockito.times(batchSize)).eraseFifoMessage(any(), eq(ConsumeResult.FAILURE));
        });
    }

    @Test
    public void testFifoCorruptedMessageFiltered() {
        int batchSize = 2;
        BatchPolicy policy = new BatchPolicy(batchSize, Duration.ofMillis(200));
        CopyOnWriteArrayList<Integer> receivedBatchSizes = new CopyOnWriteArrayList<>();

        FifoBatchConsumeService service = new FifoBatchConsumeService(clientId,
            messageViews -> {
                receivedBatchSizes.add(messageViews.size());
                return ConsumeResult.SUCCESS;
            }, policy, consumptionExecutor, interceptor, scheduler,
            () -> mockRetryPolicy(3));

        ProcessQueue pq = mockProcessQueue();
        List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl(false));
        messages.add(fakeMessageViewImpl(true));  // corrupted
        messages.add(fakeMessageViewImpl(false));
        service.consume(pq, messages);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, receivedBatchSizes.size());
            assertEquals(2, receivedBatchSizes.get(0).intValue());
        });

        verify(pq).discardFifoMessage(any());
    }
}
