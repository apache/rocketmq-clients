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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.consumer.BatchMessageListener;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class BatchConsumeServiceTest extends TestBase {

    private final ClientId clientId = new ClientId();
    private final MessageInterceptor interceptor = mock(MessageInterceptor.class);
    private final ThreadPoolExecutor consumptionExecutor = new ThreadPoolExecutor(
        2, 2, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(), new ThreadFactoryImpl("TestBatchConsumption"));
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(
        1, new ThreadFactoryImpl("TestBatchScheduler"));

    /**
     * When the number of messages reaches maxBatchCount, the batch should be flushed immediately.
     */
    @Test
    public void testFlushOnMaxBatchSize() throws InterruptedException {
        final int maxBatchCount = 3;
        final CopyOnWriteArrayList<List<MessageView>> capturedBatches = new CopyOnWriteArrayList<>();
        final BatchMessageListener listener = views -> {
            capturedBatches.add(new ArrayList<>(views));
            return ConsumeResult.SUCCESS;
        };
        final BatchPolicy policy = new BatchPolicy(maxBatchCount, Duration.ofSeconds(30));
        final ProcessQueue pq = mock(ProcessQueue.class);
        when(pq.getMessageQueue()).thenReturn(fakeMessageQueueImpl0());

        final BatchConsumeService service = new BatchConsumeService(
            clientId, listener, policy, consumptionExecutor, interceptor, scheduler);

        // Send exactly maxBatchCount messages.
        final List<MessageViewImpl> messages = new ArrayList<>();
        for (int i = 0; i < maxBatchCount; i++) {
            messages.add(fakeMessageViewImpl());
        }
        service.consume(pq, messages);

        // Wait for async consumption.
        Thread.sleep(500);

        assertEquals(1, capturedBatches.size());
        assertEquals(maxBatchCount, capturedBatches.get(0).size());
        verify(pq, timeout(1000).times(maxBatchCount)).eraseMessage(any(MessageViewImpl.class),
            eq(ConsumeResult.SUCCESS));
    }

    /**
     * When the number of messages is below maxBatchCount, nothing should be flushed until
     * maxWaitTime elapses.
     */
    @Test
    public void testBufferUntilBatchReady() throws InterruptedException {
        final int maxBatchCount = 5;
        final CopyOnWriteArrayList<List<MessageView>> capturedBatches = new CopyOnWriteArrayList<>();
        final BatchMessageListener listener = views -> {
            capturedBatches.add(new ArrayList<>(views));
            return ConsumeResult.SUCCESS;
        };
        // Long wait time so timeout won't trigger during the test.
        final BatchPolicy policy = new BatchPolicy(maxBatchCount, Duration.ofSeconds(60));
        final ProcessQueue pq = mock(ProcessQueue.class);
        when(pq.getMessageQueue()).thenReturn(fakeMessageQueueImpl0());

        final BatchConsumeService service = new BatchConsumeService(
            clientId, listener, policy, consumptionExecutor, interceptor, scheduler);

        // Send fewer than maxBatchCount messages.
        final List<MessageViewImpl> messages = new ArrayList<>();
        for (int i = 0; i < maxBatchCount - 1; i++) {
            messages.add(fakeMessageViewImpl());
        }
        service.consume(pq, messages);

        Thread.sleep(300);
        // Not enough to form a batch, nothing should be dispatched.
        assertTrue("Batch should not be flushed yet", capturedBatches.isEmpty());

        // Now send one more to reach the threshold.
        service.consume(pq, Collections.singletonList(fakeMessageViewImpl()));

        Thread.sleep(500);
        assertEquals(1, capturedBatches.size());
        assertEquals(maxBatchCount, capturedBatches.get(0).size());
    }

    /**
     * When accumulated body bytes reach maxBatchBytes, the batch should be flushed.
     */
    @Test
    public void testFlushOnMaxBatchBytes() throws InterruptedException {
        final int bodySize = 512;
        final long maxBatchBytes = bodySize * 2; // Two messages will exceed the limit.
        final CopyOnWriteArrayList<List<MessageView>> capturedBatches = new CopyOnWriteArrayList<>();
        final BatchMessageListener listener = views -> {
            capturedBatches.add(new ArrayList<>(views));
            return ConsumeResult.SUCCESS;
        };
        // Large maxBatchCount so only bytes trigger flush.
        final BatchPolicy policy = new BatchPolicy(100, maxBatchBytes, Duration.ofSeconds(30));
        final ProcessQueue pq = mock(ProcessQueue.class);
        when(pq.getMessageQueue()).thenReturn(fakeMessageQueueImpl0());

        final BatchConsumeService service = new BatchConsumeService(
            clientId, listener, policy, consumptionExecutor, interceptor, scheduler);

        final List<MessageViewImpl> messages = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            messages.add(fakeMessageViewImpl(bodySize, false));
        }
        service.consume(pq, messages);

        Thread.sleep(500);
        assertEquals(1, capturedBatches.size());
        assertEquals(2, capturedBatches.get(0).size());
    }

    /**
     * When maxWaitTime expires, any buffered messages should be flushed as a partial batch.
     */
    @Test
    public void testFlushOnMaxWaitTime() throws InterruptedException {
        final CopyOnWriteArrayList<List<MessageView>> capturedBatches = new CopyOnWriteArrayList<>();
        final BatchMessageListener listener = views -> {
            capturedBatches.add(new ArrayList<>(views));
            return ConsumeResult.SUCCESS;
        };
        // Large maxBatchCount so only maxWaitTime triggers flush.
        final BatchPolicy policy = new BatchPolicy(100, Duration.ofMillis(200));
        final ProcessQueue pq = mock(ProcessQueue.class);
        when(pq.getMessageQueue()).thenReturn(fakeMessageQueueImpl0());

        final BatchConsumeService service = new BatchConsumeService(
            clientId, listener, policy, consumptionExecutor, interceptor, scheduler);

        // Send 2 messages (far below maxBatchCount=100).
        final List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl());
        messages.add(fakeMessageViewImpl());
        service.consume(pq, messages);

        // Immediately after, batch shouldn't be flushed yet.
        assertTrue(capturedBatches.isEmpty());

        // Wait for maxWaitTime (200ms) + some margin.
        Thread.sleep(500);

        assertEquals(1, capturedBatches.size());
        assertEquals(2, capturedBatches.get(0).size());
    }

    /**
     * Corrupted messages should be discarded and never enter the batch buffer.
     */
    @Test
    public void testCorruptedMessagesDiscarded() throws InterruptedException {
        final int maxBatchCount = 2;
        final CopyOnWriteArrayList<List<MessageView>> capturedBatches = new CopyOnWriteArrayList<>();
        final BatchMessageListener listener = views -> {
            capturedBatches.add(new ArrayList<>(views));
            return ConsumeResult.SUCCESS;
        };
        final BatchPolicy policy = new BatchPolicy(maxBatchCount, Duration.ofSeconds(30));
        final ProcessQueue pq = mock(ProcessQueue.class);
        when(pq.getMessageQueue()).thenReturn(fakeMessageQueueImpl0());

        final BatchConsumeService service = new BatchConsumeService(
            clientId, listener, policy, consumptionExecutor, interceptor, scheduler);

        // Send 1 corrupted + 1 normal message.
        final List<MessageViewImpl> messages = new ArrayList<>();
        messages.add(fakeMessageViewImpl(true));  // corrupted
        messages.add(fakeMessageViewImpl(false)); // normal
        service.consume(pq, messages);

        Thread.sleep(300);
        // Only 1 valid message, not enough to fill batch of 2.
        assertTrue("Batch should not be flushed yet with only 1 valid message", capturedBatches.isEmpty());
        verify(pq).discardMessage(any(MessageViewImpl.class));
    }

    /**
     * When a single consume call brings more messages than maxBatchCount, multiple batches
     * should be flushed.
     */
    @Test
    public void testMultipleBatchesFromSingleConsume() throws InterruptedException {
        final int maxBatchCount = 2;
        final CopyOnWriteArrayList<List<MessageView>> capturedBatches = new CopyOnWriteArrayList<>();
        final BatchMessageListener listener = views -> {
            capturedBatches.add(new ArrayList<>(views));
            return ConsumeResult.SUCCESS;
        };
        final BatchPolicy policy = new BatchPolicy(maxBatchCount, Duration.ofSeconds(30));
        final ProcessQueue pq = mock(ProcessQueue.class);
        when(pq.getMessageQueue()).thenReturn(fakeMessageQueueImpl0());

        final BatchConsumeService service = new BatchConsumeService(
            clientId, listener, policy, consumptionExecutor, interceptor, scheduler);

        // Send 5 messages with maxBatchCount=2 → expect 2 full batches (2+2), 1 leftover.
        final List<MessageViewImpl> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            messages.add(fakeMessageViewImpl());
        }
        service.consume(pq, messages);

        Thread.sleep(500);
        // Should have at least 2 complete batches flushed immediately.
        assertTrue("Expected at least 2 batches", capturedBatches.size() >= 2);
        assertEquals(maxBatchCount, capturedBatches.get(0).size());
        assertEquals(maxBatchCount, capturedBatches.get(1).size());
    }

    /**
     * A single message that exceeds maxBatchBytes should still be flushed to guarantee
     * forward progress.
     */
    @Test
    public void testSingleOversizedMessageStillFlushed() throws InterruptedException {
        final int bodySize = 2048;
        final long maxBatchBytes = 100; // Much smaller than bodySize.
        final CopyOnWriteArrayList<List<MessageView>> capturedBatches = new CopyOnWriteArrayList<>();
        final BatchMessageListener listener = views -> {
            capturedBatches.add(new ArrayList<>(views));
            return ConsumeResult.SUCCESS;
        };
        // maxBatchCount=1 so a single message triggers flush, bytes won't block it.
        final BatchPolicy policy = new BatchPolicy(1, maxBatchBytes, Duration.ofSeconds(30));
        final ProcessQueue pq = mock(ProcessQueue.class);
        when(pq.getMessageQueue()).thenReturn(fakeMessageQueueImpl0());

        final BatchConsumeService service = new BatchConsumeService(
            clientId, listener, policy, consumptionExecutor, interceptor, scheduler);

        service.consume(pq, Collections.singletonList(fakeMessageViewImpl(bodySize, false)));

        Thread.sleep(500);
        assertEquals(1, capturedBatches.size());
        assertEquals(1, capturedBatches.get(0).size());
    }

    /**
     * When the listener returns FAILURE, all messages in the batch should be erased
     * with FAILURE result.
     */
    @Test
    public void testBatchFailureErasesAllMessages() throws InterruptedException {
        final int maxBatchCount = 3;
        final BatchMessageListener listener = views -> ConsumeResult.FAILURE;
        final BatchPolicy policy = new BatchPolicy(maxBatchCount, Duration.ofSeconds(30));
        final ProcessQueue pq = mock(ProcessQueue.class);
        when(pq.getMessageQueue()).thenReturn(fakeMessageQueueImpl0());

        final BatchConsumeService service = new BatchConsumeService(
            clientId, listener, policy, consumptionExecutor, interceptor, scheduler);

        final List<MessageViewImpl> messages = new ArrayList<>();
        for (int i = 0; i < maxBatchCount; i++) {
            messages.add(fakeMessageViewImpl());
        }
        service.consume(pq, messages);

        // All messages should be erased with FAILURE.
        verify(pq, timeout(1000).times(maxBatchCount)).eraseMessage(any(MessageViewImpl.class),
            eq(ConsumeResult.FAILURE));
    }

    /**
     * Calling close() should flush all remaining buffered messages (even if below maxBatchCount)
     * so they are consumed before the consumer shuts down.
     */
    @Test
    public void testCloseFlushesRemainingMessages() throws InterruptedException {
        final int maxBatchCount = 10;
        final CopyOnWriteArrayList<List<MessageView>> capturedBatches = new CopyOnWriteArrayList<>();
        final BatchMessageListener listener = views -> {
            capturedBatches.add(new ArrayList<>(views));
            return ConsumeResult.SUCCESS;
        };
        // Long wait time so timeout won't trigger during the test.
        final BatchPolicy policy = new BatchPolicy(maxBatchCount, Duration.ofSeconds(60));
        final ProcessQueue pq = mock(ProcessQueue.class);
        when(pq.getMessageQueue()).thenReturn(fakeMessageQueueImpl0());

        final BatchConsumeService service = new BatchConsumeService(
            clientId, listener, policy, consumptionExecutor, interceptor, scheduler);

        // Send fewer than maxBatchCount messages — not enough to trigger auto-flush.
        final List<MessageViewImpl> messages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            messages.add(fakeMessageViewImpl());
        }
        service.consume(pq, messages);

        Thread.sleep(300);
        assertTrue("Batch should not be flushed yet", capturedBatches.isEmpty());

        // Close should flush the remaining 3 messages.
        service.close();

        Thread.sleep(500);
        assertEquals(1, capturedBatches.size());
        assertEquals(3, capturedBatches.get(0).size());
        verify(pq, timeout(1000).times(3)).eraseMessage(any(MessageViewImpl.class),
            eq(ConsumeResult.SUCCESS));
    }

    /**
     * Messages from different ProcessQueues should be correctly routed back to their
     * owning ProcessQueue after batch consumption.
     */
    @Test
    public void testMultipleProcessQueuesInSingleBuffer() throws InterruptedException {
        final int maxBatchCount = 4;
        final CopyOnWriteArrayList<List<MessageView>> capturedBatches = new CopyOnWriteArrayList<>();
        final BatchMessageListener listener = views -> {
            capturedBatches.add(new ArrayList<>(views));
            return ConsumeResult.SUCCESS;
        };
        final BatchPolicy policy = new BatchPolicy(maxBatchCount, Duration.ofSeconds(30));
        final ProcessQueue pq1 = mock(ProcessQueue.class);
        final ProcessQueue pq2 = mock(ProcessQueue.class);
        when(pq1.getMessageQueue()).thenReturn(fakeMessageQueueImpl0());
        when(pq2.getMessageQueue()).thenReturn(fakeMessageQueueImpl1());

        final BatchConsumeService service = new BatchConsumeService(
            clientId, listener, policy, consumptionExecutor, interceptor, scheduler);

        // Send 2 messages from pq1.
        final List<MessageViewImpl> messages1 = new ArrayList<>();
        messages1.add(fakeMessageViewImpl());
        messages1.add(fakeMessageViewImpl());
        service.consume(pq1, messages1);

        // Send 2 messages from pq2 — total 4, reaching maxBatchCount.
        final List<MessageViewImpl> messages2 = new ArrayList<>();
        messages2.add(fakeMessageViewImpl());
        messages2.add(fakeMessageViewImpl());
        service.consume(pq2, messages2);

        Thread.sleep(500);
        assertEquals(1, capturedBatches.size());
        assertEquals(maxBatchCount, capturedBatches.get(0).size());
        // Each PQ should have 2 messages erased.
        verify(pq1, timeout(1000).times(2)).eraseMessage(any(MessageViewImpl.class),
            eq(ConsumeResult.SUCCESS));
        verify(pq2, timeout(1000).times(2)).eraseMessage(any(MessageViewImpl.class),
            eq(ConsumeResult.SUCCESS));
    }
}
