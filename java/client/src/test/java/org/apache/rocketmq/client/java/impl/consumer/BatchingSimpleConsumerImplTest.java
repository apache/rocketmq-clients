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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BatchingSimpleConsumerImplTest extends TestBase {

    private static final Duration INVISIBLE_DURATION = Duration.ofSeconds(10);

    @Mock
    private SimpleConsumer delegate;

    private BatchingSimpleConsumerImpl consumer;

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() throws Exception {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }

    private BatchingSimpleConsumerImpl createConsumer(BatchPolicy policy) {
        return BatchingSimpleConsumerImpl.create(delegate, policy);
    }

    private List<MessageView> fakeMessages(int count, int bodySize) {
        List<MessageView> msgs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            msgs.add(fakeMessageViewImpl(bodySize, false));
        }
        return msgs;
    }

    // -----------------------------------------------------------------------
    // 1. Batch fulfilled by count in a single round
    // -----------------------------------------------------------------------

    @Test
    public void testBatchCompletedByCountSingleRound() throws Exception {
        final int maxBatch = 3;
        final BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        consumer = createConsumer(policy);

        when(delegate.receiveAsync(anyInt(), any(Duration.class)))
            .thenReturn(CompletableFuture.completedFuture(fakeMessages(maxBatch, 10)));

        List<MessageView> result = consumer.batchReceiveAsync(INVISIBLE_DURATION)
            .get(5, TimeUnit.SECONDS);
        assertEquals(maxBatch, result.size());
    }

    // -----------------------------------------------------------------------
    // 2. Batch fulfilled by bytes limit
    // -----------------------------------------------------------------------

    @Test
    public void testBatchCompletedByBytes() throws Exception {
        final int bodySize = 1024;
        final long maxBatchBytes = 2 * 1024L;
        final int maxBatchCount = 100;

        final BatchPolicy policy = new BatchPolicy(maxBatchCount, maxBatchBytes, Duration.ofSeconds(5));
        consumer = createConsumer(policy);

        // Each receive returns 2 messages (2KB total), triggering bytes limit.
        when(delegate.receiveAsync(anyInt(), any(Duration.class)))
            .thenReturn(CompletableFuture.completedFuture(fakeMessages(2, bodySize)));

        List<MessageView> result = consumer.batchReceiveAsync(INVISIBLE_DURATION)
            .get(5, TimeUnit.SECONDS);
        // First round fires multiple receives; bytes accumulate past 2KB.
        // After the round, bytes >= maxBatchBytes → no more rounds.
        assertTrue(result.size() >= 2);
    }

    // -----------------------------------------------------------------------
    // 3. Deadline starts from first message, not from call start
    // -----------------------------------------------------------------------

    @Test
    public void testDeadlineStartsFromFirstMessage() throws Exception {
        final int maxBatch = 100;
        final Duration maxWait = Duration.ofMillis(200);
        final BatchPolicy policy = new BatchPolicy(maxBatch, maxWait);
        consumer = createConsumer(policy);

        // Round 1: returns 2 messages (starts the deadline clock).
        // Round 2+: returns empty, so no new messages. Deadline fires.
        when(delegate.receiveAsync(anyInt(), any(Duration.class)))
            .thenReturn(CompletableFuture.completedFuture(fakeMessages(2, 10)))
            .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        List<MessageView> result = consumer.batchReceiveAsync(INVISIBLE_DURATION)
            .get(5, TimeUnit.SECONDS);
        // Got the 2 messages from round 1; deadline expired → returned.
        assertEquals(2, result.size());
    }

    // -----------------------------------------------------------------------
    // 4. Adaptive concurrency: multiple rounds
    // -----------------------------------------------------------------------

    @Test
    public void testAdaptiveMultipleRounds() throws Exception {
        final int maxBatch = 64;
        final BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        consumer = createConsumer(policy);

        // Round 1: 2 receives → 32 + 16 = 48
        // Round 2: 1 receive  → 16
        // Total = 64
        when(delegate.receiveAsync(anyInt(), any(Duration.class)))
            .thenReturn(CompletableFuture.completedFuture(fakeMessages(32, 10)))
            .thenReturn(CompletableFuture.completedFuture(fakeMessages(16, 10)))
            .thenReturn(CompletableFuture.completedFuture(fakeMessages(16, 10)));

        List<MessageView> result = consumer.batchReceiveAsync(INVISIBLE_DURATION)
            .get(5, TimeUnit.SECONDS);
        assertEquals(maxBatch, result.size());
    }

    // -----------------------------------------------------------------------
    // 5. MaxInflight caps concurrency
    // -----------------------------------------------------------------------

    @Test
    public void testMaxInflightConcurrency() throws Exception {
        final int maxBatch = 200;
        final int maxInflight = 2;
        final BatchPolicy policy = new BatchPolicy(maxBatch, BatchPolicy.DEFAULT_MAX_BATCH_BYTES,
            Duration.ofSeconds(5), maxInflight);
        consumer = createConsumer(policy);

        when(delegate.receiveAsync(anyInt(), any(Duration.class)))
            .thenAnswer(invocation -> {
                int requested = invocation.getArgument(0);
                return CompletableFuture.completedFuture(fakeMessages(requested, 10));
            });

        List<MessageView> result = consumer.batchReceiveAsync(INVISIBLE_DURATION)
            .get(10, TimeUnit.SECONDS);
        assertEquals(maxBatch, result.size());
    }

    // -----------------------------------------------------------------------
    // 6. batchReceive with specific topic
    // -----------------------------------------------------------------------

    @Test
    public void testBatchReceiveWithTopic() throws Exception {
        final int maxBatch = 5;
        final BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        consumer = createConsumer(policy);

        when(delegate.receiveAsync(eq("my-topic"), anyInt(), any(Duration.class)))
            .thenReturn(CompletableFuture.completedFuture(fakeMessages(5, 10)));

        List<MessageView> result = consumer.batchReceiveAsync("my-topic", INVISIBLE_DURATION)
            .get(5, TimeUnit.SECONDS);
        assertEquals(maxBatch, result.size());
        verify(delegate, atLeast(1)).receiveAsync(eq("my-topic"), anyInt(), any(Duration.class));
    }

    // -----------------------------------------------------------------------
    // 7. Inflight requests always complete — never cancelled
    // -----------------------------------------------------------------------

    @Test
    public void testInflightRequestsAlwaysComplete() throws Exception {
        // maxBatch=64 → concurrency=min(ceil(64/32),4)=2, so two receives fire in round 1.
        final int maxBatch = 64;
        final Duration maxWait = Duration.ofMillis(100);
        final BatchPolicy policy = new BatchPolicy(maxBatch, maxWait);
        consumer = createConsumer(policy);

        final AtomicInteger completedCount = new AtomicInteger();

        // First receive returns 32 messages fast → starts the 100ms deadline.
        // Second receive is slow (300ms) but must still be awaited.
        when(delegate.receiveAsync(anyInt(), any(Duration.class)))
            .thenAnswer(invocation -> {
                int requested = invocation.getArgument(0);
                completedCount.incrementAndGet();
                return CompletableFuture.completedFuture(fakeMessages(requested, 10));
            })
            .thenAnswer(invocation -> {
                CompletableFuture<List<MessageView>> f = new CompletableFuture<>();
                new Thread(() -> {
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    int requested = invocation.getArgument(0);
                    f.complete(fakeMessages(requested, 10));
                    completedCount.incrementAndGet();
                }).start();
                return f;
            })
            // Subsequent rounds return empty (deadline already expired).
            .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        List<MessageView> result = consumer.batchReceiveAsync(INVISIBLE_DURATION)
            .get(5, TimeUnit.SECONDS);

        // Both round-1 receives completed (not cancelled).
        assertTrue("All inflight receives should complete", completedCount.get() >= 2);
        // Got results from both the fast and slow receive.
        assertTrue("Should have results from both receives", result.size() > 32);
    }

    // -----------------------------------------------------------------------
    // 8. Delegate error is handled gracefully — round continues
    // -----------------------------------------------------------------------

    @Test
    public void testDelegateExceptionPartialResult() throws Exception {
        final int maxBatch = 64;
        final Duration maxWait = Duration.ofMillis(200);
        final BatchPolicy policy = new BatchPolicy(maxBatch, maxWait);
        consumer = createConsumer(policy);

        // First receive succeeds (32), second fails, then empty.
        when(delegate.receiveAsync(anyInt(), any(Duration.class)))
            .thenReturn(CompletableFuture.completedFuture(fakeMessages(32, 10)))
            .thenReturn(failedFuture(new RuntimeException("transient error")))
            .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        List<MessageView> result = consumer.batchReceiveAsync(INVISIBLE_DURATION)
            .get(5, TimeUnit.SECONDS);
        assertTrue(result.size() >= 32);
    }

    // -----------------------------------------------------------------------
    // 9. receive(int, Duration) delegates directly
    // -----------------------------------------------------------------------

    @Test
    public void testReceiveDelegatesDirectly() throws Exception {
        final BatchPolicy policy = new BatchPolicy(10, Duration.ofSeconds(5));
        consumer = createConsumer(policy);

        when(delegate.receive(eq(5), any(Duration.class)))
            .thenReturn(fakeMessages(3, 10));

        List<MessageView> result = consumer.receive(5, INVISIBLE_DURATION);
        assertEquals(3, result.size());
        verify(delegate).receive(eq(5), any(Duration.class));
    }

    // -----------------------------------------------------------------------
    // 10. receive(String, int, Duration) delegates directly
    // -----------------------------------------------------------------------

    @Test
    public void testReceiveWithTopicDelegatesDirectly() throws Exception {
        final BatchPolicy policy = new BatchPolicy(10, Duration.ofSeconds(5));
        consumer = createConsumer(policy);

        when(delegate.receive(eq("topicA"), eq(5), any(Duration.class)))
            .thenReturn(fakeMessages(3, 10));

        List<MessageView> result = consumer.receive("topicA", 5, INVISIBLE_DURATION);
        assertEquals(3, result.size());
        verify(delegate).receive(eq("topicA"), eq(5), any(Duration.class));
    }

    // -----------------------------------------------------------------------
    // 11. No excess messages: total requested = remaining
    // -----------------------------------------------------------------------

    @Test
    public void testNoExcessMessages() throws Exception {
        final int maxBatch = 10;
        final BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        consumer = createConsumer(policy);

        when(delegate.receiveAsync(anyInt(), any(Duration.class)))
            .thenAnswer(invocation -> {
                int requested = invocation.getArgument(0);
                return CompletableFuture.completedFuture(fakeMessages(requested, 10));
            });

        List<MessageView> result = consumer.batchReceiveAsync(INVISIBLE_DURATION)
            .get(5, TimeUnit.SECONDS);
        assertEquals(maxBatch, result.size());
    }

    // -----------------------------------------------------------------------
    // 12. Close stops new rounds
    // -----------------------------------------------------------------------

    @Test
    public void testCloseStopsNewRounds() throws Exception {
        final int maxBatch = 100;
        final BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(30));
        consumer = createConsumer(policy);

        // First receive returns 3 messages, second triggers close.
        when(delegate.receiveAsync(anyInt(), any(Duration.class)))
            .thenReturn(CompletableFuture.completedFuture(fakeMessages(3, 10)))
            .thenAnswer(invocation -> {
                consumer.close();
                return CompletableFuture.completedFuture(Collections.emptyList());
            });

        List<MessageView> result = consumer.batchReceiveAsync(INVISIBLE_DURATION)
            .get(5, TimeUnit.SECONDS);
        // Closed after first round; should have partial result.
        assertTrue(result.size() >= 3);
        consumer = null; // Already closed.
    }

    // -----------------------------------------------------------------------
    // 13. Concurrent batchReceive from multiple threads
    // -----------------------------------------------------------------------

    @Test
    public void testConcurrentBatchReceive() throws Exception {
        final int maxBatch = 3;
        final int threadCount = 5;
        final BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        consumer = createConsumer(policy);

        when(delegate.receiveAsync(anyInt(), any(Duration.class)))
            .thenAnswer(invocation -> {
                int requested = invocation.getArgument(0);
                return CompletableFuture.completedFuture(fakeMessages(requested, 10));
            });

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<CompletableFuture<List<MessageView>>> futures =
            Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                futures.add(consumer.batchReceiveAsync(INVISIBLE_DURATION));
                doneLatch.countDown();
            }).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS));

        for (CompletableFuture<List<MessageView>> f : futures) {
            List<MessageView> msgs = f.get(10, TimeUnit.SECONDS);
            assertEquals(maxBatch, msgs.size());
        }
    }

    // -----------------------------------------------------------------------
    // 14. Effective invisible duration extended by maxWaitTime
    // -----------------------------------------------------------------------

    @Test
    public void testEffectiveInvisibleDuration() throws Exception {
        final Duration maxWait = Duration.ofSeconds(3);
        final BatchPolicy policy = new BatchPolicy(1, maxWait);
        consumer = createConsumer(policy);

        when(delegate.receiveAsync(anyInt(), any(Duration.class)))
            .thenReturn(CompletableFuture.completedFuture(fakeMessages(1, 10)));

        consumer.batchReceiveAsync(INVISIBLE_DURATION).get(5, TimeUnit.SECONDS);

        Duration expected = INVISIBLE_DURATION.plus(maxWait);
        verify(delegate).receiveAsync(anyInt(), eq(expected));
    }

    // -----------------------------------------------------------------------
    // 15. batchReceiveAsync with topic
    // -----------------------------------------------------------------------

    @Test
    public void testBatchReceiveAsyncWithTopic() throws Exception {
        final int maxBatch = 3;
        final BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        consumer = createConsumer(policy);

        when(delegate.receiveAsync(eq("topicB"), anyInt(), any(Duration.class)))
            .thenReturn(CompletableFuture.completedFuture(fakeMessages(maxBatch, 10)));

        CompletableFuture<List<MessageView>> future =
            consumer.batchReceiveAsync("topicB", INVISIBLE_DURATION);
        List<MessageView> result = future.get(5, TimeUnit.SECONDS);
        assertEquals(maxBatch, result.size());
        verify(delegate, atLeast(1)).receiveAsync(eq("topicB"), anyInt(), any(Duration.class));
    }

    // -----------------------------------------------------------------------
    // 16. Close delegates to underlying consumer
    // -----------------------------------------------------------------------

    @Test
    public void testCloseDelegates() throws Exception {
        final BatchPolicy policy = new BatchPolicy(10, Duration.ofSeconds(5));
        consumer = createConsumer(policy);
        consumer.close();
        verify(delegate).close();
        consumer = null;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(t);
        return f;
    }
}
