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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SimpleConsumer} decorator that adds <strong>adaptive batch-receive</strong>
 * capability on top of the delegate's standard {@code receive} calls.
 *
 * <h3>Design — fully asynchronous, no background thread, no cache</h3>
 * <p>Each {@link #batchReceiveAsync} call directly converts into multiple concurrent
 * {@code receiveAsync} calls on the delegate, adaptively adjusting the concurrency and
 * per-receive message count based on the {@link BatchPolicy} and the number of messages
 * the server actually returns.
 *
 * <h3>Key semantics</h3>
 * <ul>
 *   <li><strong>Deadline from first message</strong>: the {@link BatchPolicy#getMaxWaitTime()}
 *       clock starts ticking only after the first message is received — not from the start
 *       of the call.  If no messages arrive, the method keeps retrying (bounded by the
 *       server-side long-poll timeout per round).</li>
 *   <li><strong>Inflight requests always complete</strong>: once a round of {@code receive}
 *       calls has been fired, the method waits for <em>all</em> of them to return before
 *       evaluating whether to start a new round.  Receives are never cancelled mid-flight.</li>
 *   <li><strong>No excess messages</strong>: the total count requested across all concurrent
 *       receives in a round equals exactly the remaining count needed, so no surplus messages
 *       are pulled from the server.</li>
 * </ul>
 *
 * <h3>Adaptive algorithm</h3>
 * <pre>
 * remaining = maxBatchCount         // e.g. 64
 * deadline  = NOT_STARTED
 * loop:
 *     if satisfied or deadline expired → return result
 *     concurrency = min(ceil(remaining / FETCH_BATCH_SIZE), maxInflight)
 *     evenly distribute remaining across concurrency receives
 *     fire all receives concurrently
 *     wait for ALL to complete (never cancel)
 *     collect results; on first message start deadline = now + maxWaitTime
 *     goto loop
 * </pre>
 *
 * @see BatchPolicy
 */
public class BatchingSimpleConsumerImpl implements SimpleConsumer {

    /**
     * Maximum number of messages to request in a single {@code receive} call.
     */
    static final int FETCH_BATCH_SIZE = 32;

    private static final Logger log = LoggerFactory.getLogger(BatchingSimpleConsumerImpl.class);

    private final SimpleConsumer delegate;
    private final BatchPolicy batchPolicy;
    private volatile boolean closed = false;

    private BatchingSimpleConsumerImpl(SimpleConsumer delegate, BatchPolicy batchPolicy) {
        checkNotNull(delegate, "delegate should not be null");
        checkNotNull(batchPolicy, "batchPolicy should not be null");
        this.delegate = delegate;
        this.batchPolicy = batchPolicy;
    }

    /**
     * Creates a new {@link BatchingSimpleConsumerImpl}.
     */
    public static BatchingSimpleConsumerImpl create(SimpleConsumer delegate, BatchPolicy batchPolicy) {
        return new BatchingSimpleConsumerImpl(delegate, batchPolicy);
    }

    // -------------------------------------------------------------------------
    // Batch API — without topic (round-robin across subscribed topics)
    // -------------------------------------------------------------------------

    @Override
    public List<MessageView> batchReceive(Duration invisibleDuration) throws ClientException {
        return awaitFuture(batchReceiveAsync(invisibleDuration));
    }

    @Override
    public CompletableFuture<List<MessageView>> batchReceiveAsync(Duration invisibleDuration) {
        checkNotNull(invisibleDuration, "invisibleDuration should not be null");
        return doBatchReceive(null, invisibleDuration);
    }

    // -------------------------------------------------------------------------
    // Batch API — with specific topic
    // -------------------------------------------------------------------------

    @Override
    public List<MessageView> batchReceive(String topic, Duration invisibleDuration)
        throws ClientException {
        return awaitFuture(batchReceiveAsync(topic, invisibleDuration));
    }

    @Override
    public CompletableFuture<List<MessageView>> batchReceiveAsync(String topic,
        Duration invisibleDuration) {
        checkNotNull(topic, "topic should not be null");
        checkNotNull(invisibleDuration, "invisibleDuration should not be null");
        return doBatchReceive(topic, invisibleDuration);
    }

    // -------------------------------------------------------------------------
    // Batch ack — delegate directly
    // -------------------------------------------------------------------------

    @Override
    public void batchAck(List<MessageView> messageViews) throws ClientException {
        delegate.batchAck(messageViews);
    }

    @Override
    public CompletableFuture<Void> batchAckAsync(List<MessageView> messageViews) {
        return delegate.batchAckAsync(messageViews);
    }

    // -------------------------------------------------------------------------
    // Receive — delegate directly (no batching layer)
    // -------------------------------------------------------------------------

    @Override
    public List<MessageView> receive(int maxMessageNum, Duration invisibleDuration)
        throws ClientException {
        return delegate.receive(maxMessageNum, invisibleDuration);
    }

    @Override
    public CompletableFuture<List<MessageView>> receiveAsync(int maxMessageNum,
        Duration invisibleDuration) {
        return delegate.receiveAsync(maxMessageNum, invisibleDuration);
    }

    @Override
    public List<MessageView> receive(String topic, int maxMessageNum, Duration invisibleDuration)
        throws ClientException {
        return delegate.receive(topic, maxMessageNum, invisibleDuration);
    }

    @Override
    public CompletableFuture<List<MessageView>> receiveAsync(String topic, int maxMessageNum,
        Duration invisibleDuration) {
        return delegate.receiveAsync(topic, maxMessageNum, invisibleDuration);
    }

    // -------------------------------------------------------------------------
    // Other delegate methods
    // -------------------------------------------------------------------------

    @Override
    public String getConsumerGroup() {
        return delegate.getConsumerGroup();
    }

    @Override
    public SimpleConsumer subscribe(String topic, FilterExpression filterExpression)
        throws ClientException {
        delegate.subscribe(topic, filterExpression);
        return this;
    }

    @Override
    public SimpleConsumer unsubscribe(String topic) throws ClientException {
        delegate.unsubscribe(topic);
        return this;
    }

    @Override
    public Map<String, FilterExpression> getSubscriptionExpressions() {
        return delegate.getSubscriptionExpressions();
    }

    @Override
    public void ack(MessageView messageView) throws ClientException {
        delegate.ack(messageView);
    }

    @Override
    public CompletableFuture<Void> ackAsync(MessageView messageView) {
        return delegate.ackAsync(messageView);
    }

    @Override
    public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration)
        throws ClientException {
        delegate.changeInvisibleDuration(messageView, invisibleDuration);
    }

    @Override
    public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration,
        boolean suspend) throws ClientException {
        delegate.changeInvisibleDuration(messageView, invisibleDuration, suspend);
    }

    @Override
    public CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView,
        Duration invisibleDuration) {
        return delegate.changeInvisibleDurationAsync(messageView, invisibleDuration);
    }

    @Override
    public CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView,
        Duration invisibleDuration, boolean suspend) {
        return delegate.changeInvisibleDurationAsync(messageView, invisibleDuration, suspend);
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws IOException {
        closed = true;
        delegate.close();
    }

    // -------------------------------------------------------------------------
    // Core async adaptive fetch
    // -------------------------------------------------------------------------

    /**
     * Starts the adaptive batch-receive loop, returning a future that completes when the
     * batch is ready.
     */
    private CompletableFuture<List<MessageView>> doBatchReceive(String topic,
        Duration invisibleDuration) {
        final Duration effectiveInvisible = invisibleDuration.plus(batchPolicy.getMaxWaitTime());
        final FetchContext ctx = new FetchContext(
            topic, effectiveInvisible,
            batchPolicy.getMaxBatchCount(),
            batchPolicy.getMaxBatchBytes(),
            batchPolicy.getMaxInflight(),
            batchPolicy.getMaxWaitTime().toNanos());
        return fetchRound(ctx);
    }

    /**
     * Executes one round of concurrent receives, collects results, and decides whether to
     * continue with the next round.
     *
     * <p>This method is fully asynchronous — it chains futures via
     * {@link CompletableFuture#thenCompose} and never blocks.  All fired receives are
     * awaited via {@code allOf} before proceeding; they are never cancelled.
     */
    private CompletableFuture<List<MessageView>> fetchRound(FetchContext ctx) {
        // Terminal conditions.
        if (closed || ctx.isSatisfied() || ctx.isDeadlineExpired()) {
            return CompletableFuture.completedFuture(ctx.result);
        }

        // --- Adaptive concurrency: split remaining into parallel receives ---
        final int concurrency = Math.min(
            (ctx.remaining + FETCH_BATCH_SIZE - 1) / FETCH_BATCH_SIZE,
            ctx.maxInflight);

        final List<CompletableFuture<List<MessageView>>> futures = new ArrayList<>(concurrency);
        int distributed = 0;
        for (int i = 0; i < concurrency; i++) {
            final int count = (ctx.remaining - distributed) / (concurrency - i);
            distributed += count;

            final CompletableFuture<List<MessageView>> f;
            if (ctx.topic != null) {
                f = delegate.receiveAsync(ctx.topic, count, ctx.effectiveInvisible);
            } else {
                f = delegate.receiveAsync(count, ctx.effectiveInvisible);
            }
            futures.add(f.exceptionally(e -> {
                log.warn("Receive failed, skipping", e);
                return Collections.emptyList();
            }));
        }

        // Wait for ALL futures to complete (never cancel), then collect and continue.
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenCompose(v -> {
                for (CompletableFuture<List<MessageView>> f : futures) {
                    for (MessageView msg : f.join()) {
                        ctx.result.add(msg);
                        ctx.accumulatedBytes += msg.getBody().remaining();
                        ctx.remaining--;
                        // Start the deadline clock on the first message.
                        if (!ctx.isDeadlineStarted()) {
                            ctx.deadlineNanos = System.nanoTime() + ctx.maxWaitNanos;
                        }
                    }
                }
                // Recurse into the next round.
                return fetchRound(ctx);
            });
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static List<MessageView> awaitFuture(CompletableFuture<List<MessageView>> future)
        throws ClientException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClientException("Interrupted while waiting for batch receive");
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof ClientException) {
                throw (ClientException) cause;
            }
            throw new ClientException("Batch receive failed", cause);
        }
    }

    // -------------------------------------------------------------------------
    // Fetch context (mutable state for a single batch-receive session)
    // -------------------------------------------------------------------------

    /**
     * Mutable context that tracks the progress of a single adaptive batch-receive session.
     * Only accessed from the {@code thenCompose} chain — never concurrently.
     */
    static class FetchContext {
        final String topic;
        final Duration effectiveInvisible;
        final long maxBytes;
        final int maxInflight;
        final long maxWaitNanos;
        final List<MessageView> result = new ArrayList<>();

        int remaining;
        long accumulatedBytes;
        /** Nanotime deadline. {@code 0} means the clock has not started yet. */
        long deadlineNanos;

        FetchContext(String topic, Duration effectiveInvisible,
            int maxCount, long maxBytes, int maxInflight, long maxWaitNanos) {
            this.topic = topic;
            this.effectiveInvisible = effectiveInvisible;
            this.maxBytes = maxBytes;
            this.maxInflight = maxInflight;
            this.maxWaitNanos = maxWaitNanos;
            this.remaining = maxCount;
        }

        boolean isDeadlineStarted() {
            return deadlineNanos != 0;
        }

        boolean isDeadlineExpired() {
            return isDeadlineStarted() && System.nanoTime() >= deadlineNanos;
        }

        boolean isSatisfied() {
            return remaining <= 0 || accumulatedBytes >= maxBytes;
        }
    }
}
