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

import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.AckMessageResultEntry;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Status;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accumulates acknowledgements produced by one push consumer. A request can contain messages for only one topic and
 * one endpoint, so each client-local batch is partitioned by these two fields.
 */
class AckMessageBatcher {
    static final int MAX_BATCH_SIZE = 1024;
    static final Duration MAX_BATCH_DELAY = Duration.ofSeconds(5);
    static final Duration MAX_BATCHABLE_DURATION = Duration.ofSeconds(25);

    private static final Logger log = LoggerFactory.getLogger(AckMessageBatcher.class);
    private static final Status MISSING_RESULT_STATUS = Status.newBuilder()
        .setCode(Code.INTERNAL_ERROR)
        .setMessage("Batch acknowledgement response does not contain the message result")
        .build();

    private final PushConsumerImpl consumer;
    private final ScheduledExecutorService scheduler;
    private final int maxBatchSize;
    private final Duration maxBatchDelay;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<BatchKey, Batch> batches = new LinkedHashMap<>();
    @GuardedBy("lock")
    private final Set<Batch> inflightBatches = new HashSet<>();
    @GuardedBy("lock")
    private boolean closed;

    AckMessageBatcher(PushConsumerImpl consumer, ScheduledExecutorService scheduler) {
        this(consumer, scheduler, MAX_BATCH_SIZE, MAX_BATCH_DELAY);
    }

    AckMessageBatcher(PushConsumerImpl consumer, ScheduledExecutorService scheduler, int maxBatchSize,
        Duration maxBatchDelay) {
        this.consumer = consumer;
        this.scheduler = scheduler;
        this.maxBatchSize = maxBatchSize;
        this.maxBatchDelay = maxBatchDelay;
    }

    boolean isBatchable(MessageViewImpl messageView) {
        if (consumer.isLiteConsumer()) {
            return false;
        }
        final long elapsedMillis = System.currentTimeMillis() - messageView.getDecodeTimestamp();
        return elapsedMillis >= 0 && elapsedMillis <= MAX_BATCHABLE_DURATION.toMillis();
    }

    /**
     * @return a per-message response future, or {@code null} if the batcher is already closed.
     */
    ListenableFuture<AckMessageResponse> enqueue(MessageViewImpl messageView) {
        final PendingAck pendingAck = new PendingAck(messageView);
        Batch batchToFlush = null;
        synchronized (lock) {
            if (closed) {
                return null;
            }
            final BatchKey key = new BatchKey(messageView.getEndpoints(), messageView.getTopic());
            Batch batch = batches.get(key);
            if (null == batch) {
                batch = new Batch(key);
                try {
                    final Batch newBatch = batch;
                    batch.flushFuture = scheduler.schedule(() -> flush(newBatch), maxBatchDelay.toNanos(),
                        TimeUnit.NANOSECONDS);
                } catch (Throwable t) {
                    log.warn("Failed to schedule batch acknowledgement, topic={}, endpoints={}",
                        messageView.getTopic(), messageView.getEndpoints(), t);
                    return null;
                }
                batches.put(key, batch);
            }
            batch.pendingAcks.add(pendingAck);
            if (batch.pendingAcks.size() >= maxBatchSize) {
                batches.remove(key);
                batch.flushFuture.cancel(false);
                inflightBatches.add(batch);
                batchToFlush = batch;
            }
        }
        if (null != batchToFlush) {
            send(batchToFlush);
        }
        return pendingAck.responseFuture;
    }

    private void flush(Batch batch) {
        synchronized (lock) {
            if (batches.get(batch.key) != batch) {
                return;
            }
            batches.remove(batch.key);
            inflightBatches.add(batch);
        }
        send(batch);
    }

    ListenableFuture<List<AckMessageResponse>> flushAndClose() {
        final List<Batch> batchesToFlush;
        final List<ListenableFuture<AckMessageResponse>> responseFuturesSnapshot;
        synchronized (lock) {
            closed = true;
            batchesToFlush = new ArrayList<>(batches.values());
            batches.clear();
            for (Batch batch : batchesToFlush) {
                batch.flushFuture.cancel(false);
                inflightBatches.add(batch);
            }
            responseFuturesSnapshot = new ArrayList<>();
            for (Batch batch : inflightBatches) {
                for (PendingAck pendingAck : batch.pendingAcks) {
                    responseFuturesSnapshot.add(pendingAck.responseFuture);
                }
            }
        }
        for (Batch batch : batchesToFlush) {
            send(batch);
        }
        return Futures.allAsList(responseFuturesSnapshot);
    }

    private void send(Batch batch) {
        final List<MessageViewImpl> messageViews = new ArrayList<>(batch.pendingAcks.size());
        for (PendingAck pendingAck : batch.pendingAcks) {
            messageViews.add(pendingAck.messageView);
        }
        final RpcFuture<apache.rocketmq.v2.AckMessageRequest, AckMessageResponse> responseFuture =
            consumer.ackMessage(messageViews);
        Futures.addCallback(responseFuture, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                complete(batch.pendingAcks, response);
                onBatchCompleted(batch);
            }

            @Override
            public void onFailure(Throwable t) {
                for (PendingAck pendingAck : batch.pendingAcks) {
                    pendingAck.responseFuture.setException(t);
                }
                onBatchCompleted(batch);
            }
        }, MoreExecutors.directExecutor());
    }

    private void onBatchCompleted(Batch batch) {
        synchronized (lock) {
            inflightBatches.remove(batch);
        }
    }

    private void complete(List<PendingAck> pendingAcks, AckMessageResponse response) {
        if (response.getEntriesCount() == 0) {
            final Status status = Code.OK.equals(response.getStatus().getCode())
                ? MISSING_RESULT_STATUS : response.getStatus();
            for (PendingAck pendingAck : pendingAcks) {
                pendingAck.responseFuture.set(responseFor(pendingAck.messageView, status));
            }
            return;
        }

        final Map<String, Deque<AckMessageResultEntry>> resultTable = new HashMap<>();
        for (AckMessageResultEntry entry : response.getEntriesList()) {
            resultTable.computeIfAbsent(entry.getMessageId(), ignored -> new ArrayDeque<>()).add(entry);
        }
        for (PendingAck pendingAck : pendingAcks) {
            final String messageId = pendingAck.messageView.getMessageId().toString();
            final Deque<AckMessageResultEntry> results = resultTable.get(messageId);
            final AckMessageResultEntry entry = null == results ? null : results.pollFirst();
            if (null == entry) {
                pendingAck.responseFuture.set(responseFor(pendingAck.messageView, MISSING_RESULT_STATUS));
                continue;
            }
            pendingAck.responseFuture.set(AckMessageResponse.newBuilder()
                .setStatus(entry.getStatus())
                .addEntries(entry)
                .build());
        }
        final int extraResultCount = resultTable.values().stream().mapToInt(Collection::size).sum();
        if (extraResultCount > 0) {
            log.warn("Batch acknowledgement response contains {} unmatched entries, topic={}, endpoints={}",
                extraResultCount, pendingAcks.get(0).messageView.getTopic(),
                pendingAcks.get(0).messageView.getEndpoints());
        }
    }

    private AckMessageResponse responseFor(MessageViewImpl messageView, Status status) {
        final AckMessageResultEntry entry = AckMessageResultEntry.newBuilder()
            .setMessageId(messageView.getMessageId().toString())
            .setReceiptHandle(messageView.getReceiptHandle())
            .setStatus(status)
            .build();
        return AckMessageResponse.newBuilder().setStatus(status).addEntries(entry).build();
    }

    private static class PendingAck {
        private final MessageViewImpl messageView;
        private final SettableFuture<AckMessageResponse> responseFuture = SettableFuture.create();

        private PendingAck(MessageViewImpl messageView) {
            this.messageView = messageView;
        }
    }

    private static class Batch {
        private final BatchKey key;
        private final List<PendingAck> pendingAcks = new ArrayList<>();
        private ScheduledFuture<?> flushFuture;

        private Batch(BatchKey key) {
            this.key = key;
        }
    }

    private static class BatchKey {
        private final Endpoints endpoints;
        private final String topic;

        private BatchKey(Endpoints endpoints, String topic) {
            this.endpoints = endpoints;
            this.topic = topic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BatchKey)) {
                return false;
            }
            BatchKey batchKey = (BatchKey) o;
            return Objects.equals(endpoints, batchKey.endpoints) && Objects.equals(topic, batchKey.topic);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hashCode(endpoints) + Objects.hashCode(topic);
        }
    }
}
