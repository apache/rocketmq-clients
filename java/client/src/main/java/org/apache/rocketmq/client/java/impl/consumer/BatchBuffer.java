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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.client.java.message.MessageViewImpl;

/**
 * Single consumer-level accumulation buffer that holds messages until a batch is ready.
 *
 * <p>All mutable state is encapsulated; callers interact through well-defined methods
 * and must hold the buffer lock ({@link #lock()} / {@link #unlock()}) when accessing
 * any mutating operation.
 */
class BatchBuffer {
    private final ReentrantLock lock = new ReentrantLock();
    private final List<BufferedMessage> messages = new ArrayList<>();
    private long accumulatedBytes;
    private long firstMessageTimeMillis = -1;
    private ScheduledFuture<?> timeoutFuture;

    // ======================== lock ========================

    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }

    // ======================== mutators ========================

    /**
     * Adds a message to the buffer.
     *
     * @return {@code true} if this is the first message in the current accumulation window,
     *         signaling the caller to schedule a timeout flush.
     */
    boolean addMessage(ProcessQueue pq, MessageViewImpl messageView) {
        messages.add(new BufferedMessage(pq, messageView));
        accumulatedBytes += messageView.getBody().remaining();
        if (firstMessageTimeMillis < 0) {
            firstMessageTimeMillis = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    /**
     * Takes at most one batch worth of messages from the head of the buffer.
     *
     * <p>The method guarantees forward progress: at least one message is taken per call even if
     * a single message exceeds {@code maxBatchBytes}.
     *
     * <p>If the buffer becomes empty after taking, the accumulation window is reset and any
     * pending timeout flush is cancelled automatically.
     */
    List<BufferedMessage> takeBatch(int maxBatchCount, long maxBatchBytes) {
        final List<BufferedMessage> batch = new ArrayList<>();
        long batchBytes = 0;

        while (!this.isEmpty() && batch.size() < maxBatchCount) {
            final BufferedMessage head = messages.get(0);
            final long headBytes = head.getMessageView().getBody().remaining();
            // Always take at least one message to guarantee progress.
            if (!batch.isEmpty() && batchBytes + headBytes > maxBatchBytes) {
                break;
            }
            messages.remove(0);
            batch.add(head);
            batchBytes += headBytes;
        }

        accumulatedBytes -= batchBytes;
        if (this.isEmpty()) {
            firstMessageTimeMillis = -1;
            cancelTimeoutFlush();
        }
        return batch;
    }

    void setTimeoutFuture(ScheduledFuture<?> future) {
        this.timeoutFuture = future;
    }

    // ======================== queries ========================

    boolean isEmpty() {
        return messages.isEmpty();
    }

    /**
     * Returns {@code true} if the buffer has enough messages to form a batch based on
     * the given thresholds.
     */
    boolean isBatchReady(int maxBatchCount, long maxBatchBytes) {
        if (messages.isEmpty()) {
            return false;
        }
        return messages.size() >= maxBatchCount || accumulatedBytes >= maxBatchBytes;
    }

    // ======================== internal ========================

    private void cancelTimeoutFlush() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
            timeoutFuture = null;
        }
    }

    // ======================== inner class ========================

    /**
     * Associates a {@link MessageViewImpl} with its originating {@link ProcessQueue} so that
     * after batch consumption each message can be erased from the correct queue.
     */
    static class BufferedMessage {
        private final ProcessQueue processQueue;
        private final MessageViewImpl messageView;

        BufferedMessage(ProcessQueue processQueue, MessageViewImpl messageView) {
            this.processQueue = processQueue;
            this.messageView = messageView;
        }

        ProcessQueue getProcessQueue() {
            return processQueue;
        }

        MessageViewImpl getMessageView() {
            return messageView;
        }
    }
}
