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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;

/**
 * Process queue is a cache to store fetched messages from remote for {@link PushConsumer}.
 *
 * <p>{@link PushConsumer} queries assignments periodically and converts them into message queues, each message queue is
 * mapped into one process queue to fetch message from remote. If the message queue is removed from the newest
 * assignment, the corresponding process queue is marked as expired soon, which means its lifecycle is over.
 *
 * <h3>A standard procedure to cache/erase message</h3>
 *
 *
 * <p>
 * phase 1: Fetch 32 messages successfully from remote.
 * <pre>
 *  32 in ┌─────────────────────────┐
 * ───────►           32            │
 *        └─────────────────────────┘
 *             cached messages = 32
 * </pre>
 * phase 2: consuming 1 message.
 * <pre>
 *        ┌─────────────────────┐   ┌───┐
 *        │          31         ├───► 1 │ consuming
 *        └─────────────────────┘   └───┘
 *             cached messages = 32
 * </pre>
 * phase 3: {@link #eraseMessage(MessageViewImpl, ConsumeResult)} with 1 messages and its consume result.
 * <pre>
 *        ┌─────────────────────┐   ┌───┐ 1 consumed
 *        │          31         ├───► 0 ├───────────►
 *        └─────────────────────┘   └───┘
 *            cached messages = 31
 * </pre>
 *
 * <p>Especially, there are some different processing procedures for FIFO consumption. The server ensures that the
 * next batch of messages will not be obtained by the client until the previous batch of messages is confirmed to be
 * consumed successfully or not. In detail, the server confirms the success of consumption by message being
 * successfully acknowledged, and confirms the consumption failure by being successfully forwarding to the dead
 * letter queue, thus the client should try to ensure it succeeded in acknowledgement or forwarding to the dead
 * letter queue as possible.
 *
 * <p>Considering the different workflow of FIFO consumption, {@link #eraseFifoMessage(MessageViewImpl, ConsumeResult)}
 * and {@link #discardFifoMessage(MessageViewImpl)} is provided.
 */
public interface ProcessQueue {
    /**
     * Get the mapped message queue.
     *
     * @return mapped message queue.
     */
    MessageQueueImpl getMessageQueue();

    /**
     * Drop the current process queue, which means the process queue's lifecycle is over,
     * thus it would not fetch messages from the remote anymore if dropped.
     */
    void drop();

    /**
     * {@link ProcessQueue} would be regarded as expired if no fetch message for a long time.
     *
     * @return if it is expired.
     */
    boolean expired();

    /**
     * Start to fetch messages from remote immediately.
     */
    void fetchMessageImmediately();

    /**
     * Erase messages(Non-FIFO-consume-mode) which have been consumed properly.
     *
     * @param messageView   the message to erase.
     * @param consumeResult consume result.
     */
    void eraseMessage(MessageViewImpl messageView, ConsumeResult consumeResult);

    /**
     * Erase message(FIFO-consume-mode) which have been consumed properly.
     *
     * @param messageView   the message to erase.
     * @param consumeResult consume status.
     */
    ListenableFuture<Void> eraseFifoMessage(MessageViewImpl messageView, ConsumeResult consumeResult);

    /**
     * Discard the message(Non-FIFO-consume-mode) which could not be consumed properly.
     *
     * @param messageView the message to discard.
     */
    void discardMessage(MessageViewImpl messageView);

    /**
     * Discard the message(FIFO-consume-mode) which could not consumed properly.
     *
     * @param messageView the FIFO message to discard.
     */
    void discardFifoMessage(MessageViewImpl messageView);

    /**
     * Get the count of cached messages.
     *
     * @return count of pending messages.
     */
    long getCachedMessageCount();

    /**
     * Get the bytes of cached message memory footprint.
     *
     * @return bytes of cached message memory footprint.
     */
    long getCachedMessageBytes();

    /**
     * Do some stats work.
     */
    void doStats();
}
