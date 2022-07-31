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
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;

/**
 * Process queue is a cache to store fetched messages from remote for {@link PushConsumer}, and it provides specialized
 * function to take the message to consume for push consumer.
 *
 * <p>{@link PushConsumer} queries assignments periodically and converts them into message queues, each message queue is
 * mapped into one process queue to fetch message from remote. If the message queue is removed from the newest
 * assignment, the corresponding process queue is marked as expired soon, which makes it deprecated, which means its
 * lifecycle is over.
 *
 * <h3>A standard procedure to take/erase message</h3>
 *
 * <p>
 * phase 1: Fetch 32 messages successfully from remote.
 * <pre>
 * 32 in   ┌─────────────────────────┐      ┌───┐
 * ────────►           32            │      │ 0 │
 *         └─────────────────────────┘      └───┘
 *               pending messages     in-flight messages
 * </pre>
 * phase 2: {@link #tryTakeMessage} with 1 messages and prepare to consume it.
 * <pre>
 *        ┌─────────────────────┐  1   ┌─────┐
 *        │         31          ├──────►  1  │
 *        └─────────────────────┘      └─────┘
 *            pending messages      in-flight messages
 * </pre>
 * phase 3: {@link #eraseMessage(MessageViewImpl, ConsumeResult)} with 1 messages and its consume result.
 * <pre>
 *        ┌─────────────────────┐      ┌───┐ 1 out
 *        │         31          │      │ 0 ├──────►
 *        └─────────────────────┘      └───┘
 *           pending messages      in-flight messages
 * </pre>
 *
 * <p>Especially, there are some different processing procedures for FIFO consumption.
 *
 * <p>Let us emphasize two points:
 * 1. For the push consumer, the order of FIFO messages means: for messages under the same message group, only when the
 * previous message is successfully consumed and successfully acknowledged or successfully sent to the dead-letter queue
 * if the consumption is failed, the latter message can be delivered into the {@link MessageListener}.
 * 2. The push consumer essentially uses the 'pull' to simulate the behavior of 'push', and the consumer pulls messages
 * in batches.
 *
 * <p>Based on the above assumptions, the server may put multiple messages of the same message group orderly in a
 * batch of message acquisition, but until the above messages are successfully acknowledged or sent to the dead letter
 * queue, the subsequent messages of the same message group will be obtained by the client. The above is enforced by
 * the server, this is why the return value of the {@link #tryTakeFifoMessages()} is an iterator, an iterator represents
 * a batch, the client itself must ensure the order of message consumption within the batch, and the order between
 * batches are guaranteed by the server.
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
     * Try to take messages from cache except FIFO messages.
     *
     * @return messages which have been taken.
     */
    Optional<MessageViewImpl> tryTakeMessage();

    /**
     * Erase messages which have been taken except FIFO messages.
     *
     * @param messageView   the message to erase.
     * @param consumeResult consume result.
     */
    void eraseMessage(MessageViewImpl messageView, ConsumeResult consumeResult);

    /**
     * Try to take a FIFO message from the cache.
     *
     * @return message which has been taken, or {@link Collections#emptyIterator()} if no message.
     */
    Iterator<MessageViewImpl> tryTakeFifoMessages();

    /**
     * Erase FIFO message which has been taken.
     *
     * @param messageView   the message to erase.
     * @param consumeResult consume status.
     */
    ListenableFuture<Void> eraseFifoMessage(MessageViewImpl messageView, ConsumeResult consumeResult);

    /**
     * Get the count of pending messages.
     *
     * @return count of pending messages.
     */
    long getPendingMessageCount();

    /**
     * Get the count of in-flight messages.
     *
     * @return count of in-flight messages.
     */
    long getInflightMessageCount();

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
