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

package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;

/**
 * This class is a cache of messages in responding {@link MessageQueue} regardless of {@link MessageModel}.
 *
 * <p>Serve for The {@link ConsumeService}, which only in charge of take and erase message(s) from here. Each message
 * taken from {@link ProcessQueue} must be erased with {@link ConsumeStatus}. There are different methods of
 * take/erase for FIFO and other messages.
 *
 * <p>'take' means message(s) has been delivered to {@link MessageListener}, but is still cached until 'erase'.
 */
@ThreadSafe
public interface ProcessQueue {
    /**
     * Get the message queue bound.
     *
     * @return bound message queue.
     */
    MessageQueue getMessageQueue();

    /**
     * Drop current process queue, it would not pull/receive message any more if dropped.
     */
    void drop();

    /**
     * It would be regarded as expired if no pull/receive message for a long time.
     *
     * @return if it is expired.
     */
    boolean expired();

    /**
     * Start to receive message immediately.
     */
    void receiveMessageImmediately();

    /**
     * Start to pull message immediately.
     */
    void pullMessageImmediately();

    /**
     * Try to take messages from cache except FIFO messages.
     *
     * @param batchMaxSize max batch size to take messages.
     * @return messages which have been taken.
     */
    List<MessageExt> tryTakeMessages(int batchMaxSize);

    /**
     * Erase messages which haven been taken except FIFO messages.
     *
     * @param messageExtList messages to erase.
     * @param status         consume status.
     */
    void eraseMessages(List<MessageExt> messageExtList, ConsumeStatus status);

    /**
     * Try to take FIFO message from cache.
     *
     * @return message which has been taken, or null if no message.
     */
    MessageExt tryTakeFifoMessage();

    /**
     * Erase FIFO message which has been taken.
     *
     * @param messageExt message to erase.
     * @param status     consume status.
     */
    void eraseFifoMessage(MessageExt messageExt, ConsumeStatus status);
}
