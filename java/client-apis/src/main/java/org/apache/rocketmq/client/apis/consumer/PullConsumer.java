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

package org.apache.rocketmq.client.apis.consumer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageQueue;
import org.apache.rocketmq.client.apis.message.MessageView;

public interface PullConsumer extends Closeable {
    /**
     * Get the consumer group of the consumer.
     */
    String getConsumerGroup();

    /**
     * @param topic    the topic that needs to be monitored.
     * @param listener the callback to detect the message queue changes.
     */
    void registerMessageQueueChangeListenerByTopic(String topic, TopicMessageQueueChangeListener listener);

    /**
     * Fetch message queues of the topic.
     */
    Collection<MessageQueue> fetchMessageQueues(String topic) throws ClientException;

    /**
     * Manually assign a list of message queues to this consumer.
     *
     * <p>This interface does not allow for incremental assignment and will replace the previous assignment (if
     * previous assignment existed).
     *
     * @param messageQueues the list of message queues that are to be assigned to this consumer.
     */
    void assign(Collection<MessageQueue> messageQueues);

    /**
     * Fetch messages from assigned message queues specified by {@link #assign(Collection)}.
     *
     * @param timeout the maximum time to block.
     * @return list of fetched messages.
     */
    List<MessageView> poll(Duration timeout);

    /**
     * Overrides the fetch offsets that the consumer will use on the next poll. If this method is invoked for the same
     * message queue more than once, the latest offset will be used on the next {@link #poll(Duration)}.
     *
     * @param messageQueue the message queue to override the fetch offset.
     * @param offset       message offset.
     */
    void seek(MessageQueue messageQueue, long offset);

    /**
     * Suspending message pulling from the message queues.
     *
     * @param messageQueues message queues that need to be suspended.
     */
    void pause(Collection<MessageQueue> messageQueues);

    /**
     * Resuming message pulling from the message queues.
     *
     * @param messageQueues message queues that need to be resumed.
     */
    void resume(Collection<MessageQueue> messageQueues);

    /**
     * Look up the offsets for the given message queue by timestamp. The returned offset for each message queue is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding message
     * queue.
     *
     * @param messageQueue message queue that needs to be looked up.
     * @param timestamp    the timestamp for which to search.
     * @return the offset of the message queue, or {@link Optional#empty()} if there is no message.
     */
    Optional<Long> offsetForTimestamp(MessageQueue messageQueue, Long timestamp);

    /**
     * Get the latest committed offset for the given message queue.
     *
     * @return the latest committed offset, or {@link Optional#empty()} if there was no prior commit.
     */
    Optional<Long> committed(MessageQueue messageQueue);

    /**
     * Commit offset manually.
     */
    void commit() throws ClientException;

    /**
     * Overrides the fetch offsets with the beginning offset that the consumer will use on the next poll. If this
     * method is invoked for the same message queue more than once, the latest offset will be used on the next
     * {@link #poll(Duration)}.
     *
     * @param messageQueue the message queue to seek.
     */
    void seekToBegin(MessageQueue messageQueue) throws ClientException;

    /**
     * Overrides the fetch offsets with the end offset that the consumer will use on the next poll. If this method is
     * invoked for the same message queue more than once, the latest offset will be used on the next
     * {@link #poll(Duration)}.
     *
     * @param messageQueue the message queue to seek.
     */
    void seekToEnd(MessageQueue messageQueue) throws ClientException;

    /**
     * Close the pull consumer and release all related resources.
     */
    @Override
    void close() throws IOException;
}
