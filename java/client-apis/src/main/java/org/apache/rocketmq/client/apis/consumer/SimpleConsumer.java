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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;

/**
 * Simple consumer is a thread-safe rocketmq client which is used to consume messages by the consumer group.
 *
 * <p>Simple consumer is a lightweight consumer, if you want to fully control the message consumption operation by
 * yourself, the simple consumer should be your first consideration.
 *
 * <p>Similar to {@link PushConsumer}, consumers belong to the same consumer group share messages from the server, which
 * means they must have the same subscription expressions, otherwise the behavior is <strong>UNDEFINED</strong>.
 *
 * <p>In addition, the simple consumer can share a consumer group with the {@link PushConsumer}, at which time they
 * share the common consumption progress.
 *
 * <h3>Share consume progress with push consumer</h3>
 * <pre>
 * ┌──────────────────┐        ┌─────────────────┐
 * │consume progress 0│◄─┐  ┌─►│simple consumer A│
 * └──────────────────┘  │  │  └─────────────────┘
 *                       ├──┤
 *  ┌─────────────────┐  │  │  ┌───────────────┐
 *  │topic X + group 0│◄─┘  └─►│push consumer B│
 *  └─────────────────┘        └───────────────┘
 * </pre>
 *
 * <p>Simple consumer divide message consumption to 3 phases.
 * 1. Receive messages from the server.
 * 2. Executes your operations after receiving messages.
 * 3. Acknowledge the message or change its invisible duration before the next delivery according to the operation
 * result.
 */
public interface SimpleConsumer extends Closeable {
    /**
     * Get the load balancing group for the simple consumer.
     *
     * @return consumer load balancing group.
     */
    String getConsumerGroup();

    /**
     * Add subscription expression dynamically.
     *
     * <p>If the first subscription expression that contains topicA and tag1 has existed already in the consumer, then
     * second subscription expression which contains topicA and tag2, <strong>the result is that the second one
     * replaces the first one instead of integrating them</strong>.
     *
     * @param topic            new topic that needs to add or update.
     * @param filterExpression new filter expression to add or update.
     * @return simple consumer instance.
     */
    SimpleConsumer subscribe(String topic, FilterExpression filterExpression) throws ClientException;

    /**
     * Remove subscription expression dynamically by topic.
     *
     * <p>It stops the backend task to fetch messages from remote, and besides that, the locally cached message whose
     * topic
     * was removed before would not be delivered to {@link MessageListener} anymore.
     *
     * <p>Nothing occurs if the specified topic does not exist in subscription expressions of the push consumer.
     *
     * @param topic the topic to remove the subscription.
     * @return simple consumer instance.
     */
    SimpleConsumer unsubscribe(String topic) throws ClientException;

    /**
     * List the existed subscription expressions in simple consumer.
     *
     * @return map of topics to filter expression.
     */
    Map<String, FilterExpression> getSubscriptionExpressions();

    /**
     * Fetch messages from the server synchronously.
     * <p> This method returns immediately if there are messages available.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty map will be returned.
     *
     * @param maxMessageNum     max message num of server returned.
     * @param invisibleDuration set the invisibleDuration of messages to return from the server. These messages will be
     *                          invisible to other consumers unless timeout.
     * @return list of message view.
     */
    List<MessageView> receive(int maxMessageNum, Duration invisibleDuration) throws ClientException;

    /**
     * Fetch messages from the server asynchronously.
     * <p> This method returns immediately if there are messages available.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty map will be returned.
     *
     * @param maxMessageNum     max message num of server returned.
     * @param invisibleDuration set the invisibleDuration of messages to return from the server. These messages will be
     *                          invisible to other consumers unless timeout.
     * @return list of message view.
     */
    CompletableFuture<List<MessageView>> receiveAsync(int maxMessageNum, Duration invisibleDuration);

    /**
     * Ack message to server synchronously, server commit this message.
     *
     * <p>Duplicate ack request does not take effect and throw an exception.
     *
     * @param messageView special message view with handle want to ack.
     */
    void ack(MessageView messageView) throws ClientException;

    /**
     * Ack message to the server asynchronously, server commit this message.
     *
     * <p> Duplicate ack request does not take effect and throw an exception.
     *
     * @param messageView special message view with handle want to ack.
     * @return CompletableFuture of this request.
     */
    CompletableFuture<Void> ackAsync(MessageView messageView);

    /**
     * Changes the invisible duration of a specified message synchronously.
     *
     * <p> The origin invisible duration for a message decide by ack request.
     *
     * <p>Duplicate change requests will refresh the next visible time of this message to consumers.
     *
     * @param messageView       the message view to change invisible time.
     * @param invisibleDuration new timestamp the message could be visible and re-consume which start from current time.
     */
    void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration) throws ClientException;

    /**
     * Changes the invisible duration of a specified message asynchronously.
     *
     * <p> The origin invisible duration for a message decide by ack request.
     *
     * <p>Duplicate change requests will refresh the next visible time of this message to consumers.
     *
     * @param messageView       the message view to change invisible time.
     * @param invisibleDuration new timestamp the message could be visible and re-consume which start from current time.
     * @return CompletableFuture of this request.
     */
    CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView, Duration invisibleDuration);

    /**
     * Close the simple consumer and release all related resources.
     *
     * <p>Once simple consumer is closed, <strong>it could not be started once again.</strong> we maintained an FSM
     * (finite-state machine) to record the different states for each simple consumer.
     */
    @Override
    void close() throws IOException;
}
