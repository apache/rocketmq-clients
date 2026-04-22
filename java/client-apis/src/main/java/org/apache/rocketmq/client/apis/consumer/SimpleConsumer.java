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
     * <p>Nothing occurs if the specified topic does not exist in subscription expressions of the consumer.
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
     * Fetch messages from a <strong>specific topic</strong> synchronously.
     *
     * <p>Unlike {@link #receive(int, Duration)} which round-robins across all subscribed topics (and may
     * be blocked by a topic with no messages), this method targets a single topic so that the caller
     * has full control over which topic to poll.
     *
     * @param topic             the topic to receive messages from; must be subscribed.
     * @param maxMessageNum     max message num of server returned.
     * @param invisibleDuration set the invisibleDuration of messages to return from the server.
     * @return list of message view.
     * @throws IllegalArgumentException if the topic is not subscribed.
     */
    List<MessageView> receive(String topic, int maxMessageNum, Duration invisibleDuration) throws ClientException;

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
     * Fetch messages from a <strong>specific topic</strong> asynchronously.
     *
     * <p>Same semantics as {@link #receive(String, int, Duration)} but returns a
     * {@link CompletableFuture} immediately.
     *
     * @param topic             the topic to receive messages from; must be subscribed.
     * @param maxMessageNum     max message num of server returned.
     * @param invisibleDuration set the invisibleDuration of messages to return from the server.
     * @return list of message view.
     * @throws IllegalArgumentException if the topic is not subscribed.
     */
    CompletableFuture<List<MessageView>> receiveAsync(String topic, int maxMessageNum, Duration invisibleDuration);

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
     * Changes the invisible duration of a specified message synchronously, with an option to
     * suspend retry-count increment on the server.
     *
     * <p>When {@code suspend} is {@code true}, the server will <em>not</em> increment the
     * delivery attempt counter for this message.  This is useful for messages that are released
     * back to the server due to client-side cache eviction rather than a genuine processing failure.
     *
     * @param messageView       the message view to change invisible time.
     * @param invisibleDuration new timestamp the message could be visible and re-consume which start from current time.
     * @param suspend           if {@code true}, the server will not increment the retry times.
     */
    void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration, boolean suspend)
        throws ClientException;

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
     * Asynchronous variant of {@link #changeInvisibleDuration(MessageView, Duration, boolean)}.
     *
     * @param messageView       the message view to change invisible time.
     * @param invisibleDuration new timestamp the message could be visible and re-consume which start from current time.
     * @param suspend           if {@code true}, the server will not increment the retry times.
     * @return CompletableFuture of this request.
     */
    CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView, Duration invisibleDuration,
        boolean suspend);

    /**
     * Batch-fetch messages synchronously by issuing adaptive concurrent {@code receive} calls.
     *
     * <p>This method blocks until either {@link BatchPolicy#getMaxBatchCount()} messages have been
     * collected, the total body size reaches {@link BatchPolicy#getMaxBatchBytes()}, or
     * {@link BatchPolicy#getMaxWaitTime()} has elapsed since the first message was received.
     *
     * <p>A {@link BatchPolicy} must have been set via
     * {@link SimpleConsumerBuilder#setBatchPolicy(BatchPolicy)} before calling this method; otherwise an
     * {@link UnsupportedOperationException} is thrown.
     *
     * @param invisibleDuration the invisible duration assigned to each message on the server side.
     *                          Note that the <em>effective</em> invisible window on the server is
     *                          {@code invisibleDuration + batchPolicy.maxWaitTime}.
     * @return a list of messages (size &le; {@link BatchPolicy#getMaxBatchCount()}).
     * @throws ClientException          if an unrecoverable error occurs.
     * @throws UnsupportedOperationException if no {@link BatchPolicy} has been configured.
     */
    default List<MessageView> batchReceive(Duration invisibleDuration) throws ClientException {
        throw new UnsupportedOperationException("batchReceive is not supported without a BatchPolicy. "
            + "Call SimpleConsumerBuilder#setBatchPolicy first.");
    }

    /**
     * Batch-fetch messages from a <strong>specific topic</strong> synchronously.
     *
     * <p>Same semantics as {@link #batchReceive(Duration)} but targets a single topic,
     * giving the caller precise control over which topic to poll.
     *
     * @param topic             the topic to receive messages from; must be subscribed.
     * @param invisibleDuration the invisible duration assigned to each message on the server side.
     * @return a list of messages (size &le; {@link BatchPolicy#getMaxBatchCount()}).
     * @throws ClientException          if an unrecoverable error occurs.
     * @throws UnsupportedOperationException if no {@link BatchPolicy} has been configured.
     */
    default List<MessageView> batchReceive(String topic, Duration invisibleDuration) throws ClientException {
        throw new UnsupportedOperationException("batchReceive is not supported without a BatchPolicy. "
            + "Call SimpleConsumerBuilder#setBatchPolicy first.");
    }

    /**
     * Batch-fetch messages asynchronously by issuing adaptive concurrent {@code receive} calls.
     *
     * <p>Same semantics as {@link #batchReceive(Duration)} but returns a {@link CompletableFuture}
     * immediately.  Each call is independent; there is no internal queue or background thread.
     *
     * @param invisibleDuration the invisible duration assigned to each message on the server side.
     * @return a {@link CompletableFuture} that completes with a list of messages.
     * @throws UnsupportedOperationException if no {@link BatchPolicy} has been configured.
     */
    default CompletableFuture<List<MessageView>> batchReceiveAsync(Duration invisibleDuration) {
        throw new UnsupportedOperationException("batchReceiveAsync is not supported without a BatchPolicy. "
            + "Call SimpleConsumerBuilder#setBatchPolicy first.");
    }

    /**
     * Batch-fetch messages from a <strong>specific topic</strong> asynchronously.
     *
     * <p>Same semantics as {@link #batchReceive(String, Duration)} but returns a
     * {@link CompletableFuture} immediately.
     *
     * @param topic             the topic to receive messages from; must be subscribed.
     * @param invisibleDuration the invisible duration assigned to each message on the server side.
     * @return a {@link CompletableFuture} that completes with a list of messages.
     * @throws UnsupportedOperationException if no {@link BatchPolicy} has been configured.
     */
    default CompletableFuture<List<MessageView>> batchReceiveAsync(String topic, Duration invisibleDuration) {
        throw new UnsupportedOperationException("batchReceiveAsync is not supported without a BatchPolicy. "
            + "Call SimpleConsumerBuilder#setBatchPolicy first.");
    }

    /**
     * Acknowledges a list of messages in batch.
     *
     * <p>All entries are grouped by (topic, broker) and each group is further split into chunks
     * of a bounded size to prevent oversized RPC payloads.  Each chunk is sent as a single
     * {@code AckMessageRequest}, which is significantly more efficient than calling
     * {@link #ack(MessageView)} in a loop.
     *
     * @param messageViews the messages to acknowledge; must not be {@code null}.
     * @throws ClientException if an unrecoverable error occurs.
     */
    default void batchAck(List<MessageView> messageViews) throws ClientException {
        throw new UnsupportedOperationException("batchAck is not supported without a BatchPolicy. "
            + "Call SimpleConsumerBuilder#setBatchPolicy first.");
    }

    /**
     * Asynchronous variant of {@link #batchAck(List)}.
     *
     * @param messageViews the messages to acknowledge; must not be {@code null}.
     * @return a {@link CompletableFuture} that completes when all ack RPCs have finished.
     */
    default CompletableFuture<Void> batchAckAsync(List<MessageView> messageViews) {
        throw new UnsupportedOperationException("batchAckAsync is not supported without a BatchPolicy. "
            + "Call SimpleConsumerBuilder#setBatchPolicy first.");
    }

    /**
     * Close the simple consumer and release all related resources.
     *
     * <p>Once simple consumer is closed, <strong>it could not be started once again.</strong> we maintained an FSM
     * (finite-state machine) to record the different states for each simple consumer.
     */
    @Override
    void close() throws IOException;
}
