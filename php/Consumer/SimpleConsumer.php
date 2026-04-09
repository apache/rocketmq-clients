<?php
/**
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

namespace Apache\Rocketmq\Consumer;

use Apache\Rocketmq\Exception\ClientException;
use Apache\Rocketmq\Message\MessageView;

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
interface SimpleConsumer {
    /**
     * Get the load balancing group for the simple consumer.
     *
     * @return string consumer load balancing group.
     */
    public function getConsumerGroup(): string;

    /**
     * Add subscription expression dynamically.
     *
     * <p>If the first subscription expression that contains topicA and tag1 has existed already in the consumer, then
     * second subscription expression which contains topicA and tag2, <strong>the result is that the second one
     * replaces the first one instead of integrating them</strong>.
     *
     * @param string $topic new topic that needs to add or update.
     * @param FilterExpression $filterExpression new filter expression to add or update.
     * @return SimpleConsumer simple consumer instance.
     * @throws ClientException If an error occurs
     */
    public function subscribe(string $topic, FilterExpression $filterExpression): SimpleConsumer;

    /**
     * Remove subscription expression dynamically by topic.
     *
     * <p>It stops the backend task to fetch messages from remote, and besides that, the locally cached message whose
     * topic
     * was removed before would not be delivered to {@link MessageListener} anymore.
     *
     * <p>Nothing occurs if the specified topic does not exist in subscription expressions of the push consumer.
     *
     * @param string $topic the topic to remove the subscription.
     * @return SimpleConsumer simple consumer instance.
     * @throws ClientException If an error occurs
     */
    public function unsubscribe(string $topic): SimpleConsumer;

    /**
     * List the existed subscription expressions in simple consumer.
     *
     * @return array map of topics to filter expression.
     */
    public function getSubscriptionExpressions(): array;

    /**
     * Fetch messages from the server synchronously.
     * <p> This method returns immediately if there are messages available.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty map will be returned.
     *
     * @param int $maxMessageNum max message num of server returned.
     * @param int $invisibleDuration set the invisibleDuration of messages to return from the server. These messages will be
     *                          invisible to other consumers unless timeout.
     * @return array list of message view.
     * @throws ClientException If an error occurs
     */
    public function receive(int $maxMessageNum, int $invisibleDuration): array;

    /**
     * Fetch messages from the server asynchronously.
     * <p> This method returns immediately if there are messages available.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty map will be returned.
     *
     * @param int $maxMessageNum max message num of server returned.
     * @param int $invisibleDuration set the invisibleDuration of messages to return from the server. These messages will be
     *                          invisible to other consumers unless timeout.
     * @return mixed list of message view.
     */
    public function receiveAsync(int $maxMessageNum, int $invisibleDuration);

    /**
     * Ack message to server synchronously, server commit this message.
     *
     * <p>Duplicate ack request does not take effect and throw an exception.
     *
     * @param MessageView $messageView special message view with handle want to ack.
     * @throws ClientException If an error occurs
     */
    public function ack(MessageView $messageView): void;

    /**
     * Ack message to the server asynchronously, server commit this message.
     *
     * <p> Duplicate ack request does not take effect and throw an exception.
     *
     * @param MessageView $messageView special message view with handle want to ack.
     * @return mixed CompletableFuture of this request.
     */
    public function ackAsync(MessageView $messageView);

    /**
     * Changes the invisible duration of a specified message synchronously.
     *
     * <p> The origin invisible duration for a message decide by ack request.
     *
     * <p>Duplicate change requests will refresh the next visible time of this message to consumers.
     *
     * @param MessageView $messageView the message view to change invisible time.
     * @param int $invisibleDuration new timestamp the message could be visible and re-consume which start from current time.
     * @throws ClientException If an error occurs
     */
    public function changeInvisibleDuration(MessageView $messageView, int $invisibleDuration): void;

    /**
     * Changes the invisible duration of a specified message asynchronously.
     *
     * <p> The origin invisible duration for a message decide by ack request.
     *
     * <p>Duplicate change requests will refresh the next visible time of this message to consumers.
     *
     * @param MessageView $messageView the message view to change invisible time.
     * @param int $invisibleDuration new timestamp the message could be visible and re-consume which start from current time.
     * @return mixed CompletableFuture of this request.
     */
    public function changeInvisibleDurationAsync(MessageView $messageView, int $invisibleDuration);

    /**
     * Close the simple consumer and release all related resources.
     *
     * <p>Once simple consumer is closed, <strong>it could not be started once again.</strong> we maintained an FSM
     * (finite-state machine) to record the different states for each simple consumer.
     *
     * @return void
     */
    public function close();
}
