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
import java.util.Set;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;

/**
 * Similar to {@link SimpleConsumer}, but for lite topic.
 */
public interface LiteSimpleConsumer extends Closeable {

    /**
     * Get the load balancing group for the lite simple consumer.
     *
     * @return consumer load balancing group.
     */
    String getConsumerGroup();

    /**
     * Subscribe to a lite topic.
     * Similar to  {@link LitePushConsumer#subscribeLite(String)}
     *
     * @param liteTopic the name of the lite topic to subscribe
     * @throws ClientException if an error occurs during subscription
     */
    void subscribeLite(String liteTopic) throws ClientException;

    /**
     * Subscribe to a lite topic with consumeFromOption to specify the consume from offset.
     * Similar to  {@link LitePushConsumer#subscribeLite(String, OffsetOption)}
     *
     * @param liteTopic    the name of the lite topic to subscribe
     * @param offsetOption the consume from offset
     * @throws ClientException if an error occurs during subscription
     */
    void subscribeLite(String liteTopic, OffsetOption offsetOption) throws ClientException;

    /**
     * Unsubscribe from a lite topic.
     *
     * @param liteTopic the name of the lite topic to unsubscribe from
     * @throws ClientException if an error occurs during unsubscription
     */
    void unsubscribeLite(String liteTopic) throws ClientException;

    /**
     * Get the lite topic immutable set.
     *
     * @return lite topic immutable set.
     */
    Set<String> getLiteTopicSet();

    /**
     * Fetch messages from the server synchronously.
     * <p> This method returns immediately if there are messages available.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty list will be returned.
     *
     * @param maxMessageNum     max message num of server returned.
     * @param invisibleDuration set the invisibleDuration of messages to return from the server. These messages will be
     *                          invisible to other consumers unless timeout.
     * @return list of message view.
     */
    List<MessageView> receive(int maxMessageNum, Duration invisibleDuration) throws ClientException;

    /**
     * Ack message to server synchronously, server commit this message.
     *
     * <p>Duplicate ack request does not take effect and throw an exception.
     *
     * @param messageView special message view with handle want to ack.
     */
    void ack(MessageView messageView) throws ClientException;

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
     * Close the lite simple consumer and release all related resources.
     *
     * <p>Once lite simple consumer is closed, <strong>it could not be started once again.</strong> we maintained an FSM
     * (finite-state machine) to record the different states for each lite simple consumer.
     */
    @Override
    void close() throws IOException;
}