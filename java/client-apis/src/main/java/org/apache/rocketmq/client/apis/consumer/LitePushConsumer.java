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
import java.util.Set;
import org.apache.rocketmq.client.apis.ClientException;

public interface LitePushConsumer extends Closeable {

    /**
     * Subscribe to a lite topic.
     *
     * @param liteTopic the name of the lite topic to subscribe to
     * @throws ClientException if an error occurs during subscription
     */
    void subscribeLite(String liteTopic) throws ClientException;

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
     * Get the load balancing group for the consumer.
     *
     * @return consumer load balancing group.
     */
    String getConsumerGroup();

    /**
     * Close the consumer and release all related resources.
     *
     * <p>Once consumer is closed, <strong>it could not be started once again.</strong> we maintained an FSM
     * (finite-state machine) to record the different states for each push consumer.
     */
    @Override
    void close() throws IOException;
}
