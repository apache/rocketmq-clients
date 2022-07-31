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

package org.apache.rocketmq.client.apis.producer;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.Message;

/**
 * Builder to config and start {@link Producer}.
 */
public interface ProducerBuilder {
    /**
     * Set the client configuration for the producer.
     *
     * @param clientConfiguration client's configuration.
     * @return the producer builder instance.
     */
    ProducerBuilder setClientConfiguration(ClientConfiguration clientConfiguration);

    /**
     * Declare topics ahead of message sending.
     *
     * <p>Even though the declaration is not essential, we <strong>highly recommend</strong> to declare the topics in
     * advance, which could help to discover potential mistakes.
     *
     * <pre>{@code
     * // Example 0: single topic.
     * producerBuilder.setTopics("topicA");
     * // Example 1: multiple topics.
     * producerBuilder.setTopics("topicA", "topicB");
     * // Example 2: multiple topics.
     * ArrayList<String> topicList = new ArrayList<>();
     * topicList.add("topicA");
     * topicList.add("topicB");
     * producerBuilder.setTopics(topicList);
     * }</pre>
     *
     * @param topics topics to send/prepare.
     * @return the producer builder instance.
     */
    ProducerBuilder setTopics(String... topics);

    /**
     * Set the max attempts for max internal retries of message publishing.
     *
     * <p>Internal retries are not performed for transactional message({@link Producer#send(Message, Transaction)})
     *
     * @param maxAttempts max attempts for max internal retries of message publishing.
     * @return the producer builder instance.
     */
    ProducerBuilder setMaxAttempts(int maxAttempts);

    /**
     * Set the transaction checker for the producer.
     *
     * @param checker transaction checker.
     * @return the produce builder instance.
     */
    ProducerBuilder setTransactionChecker(TransactionChecker checker);

    /**
     * Finalize the build of {@link Producer} instance and start.
     *
     * <p>This method will block until the push consumer starts successfully.
     *
     * <p>Especially, if this method is invoked more than once, different producers will be created and started.
     *
     * @return the producer instance.
     */
    Producer build() throws ClientException;
}
