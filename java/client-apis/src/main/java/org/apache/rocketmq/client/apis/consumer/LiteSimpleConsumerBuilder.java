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

import java.time.Duration;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;

/**
 * Builder to config and start {@link LiteSimpleConsumer}.
 */
public interface LiteSimpleConsumerBuilder {

    /**
     * Set the bind topic for lite consumer.
     *
     * @return the consumer builder instance.
     */
    LiteSimpleConsumerBuilder bindTopic(String bindTopic);

    /**
     * Set the client configuration for the lite simple consumer.
     *
     * @param clientConfiguration client's configuration.
     * @return the lite simple consumer builder instance.
     */
    LiteSimpleConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration);

    /**
     * Set the load balancing group for the lite simple consumer.
     *
     * @param consumerGroup consumer load balancing group.
     * @return the consumer builder instance.
     */
    LiteSimpleConsumerBuilder setConsumerGroup(String consumerGroup);

    /**
     * Set the max await time when receive messages from the server.
     *
     * <p>The lite simple consumer will hold this long-polling receive requests until a message is returned or a timeout
     * occurs.
     *
     * <p> Especially, the RPC request timeout for long-polling of {@link LiteSimpleConsumer} is increased by
     * {@linkplain ClientConfigurationBuilder#setRequestTimeout(Duration) request timeout} based on await duration here.
     *
     * @param awaitDuration The maximum time to block when no message is available.
     * @return the consumer builder instance.
     */
    LiteSimpleConsumerBuilder setAwaitDuration(Duration awaitDuration);

    /**
     * Finalize the build of the {@link LiteSimpleConsumer} instance and start.
     *
     * <p>This method will block until the lite simple consumer starts successfully.
     *
     * <p>Especially, if this method is invoked more than once,
     * different lite simple consumers will be created and started.
     *
     * @return the lite simple consumer instance.
     */
    LiteSimpleConsumer build() throws ClientException;
}