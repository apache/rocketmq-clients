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

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;

/**
 * Builder to config and start {@link LitePushConsumer}.
 */
public interface LitePushConsumerBuilder {

    /**
     * Set the bind topic for lite push consumer.
     *
     * @return the consumer builder instance.
     */
    LitePushConsumerBuilder bindTopic(String bindTopic);

    /**
     * Set the client configuration for the consumer.
     *
     * @param clientConfiguration client's configuration.
     * @return the consumer builder instance.
     */
    LitePushConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration);

    /**
     * Set the load balancing group for the consumer.
     *
     * @param consumerGroup consumer load balancing group.
     * @return the consumer builder instance.
     */
    LitePushConsumerBuilder setConsumerGroup(String consumerGroup);

    /**
     * Register message listener, all messages meet the subscription expression would across listener here.
     *
     * @param listener message listener.
     * @return the consumer builder instance.
     */
    LitePushConsumerBuilder setMessageListener(MessageListener listener);

    /**
     * Set the maximum number of messages cached locally.
     *
     * @param count message count.
     * @return the consumer builder instance.
     */
    LitePushConsumerBuilder setMaxCacheMessageCount(int count);

    /**
     * Set the maximum bytes of messages cached locally.
     *
     * @param bytes message size.
     * @return the consumer builder instance.
     */
    LitePushConsumerBuilder setMaxCacheMessageSizeInBytes(int bytes);

    /**
     * Set the consumption thread count in parallel.
     *
     * @param count thread count.
     * @return the consumer builder instance.
     */
    LitePushConsumerBuilder setConsumptionThreadCount(int count);

    /**
     * Set enable fifo consume accelerator. If enabled, the consumer will consume messages in parallel by messageGroup,
     * it may increase the probability of repeatedly consuming the same message.
     *
     * @param enableFifoConsumeAccelerator  enable fifo parallel processing.
     * @return the consumer builder instance.
     */
    LitePushConsumerBuilder setEnableFifoConsumeAccelerator(boolean enableFifoConsumeAccelerator);

    /**
     * Finalize the build of {@link LitePushConsumer} and start.
     *
     * <p>This method will block until the push consumer starts successfully.
     *
     * <p>Especially, if this method is invoked more than once, different push consumers will be created and started.
     *
     * @return the lite push consumer instance.
     */
    LitePushConsumer build() throws ClientException;
}
