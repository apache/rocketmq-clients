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
import org.apache.rocketmq.client.apis.ClientException;

public interface PullConsumerBuilder {
    /**
     * Set the client configuration for the consumer.
     *
     * @param clientConfiguration client's configuration.
     * @return the consumer builder instance.
     */
    PullConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration);

    /**
     * Set the load balancing group for the consumer.
     *
     * @param consumerGroup consumer load balancing group.
     * @return the consumer builder instance.
     */
    PullConsumerBuilder setConsumerGroup(String consumerGroup);

    /**
     * Automate the consumer's offset commit.
     *
     * @return the consumer builder instance.
     */
    PullConsumerBuilder enableAutoCommit(boolean enable);

    /**
     * Set the consumer's offset commit interval if auto commit is enabled.
     *
     * @param duration offset commit interval
     * @return the consumer builder instance.
     */
    PullConsumerBuilder setAutoCommitInterval(Duration duration);

    /**
     * Set the maximum number of messages cached locally.
     *
     * @param count message count.
     * @return the consumer builder instance.
     */
    PullConsumerBuilder setMaxCacheMessageCountEachQueue(int count);

    /**
     * Set the maximum bytes of messages cached locally.
     *
     * @param bytes message size.
     * @return the consumer builder instance.
     */
    PullConsumerBuilder setMaxCacheMessageSizeInBytesEachQueue(int bytes);

    /**
     * Finalize the build of {@link PullConsumer} and start.
     *
     * <p>This method will block until the pull consumer starts successfully.
     *
     * <p>Especially, if this method is invoked more than once, different pull consumer will be created and started.
     *
     * @return the pull consumer instance.
     */
    PullConsumer build() throws ClientException;
}
