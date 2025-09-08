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
import java.util.Map;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;

/**
 * Builder to config and start {@link LitePushConsumer}.
 */
public interface LitePushConsumerBuilder extends PushConsumerBuilder {

    LitePushConsumerBuilder bindTopic(String bindTopic);

    LitePushConsumerBuilder setInvisibleDuration(Duration invisibleDuration);

    @Override
    LitePushConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration);

    @Override
    LitePushConsumerBuilder setConsumerGroup(String consumerGroup);

    @Override
    LitePushConsumerBuilder setSubscriptionExpressions(Map<String, FilterExpression> subscriptionExpressions);

    @Override
    LitePushConsumerBuilder setMessageListener(MessageListener listener);

    @Override
    LitePushConsumerBuilder setMaxCacheMessageCount(int count);

    @Override
    LitePushConsumerBuilder setMaxCacheMessageSizeInBytes(int bytes);

    @Override
    LitePushConsumerBuilder setConsumptionThreadCount(int count);

    @Override
    LitePushConsumerBuilder setEnableFifoConsumeAccelerator(boolean enableFifoConsumeAccelerator);

    @Override
    LitePushConsumer build() throws ClientException;
}
