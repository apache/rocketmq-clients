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

package org.apache.rocketmq.client.java.impl.consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rocketmq.client.java.impl.consumer.ConsumerImpl.CONSUMER_GROUP_PATTERN;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumer;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumerBuilder;
import org.apache.rocketmq.client.apis.consumer.MessageListener;

public class LitePushConsumerBuilderImpl implements LitePushConsumerBuilder {

    protected String bindTopic = null;
    // below is same as PushConsumerBuilderImpl
    protected ClientConfiguration clientConfiguration = null;
    protected String consumerGroup = null;
    protected Map<String, FilterExpression> subscriptionExpressions = null;
    protected MessageListener messageListener = null;
    protected int maxCacheMessageCount = 1024;
    protected int maxCacheMessageSizeInBytes = 64 * 1024 * 1024;
    protected int consumptionThreadCount = 20;
    protected boolean enableFifoConsumeAccelerator = false;

    @Override
    public LitePushConsumerBuilder bindTopic(String bindTopic) {
        checkArgument(StringUtils.isNotBlank(bindTopic), "bindTopic should not be blank");
        this.bindTopic = bindTopic;
        return this;
    }

    @Override
    public LitePushConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        return this;
    }

    @Override
    public LitePushConsumerBuilder setConsumerGroup(String consumerGroup) {
        checkNotNull(consumerGroup, "consumerGroup should not be null");
        checkArgument(CONSUMER_GROUP_PATTERN.matcher(consumerGroup).matches(), "consumerGroup does not match the "
            + "regex [regex=%s]", CONSUMER_GROUP_PATTERN.pattern());
        this.consumerGroup = consumerGroup;
        return this;
    }

    @Override
    public LitePushConsumerBuilder setMessageListener(MessageListener messageListener) {
        this.messageListener = checkNotNull(messageListener, "messageListener should not be null");
        return this;
    }

    @Override
    public LitePushConsumerBuilder setMaxCacheMessageCount(int maxCachedMessageCount) {
        checkArgument(maxCachedMessageCount > 0, "maxCachedMessageCount should be positive");
        this.maxCacheMessageCount = maxCachedMessageCount;
        return this;
    }

    @Override
    public LitePushConsumerBuilder setMaxCacheMessageSizeInBytes(int maxCacheMessageSizeInBytes) {
        checkArgument(maxCacheMessageSizeInBytes > 0, "maxCacheMessageSizeInBytes should be positive");
        this.maxCacheMessageSizeInBytes = maxCacheMessageSizeInBytes;
        return this;
    }

    @Override
    public LitePushConsumerBuilder setConsumptionThreadCount(int consumptionThreadCount) {
        checkArgument(consumptionThreadCount > 0, "consumptionThreadCount should be positive");
        this.consumptionThreadCount = consumptionThreadCount;
        return this;
    }

    @Override
    public LitePushConsumerBuilder setEnableFifoConsumeAccelerator(boolean enableFifoConsumeAccelerator) {
        this.enableFifoConsumeAccelerator = enableFifoConsumeAccelerator;
        return this;
    }

    @Override
    public LitePushConsumer build() throws ClientException {
        checkNotNull(clientConfiguration, "clientConfiguration has not been set yet");
        checkNotNull(consumerGroup, "consumerGroup has not been set yet");
        checkNotNull(messageListener, "messageListener has not been set yet");
        checkNotNull(bindTopic, "bindTopic has not been set yet");
        // passing bindTopic through subscriptionExpressions to ClientImpl
        subscriptionExpressions = ImmutableMap.of(bindTopic, FilterExpression.SUB_ALL);;
        final LitePushConsumerImpl litePushConsumer = new LitePushConsumerImpl(this);
        litePushConsumer.startAsync().awaitRunning();
        return litePushConsumer;
    }

}
