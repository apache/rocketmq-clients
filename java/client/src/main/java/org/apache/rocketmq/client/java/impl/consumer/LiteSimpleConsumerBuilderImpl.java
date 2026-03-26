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
import java.time.Duration;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.LiteSimpleConsumer;
import org.apache.rocketmq.client.apis.consumer.LiteSimpleConsumerBuilder;

public class LiteSimpleConsumerBuilderImpl implements LiteSimpleConsumerBuilder {

    protected String bindTopic = null;
    protected ClientConfiguration clientConfiguration = null;
    protected String consumerGroup = null;
    protected Map<String, FilterExpression> subscriptionExpressions = null;
    protected Duration awaitDuration = null;

    @Override
    public LiteSimpleConsumerBuilder bindTopic(String bindTopic) {
        checkArgument(StringUtils.isNotBlank(bindTopic), "bindTopic should not be blank");
        this.bindTopic = bindTopic;
        // Default subscription: (bindTopic, *) for code reuse.
        this.subscriptionExpressions = ImmutableMap.of(bindTopic, FilterExpression.SUB_ALL);
        return this;
    }

    @Override
    public LiteSimpleConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        return this;
    }

    @Override
    public LiteSimpleConsumerBuilder setConsumerGroup(String consumerGroup) {
        checkNotNull(consumerGroup, "consumerGroup should not be null");
        checkArgument(CONSUMER_GROUP_PATTERN.matcher(consumerGroup).matches(),
            "consumerGroup does not match the regex [regex=%s]", CONSUMER_GROUP_PATTERN.pattern());
        this.consumerGroup = consumerGroup;
        return this;
    }

    @Override
    public LiteSimpleConsumerBuilder setAwaitDuration(Duration awaitDuration) {
        this.awaitDuration = checkNotNull(awaitDuration, "awaitDuration should not be null");
        return this;
    }

    @Override
    public LiteSimpleConsumer build() throws ClientException {
        checkNotNull(clientConfiguration, "clientConfiguration has not been set yet");
        checkNotNull(consumerGroup, "consumerGroup has not been set yet");
        checkNotNull(awaitDuration, "awaitDuration has not been set yet");
        checkNotNull(bindTopic, "bindTopic has not been set yet");
        final LiteSimpleConsumerImpl liteSimpleConsumer = new LiteSimpleConsumerImpl(this);
        liteSimpleConsumer.startAsync().awaitRunning();
        return liteSimpleConsumer;
    }
}