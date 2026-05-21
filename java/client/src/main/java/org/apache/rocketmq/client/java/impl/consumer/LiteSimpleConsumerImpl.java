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

import apache.rocketmq.v2.NotifyUnsubscribeLiteCommand;
import apache.rocketmq.v2.Settings;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.LiteSimpleConsumer;
import org.apache.rocketmq.client.apis.consumer.OffsetOption;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.impl.ClientType;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;

public class LiteSimpleConsumerImpl extends SimpleConsumerImpl implements LiteSimpleConsumer {

    private final LiteSubscriptionManager liteSubscriptionManager;

    public LiteSimpleConsumerImpl(LiteSimpleConsumerBuilderImpl builder) {
        super(builder.clientConfiguration, builder.consumerGroup,
            builder.awaitDuration, builder.subscriptionExpressions);
        this.liteSubscriptionManager = new LiteSubscriptionManager(this,
            new Resource(builder.clientConfiguration.getNamespace(), builder.bindTopic),
            new Resource(builder.clientConfiguration.getNamespace(), getConsumerGroup()));
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();
        liteSubscriptionManager.startUp();
    }

    @Override
    public void onSettingsCommand(Endpoints endpoints, Settings settings) {
        super.onSettingsCommand(endpoints, settings);
        liteSubscriptionManager.sync(settings);
    }

    @Override
    public void subscribeLite(String liteTopic) throws ClientException {
        liteSubscriptionManager.subscribeLite(liteTopic, null);
    }

    @Override
    public void subscribeLite(String liteTopic, OffsetOption offsetOption) throws ClientException {
        liteSubscriptionManager.subscribeLite(liteTopic, offsetOption);
    }

    @Override
    public void unsubscribeLite(String liteTopic) throws ClientException {
        liteSubscriptionManager.unsubscribeLite(liteTopic);
    }

    @Override
    public Set<String> getLiteTopicSet() {
        return liteSubscriptionManager.getLiteTopicSet();
    }

    @Override
    public void onNotifyUnsubscribeLiteCommand(Endpoints endpoints, NotifyUnsubscribeLiteCommand command) {
        liteSubscriptionManager.onNotifyUnsubscribeLiteCommand(command);
    }

    @Override
    protected ClientType clientType() {
        return ClientType.LITE_SIMPLE_CONSUMER;
    }

    @Override
    protected SubscriptionLoadBalancer updateSubscriptionLoadBalancer(String topic, TopicRouteData topicRouteData) {
        // For lite consumers, we only need routes to brokers, so keep only the first readable queue
        final TopicRouteData liteTopicRouteData = new TopicRouteData(
            topicRouteData.getMessageQueues().stream()
                // find the first readable queue
                .filter(SubscriptionLoadBalancer::isReadableMasterQueue)
                .findFirst()
                .map(MessageQueueImpl::toProtobuf)
                .map(Collections::singletonList)
                .orElse(Collections.emptyList())
        );
        return super.updateSubscriptionLoadBalancer(topic, liteTopicRouteData);
    }

    @Override
    public CompletableFuture<List<MessageView>> receiveAsync(int maxMessageNum, Duration invisibleDuration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> ackAsync(MessageView messageView) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView, Duration invisibleDuration) {
        throw new UnsupportedOperationException();
    }
}