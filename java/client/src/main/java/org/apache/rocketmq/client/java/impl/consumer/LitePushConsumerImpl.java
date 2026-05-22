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
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumer;
import org.apache.rocketmq.client.apis.consumer.OffsetOption;
import org.apache.rocketmq.client.java.impl.ClientType;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LitePushConsumerImpl extends PushConsumerImpl implements LitePushConsumer {
    private static final Logger log = LoggerFactory.getLogger(LitePushConsumerImpl.class);

    private final LiteSubscriptionManager liteSubscriptionManager;

    public LitePushConsumerImpl(LitePushConsumerBuilderImpl builder) {
        super(builder.clientConfiguration, builder.consumerGroup,
            builder.subscriptionExpressions, builder.messageListener,
            null, null,
            builder.maxCacheMessageCount, builder.maxCacheMessageSizeInBytes,
            builder.consumptionThreadCount, builder.enableFifoConsumeAccelerator, false);
        this.liteSubscriptionManager = new LiteSubscriptionManager(this,
            new Resource(builder.clientConfiguration.getNamespace(), builder.bindTopic),
            groupResource);
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
        return ClientType.LITE_PUSH_CONSUMER;
    }

    @Override
    protected ConsumeService createConsumeService() {
        final ScheduledExecutorService scheduler = this.getClientManager().getScheduler();
        if (getSettings().isFifo()) {
            log.info("Create Lite FIFO consume service, consumerGroup={}, clientId={}, enableFifoConsumeAccelerator={}",
                getConsumerGroup(), clientId, enableFifoConsumeAccelerator);
            return new LiteFifoConsumeService(clientId, messageListener, consumptionExecutor, this,
                scheduler, enableFifoConsumeAccelerator);
        }
        log.info("Create Lite standard consume service, consumerGroup={}, clientId={}", getConsumerGroup(), clientId);
        return new LiteStandardConsumeService(clientId, messageListener, consumptionExecutor, this, scheduler);
    }

}