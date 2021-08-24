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

package org.apache.rocketmq.client.impl.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.ConsumerGroup;
import apache.rocketmq.v1.FilterExpression;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SubscriptionEntry;
import java.util.List;
import org.apache.rocketmq.client.consumer.ConsumeContext;
import org.apache.rocketmq.client.consumer.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.filter.ExpressionType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.impl.ClientManager;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.tools.TestBase;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PushConsumerImplTest extends TestBase {
    @Mock
    private ClientManager clientManager;

    @InjectMocks
    private final PushConsumerImpl consumerImpl = new PushConsumerImpl(dummyGroup0);

    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);
        consumerImpl.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeStatus consume(List<MessageExt> messages, ConsumeContext context) {
                return null;
            }
        });
    }

    @Test
    public void setOffsetStoreWithNull() {
        try {
            consumerImpl.setOffsetStore(null);
            fail();
        } catch (NullPointerException ignore) {
            // Ignore on purpose.
        }
    }

    @Test
    public void testHasCustomOffsetStore() {
        OffsetStore offsetStore = new OffsetStore() {
            @Override
            public void start() {
            }

            @Override
            public void shutdown() {
            }

            @Override
            public void updateOffset(MessageQueue mq, long offset) {
            }

            @Override
            public long readOffset(MessageQueue mq) {
                return 0;
            }
        };
        consumerImpl.setOffsetStore(offsetStore);
        assertTrue(consumerImpl.hasCustomOffsetStore());
    }

    @Test
    public void testPrepareHeartbeatData() {
        consumerImpl.subscribe(dummyTopic0, "*", ExpressionType.TAG);
        HeartbeatEntry heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(heartbeatEntry.getClientId(), consumerImpl.getClientId());
        final ConsumerGroup consumerGroup = heartbeatEntry.getConsumerGroup();
        final Resource group = consumerGroup.getGroup();
        assertEquals(group.getName(), consumerImpl.getGroup());
        assertEquals(group.getArn(), consumerImpl.getArn());
        final List<SubscriptionEntry> subscriptionsList = consumerGroup.getSubscriptionsList();
        assertEquals(1, subscriptionsList.size());
        final SubscriptionEntry subscriptionEntry = subscriptionsList.get(0);
        final Resource topicResource = subscriptionEntry.getTopic();
        assertEquals(dummyTopic0, topicResource.getName());
        final FilterExpression expression = subscriptionEntry.getExpression();
        assertEquals(expression.getType(), FilterType.TAG);

        consumerImpl.setMessageModel(MessageModel.BROADCASTING);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumeModel.BROADCASTING, heartbeatEntry.getConsumerGroup().getConsumeModel());

        consumerImpl.setMessageModel(MessageModel.CLUSTERING);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumeModel.CLUSTERING, heartbeatEntry.getConsumerGroup().getConsumeModel());

        consumerImpl.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumePolicy.PLAYBACK, heartbeatEntry.getConsumerGroup().getConsumePolicy());

        consumerImpl.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_MAX_OFFSET);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumePolicy.DISCARD, heartbeatEntry.getConsumerGroup().getConsumePolicy());

        consumerImpl.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumePolicy.TARGET_TIMESTAMP, heartbeatEntry.getConsumerGroup().getConsumePolicy());

        consumerImpl.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumePolicy.RESUME, heartbeatEntry.getConsumerGroup().getConsumePolicy());

        consumerImpl.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeStatus consume(List<MessageExt> messages, ConsumeContext context) {
                return null;
            }
        });
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertTrue(heartbeatEntry.getNeedRebalance());
    }
}
