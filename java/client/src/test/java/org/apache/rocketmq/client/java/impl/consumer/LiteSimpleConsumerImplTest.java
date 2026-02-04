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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LiteSimpleConsumerImplTest extends TestBase {

    @Test
    public void testUpdateSubscriptionLoadBalancerWithTwoReadableQueues() {
        final ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();

        LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        builder.setClientConfiguration(clientConfiguration);
        builder.setConsumerGroup(FAKE_CONSUMER_GROUP_0);
        builder.setAwaitDuration(Duration.ofSeconds(30));
        builder.bindTopic(FAKE_TOPIC_0);
        LiteSimpleConsumerImpl consumer = new LiteSimpleConsumerImpl(builder);

        TopicRouteData topicRouteData = fakeTopicRouteData(
            FAKE_TOPIC_0,
            fakePbBroker0(),
            Arrays.asList(apache.rocketmq.v2.Permission.READ, apache.rocketmq.v2.Permission.READ),
            Arrays.asList(-1, 0)
        );
        SubscriptionLoadBalancer loadBalancer = consumer.updateSubscriptionLoadBalancer(FAKE_TOPIC_0, topicRouteData);

        assertEquals(1, loadBalancer.messageQueues.size());
        MessageQueueImpl mq = loadBalancer.messageQueues.get(0);
        assertTrue(mq.getPermission().isReadable());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSubscriptionLoadBalancerWithTwoWritableQueues() {
        final ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();

        LiteSimpleConsumerBuilderImpl builder = new LiteSimpleConsumerBuilderImpl();
        builder.setClientConfiguration(clientConfiguration);
        builder.setConsumerGroup(FAKE_CONSUMER_GROUP_0);
        builder.setAwaitDuration(Duration.ofSeconds(30));
        builder.bindTopic(FAKE_TOPIC_0);
        LiteSimpleConsumerImpl consumer = new LiteSimpleConsumerImpl(builder);

        TopicRouteData topicRouteData = fakeTopicRouteData(
            FAKE_TOPIC_0,
            fakePbBroker0(),
            Arrays.asList(apache.rocketmq.v2.Permission.WRITE, apache.rocketmq.v2.Permission.WRITE),
            Arrays.asList(-1, 0)
        );
        SubscriptionLoadBalancer loadBalancer = consumer.updateSubscriptionLoadBalancer(FAKE_TOPIC_0, topicRouteData);

        assertEquals(0, loadBalancer.messageQueues.size());
    }

    static TopicRouteData fakeTopicRouteData(String topic, apache.rocketmq.v2.Broker broker,
        List<apache.rocketmq.v2.Permission> permissions,
        List<Integer> queueIds) {
        List<apache.rocketmq.v2.MessageQueue> messageQueueList = new ArrayList<>();
        apache.rocketmq.v2.Resource topicResource = apache.rocketmq.v2.Resource.newBuilder()
            .setName(topic).build();

        for (int i = 0; i < permissions.size(); i++) {
            apache.rocketmq.v2.MessageQueue mq = apache.rocketmq.v2.MessageQueue.newBuilder()
                .setTopic(topicResource)
                .setBroker(broker)
                .setPermission(permissions.get(i))
                .setId(queueIds.get(i))
                .build();
            messageQueueList.add(mq);
        }

        return new TopicRouteData(messageQueueList);
    }
}