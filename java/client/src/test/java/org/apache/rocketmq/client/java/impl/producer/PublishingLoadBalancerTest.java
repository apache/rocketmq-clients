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

package org.apache.rocketmq.client.java.impl.producer;

import apache.rocketmq.v2.MessageQueue;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Assert;
import org.junit.Test;

public class PublishingLoadBalancerTest extends TestBase {

    @Test
    public void testTakeMessageQueueByMessageGroup() {
        final MessageQueue messageQueue = fakePbMessageQueue0();
        List<MessageQueue> mqs = new ArrayList<>();
        mqs.add(messageQueue);
        final TopicRouteData topicRouteData = new TopicRouteData(mqs);
        final PublishingLoadBalancer publishingLoadBalancer = new PublishingLoadBalancer(topicRouteData);
        Assert.assertNotNull(publishingLoadBalancer.takeMessageQueueByMessageGroup("test"));
    }

    @Test
    public void testTakeTwoMessageQueues() {
        final MessageQueue messageQueue = fakePbMessageQueue0();
        List<MessageQueue> mqs = new ArrayList<>();
        mqs.add(messageQueue);
        List<MessageQueueImpl> messageQueueImpls = new ArrayList<>();
        messageQueueImpls.add(new MessageQueueImpl(messageQueue));
        final TopicRouteData topicRouteData = new TopicRouteData(mqs);
        final PublishingLoadBalancer publishingLoadBalancer = new PublishingLoadBalancer(topicRouteData);
        final List<MessageQueueImpl> result = publishingLoadBalancer.takeMessageQueues(new HashSet<>(), 2);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(result, messageQueueImpls);
    }

    @Test
    public void testTakeMessageQueuesWithALlEndpointsIsolated() {
        final MessageQueue messageQueue = fakePbMessageQueue0();
        List<MessageQueue> mqs = new ArrayList<>();
        mqs.add(messageQueue);
        List<MessageQueueImpl> messageQueueImpls = new ArrayList<>();
        messageQueueImpls.add(new MessageQueueImpl(messageQueue));
        final TopicRouteData topicRouteData = new TopicRouteData(mqs);
        final PublishingLoadBalancer publishingLoadBalancer = new PublishingLoadBalancer(topicRouteData);
        final Endpoints endpoints = new Endpoints(messageQueue.getBroker().getEndpoints());
        Set<Endpoints> isolated = new HashSet<>();
        isolated.add(endpoints);
        final List<MessageQueueImpl> result = publishingLoadBalancer.takeMessageQueues(isolated, 1);
        Assert.assertEquals(result, messageQueueImpls);
    }
}