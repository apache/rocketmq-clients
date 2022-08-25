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

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Assert;
import org.junit.Test;

public class SimpleSubscriptionSettingsTest extends TestBase {

    @Test
    public void testToProtobuf() {
        Resource groupResource = new Resource(FAKE_CONSUMER_GROUP_0);
        ClientId clientId = new ClientId();
        Map<String, FilterExpression> subscriptionExpression = new HashMap<>();
        subscriptionExpression.put(FAKE_TOPIC_0, new FilterExpression());
        final Duration requestTimeout = Duration.ofSeconds(3);
        final Duration longPollingTimeout = Duration.ofSeconds(15);
        final SimpleSubscriptionSettings simpleSubscriptionSettings = new SimpleSubscriptionSettings(clientId,
            fakeEndpoints(), groupResource, requestTimeout, longPollingTimeout, subscriptionExpression);
        final Settings settings = simpleSubscriptionSettings.toProtobuf();
        Assert.assertEquals(settings.getClientType(), ClientType.SIMPLE_CONSUMER);
        Assert.assertEquals(settings.getRequestTimeout(), Durations.fromNanos(requestTimeout.toNanos()));
        Assert.assertTrue(settings.hasSubscription());
        final Subscription subscription = settings.getSubscription();
        Assert.assertEquals(subscription.getGroup(),
            apache.rocketmq.v2.Resource.newBuilder().setName(FAKE_CONSUMER_GROUP_0).build());
        Assert.assertFalse(subscription.getFifo());
        Assert.assertEquals(subscription.getLongPollingTimeout(), Durations.fromNanos(longPollingTimeout.toNanos()));
        final List<SubscriptionEntry> subscriptionsList = subscription.getSubscriptionsList();
        Assert.assertEquals(subscriptionsList.size(), 1);
        final SubscriptionEntry subscriptionEntry = subscriptionsList.get(0);
        Assert.assertEquals(subscriptionEntry.getExpression().getType(), FilterType.TAG);
        Assert.assertEquals(subscriptionEntry.getTopic(),
            apache.rocketmq.v2.Resource.newBuilder().setName(FAKE_TOPIC_0).build());
    }

    @Test
    public void testToProtobufWithSqlExpression() {
        Resource groupResource = new Resource(FAKE_CONSUMER_GROUP_0);
        ClientId clientId = new ClientId();
        Map<String, FilterExpression> subscriptionExpression = new HashMap<>();
        subscriptionExpression.put(FAKE_TOPIC_0, new FilterExpression("(a > 10 AND a < 100) OR (b IS NOT NULL AND "
            + "b=TRUE)", FilterExpressionType.SQL92));
        final Duration requestTimeout = Duration.ofSeconds(3);
        final Duration longPollingTimeout = Duration.ofSeconds(15);
        final SimpleSubscriptionSettings simpleSubscriptionSettings = new SimpleSubscriptionSettings(clientId,
            fakeEndpoints(), groupResource, requestTimeout, longPollingTimeout, subscriptionExpression);
        final Settings settings = simpleSubscriptionSettings.toProtobuf();
        Assert.assertEquals(settings.getClientType(), ClientType.SIMPLE_CONSUMER);
        Assert.assertEquals(settings.getRequestTimeout(), Durations.fromNanos(requestTimeout.toNanos()));
        Assert.assertTrue(settings.hasSubscription());
        final Subscription subscription = settings.getSubscription();
        Assert.assertEquals(subscription.getGroup(),
            apache.rocketmq.v2.Resource.newBuilder().setName(FAKE_CONSUMER_GROUP_0).build());
        Assert.assertFalse(subscription.getFifo());
        Assert.assertEquals(subscription.getLongPollingTimeout(), Durations.fromNanos(longPollingTimeout.toNanos()));
        final List<SubscriptionEntry> subscriptionsList = subscription.getSubscriptionsList();
        Assert.assertEquals(subscriptionsList.size(), 1);
        final SubscriptionEntry subscriptionEntry = subscriptionsList.get(0);
        Assert.assertEquals(subscriptionEntry.getExpression().getType(), FilterType.SQL);
        Assert.assertEquals(subscriptionEntry.getTopic(),
            apache.rocketmq.v2.Resource.newBuilder().setName(FAKE_TOPIC_0).build());
    }

}