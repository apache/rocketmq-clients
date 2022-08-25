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
import apache.rocketmq.v2.CustomizedBackoff;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import java.util.ArrayList;
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

public class PushSubscriptionSettingsTest extends TestBase {

    @Test
    public void testToProtobuf() {
        Resource groupResource = new Resource(FAKE_CONSUMER_GROUP_0);
        ClientId clientId = new ClientId();
        Map<String, FilterExpression> subscriptionExpression = new HashMap<>();
        subscriptionExpression.put(FAKE_TOPIC_0, new FilterExpression());
        final Duration requestTimeout = Duration.ofSeconds(3);
        final PushSubscriptionSettings pushSubscriptionSettings = new PushSubscriptionSettings(clientId,
            fakeEndpoints(), groupResource, requestTimeout, subscriptionExpression);
        final Settings settings = pushSubscriptionSettings.toProtobuf();
        Assert.assertEquals(settings.getClientType(), ClientType.PUSH_CONSUMER);
        Assert.assertEquals(settings.getRequestTimeout(), Durations.fromNanos(requestTimeout.toNanos()));
        Assert.assertTrue(settings.hasSubscription());
        final Subscription subscription = settings.getSubscription();
        Assert.assertEquals(subscription.getGroup(),
            apache.rocketmq.v2.Resource.newBuilder().setName(FAKE_CONSUMER_GROUP_0).build());
        Assert.assertFalse(subscription.getFifo());
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
        final PushSubscriptionSettings pushSubscriptionSettings = new PushSubscriptionSettings(clientId,
            fakeEndpoints(), groupResource, requestTimeout, subscriptionExpression);
        final Settings settings = pushSubscriptionSettings.toProtobuf();
        Assert.assertEquals(settings.getClientType(), ClientType.PUSH_CONSUMER);
        Assert.assertEquals(settings.getRequestTimeout(), Durations.fromNanos(requestTimeout.toNanos()));
        Assert.assertTrue(settings.hasSubscription());
        final Subscription subscription = settings.getSubscription();
        Assert.assertEquals(subscription.getGroup(),
            apache.rocketmq.v2.Resource.newBuilder().setName(FAKE_CONSUMER_GROUP_0).build());
        Assert.assertFalse(subscription.getFifo());
        final List<SubscriptionEntry> subscriptionsList = subscription.getSubscriptionsList();
        Assert.assertEquals(subscriptionsList.size(), 1);
        final SubscriptionEntry subscriptionEntry = subscriptionsList.get(0);
        Assert.assertEquals(subscriptionEntry.getExpression().getType(), FilterType.SQL);
        Assert.assertEquals(subscriptionEntry.getTopic(),
            apache.rocketmq.v2.Resource.newBuilder().setName(FAKE_TOPIC_0).build());
    }

    @Test
    public void testSync() {
        com.google.protobuf.Duration duration0 = Durations.fromSeconds(1);
        com.google.protobuf.Duration duration1 = Durations.fromSeconds(2);
        com.google.protobuf.Duration duration2 = Durations.fromSeconds(3);
        List<com.google.protobuf.Duration> durations = new ArrayList<>();
        durations.add(duration0);
        durations.add(duration1);
        durations.add(duration2);
        CustomizedBackoff customizedBackoff = CustomizedBackoff.newBuilder().addAllNext(durations).build();
        apache.rocketmq.v2.RetryPolicy retryPolicy = apache.rocketmq.v2.RetryPolicy.newBuilder()
            .setCustomizedBackoff(customizedBackoff).setMaxAttempts(3).build();
        boolean fifo = true;
        int receiveBatchSize = 96;
        com.google.protobuf.Duration longPollingTimeout = Durations.fromSeconds(60);
        Subscription subscription = Subscription.newBuilder().setFifo(fifo).setReceiveBatchSize(receiveBatchSize)
            .setLongPollingTimeout(longPollingTimeout).build();
        Settings settings = Settings.newBuilder().setSubscription(subscription).setBackoffPolicy(retryPolicy).build();
        Resource groupResource = new Resource(FAKE_CONSUMER_GROUP_0);
        ClientId clientId = new ClientId();
        Map<String, FilterExpression> subscriptionExpression = new HashMap<>();
        subscriptionExpression.put(FAKE_TOPIC_0, new FilterExpression("(a > 10 AND a < 100) OR (b IS NOT NULL AND "
            + "b=TRUE)", FilterExpressionType.SQL92));
        final Duration requestTimeout = Duration.ofSeconds(3);
        final PushSubscriptionSettings pushSubscriptionSettings = new PushSubscriptionSettings(clientId,
            fakeEndpoints(), groupResource, requestTimeout, subscriptionExpression);
        pushSubscriptionSettings.sync(settings);
    }
}