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

import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import com.google.common.base.MoreObjects;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.java.impl.ClientType;
import org.apache.rocketmq.client.java.impl.Settings;
import org.apache.rocketmq.client.java.impl.UserAgent;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSubscriptionSettings extends Settings {
    private static final Logger log = LoggerFactory.getLogger(SimpleSubscriptionSettings.class);

    private final Resource group;
    private final Duration longPollingTimeout;
    private final Map<String, FilterExpression> subscriptionExpressions;

    public SimpleSubscriptionSettings(ClientId clientId, Endpoints endpoints, Resource group,
        Duration requestTimeout, Duration longPollingTimeout, Map<String, FilterExpression> subscriptionExpression) {
        super(clientId, ClientType.SIMPLE_CONSUMER, endpoints, requestTimeout);
        this.group = group;
        this.subscriptionExpressions = subscriptionExpression;
        this.longPollingTimeout = longPollingTimeout;
    }

    @Override
    public apache.rocketmq.v2.Settings toProtobuf() {
        List<SubscriptionEntry> subscriptionEntries = new ArrayList<>();
        for (Map.Entry<String, FilterExpression> entry : subscriptionExpressions.entrySet()) {
            final FilterExpression filterExpression = entry.getValue();
            apache.rocketmq.v2.Resource topic = apache.rocketmq.v2.Resource.newBuilder()
                .setName(entry.getKey()).build();
            final apache.rocketmq.v2.FilterExpression.Builder expressionBuilder =
                apache.rocketmq.v2.FilterExpression.newBuilder().setExpression(filterExpression.getExpression());
            final FilterExpressionType type = filterExpression.getFilterExpressionType();
            switch (type) {
                case TAG:
                    expressionBuilder.setType(FilterType.TAG);
                    break;
                case SQL92:
                    expressionBuilder.setType(FilterType.SQL);
                    break;
                default:
                    log.warn("[Bug] Unrecognized filter type for simple consumer, type={}", type);
            }
            SubscriptionEntry subscriptionEntry =
                SubscriptionEntry.newBuilder().setTopic(topic).setExpression(expressionBuilder.build()).build();
            subscriptionEntries.add(subscriptionEntry);
        }
        Subscription subscription = Subscription.newBuilder().setGroup(group.toProtobuf())
            .setLongPollingTimeout(Durations.fromNanos(longPollingTimeout.toNanos()))
            .addAllSubscriptions(subscriptionEntries).build();
        return apache.rocketmq.v2.Settings.newBuilder().setAccessPoint(accessPoint.toProtobuf())
            .setClientType(clientType.toProtobuf()).setRequestTimeout(Durations.fromNanos(requestTimeout.toNanos()))
            .setSubscription(subscription).setUserAgent(UserAgent.INSTANCE.toProtoBuf()).build();
    }

    @Override
    public void sync(apache.rocketmq.v2.Settings settings) {
        final apache.rocketmq.v2.Settings.PubSubCase pubSubCase = settings.getPubSubCase();
        if (!apache.rocketmq.v2.Settings.PubSubCase.SUBSCRIPTION.equals(pubSubCase)) {
            log.error("[Bug] Issued settings not match with the client type, clientId={}, pubSubCase={}, "
                + "clientType={}", clientId, pubSubCase, clientType);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("clientId", clientId)
            .add("clientType", clientType)
            .add("accessPoint", accessPoint)
            .add("retryPolicy", retryPolicy)
            .add("requestTimeout", requestTimeout)
            .add("group", group)
            .add("longPollingTimeout", longPollingTimeout)
            .add("subscriptionExpressions", subscriptionExpressions)
            .toString();
    }
}
