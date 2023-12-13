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

import apache.rocketmq.v2.Subscription;
import com.google.common.base.MoreObjects;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import org.apache.rocketmq.client.java.impl.ClientType;
import org.apache.rocketmq.client.java.impl.Settings;
import org.apache.rocketmq.client.java.impl.UserAgent;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ExcludeFromJacocoGeneratedReport;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullSubscriptionSettings extends Settings {
    private static final Logger log = LoggerFactory.getLogger(PullSubscriptionSettings.class);
    private final Resource group;

    private volatile Duration longPollingTimeout = Duration.ofSeconds(30);

    public PullSubscriptionSettings(ClientId clientId, Endpoints endpoints, Resource group, Duration requestTimeout) {
        super(clientId, ClientType.PULL_CONSUMER, endpoints, requestTimeout);
        this.group = group;
    }

    public Duration getLongPollingTimeout() {
        return longPollingTimeout;
    }

    public apache.rocketmq.v2.Settings toProtobuf() {
        Subscription subscription = Subscription.newBuilder().setGroup(group.toProtobuf()).build();
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
            return;
        }
        final Subscription subscription = settings.getSubscription();
        this.longPollingTimeout = Duration.ofNanos(Durations.toNanos(subscription.getLongPollingTimeout()));
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("group", group)
            .add("longPollingTimeout", longPollingTimeout)
            .add("clientId", clientId)
            .add("clientType", clientType)
            .add("accessPoint", accessPoint)
            .add("retryPolicy", retryPolicy)
            .add("requestTimeout", requestTimeout)
            .toString();
    }
}
