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

import apache.rocketmq.v2.Interest;
import apache.rocketmq.v2.InterestType;
import apache.rocketmq.v2.RetryPolicy;
import com.google.common.base.MoreObjects;
import com.google.protobuf.util.Durations;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.java.impl.ClientType;
import org.apache.rocketmq.client.java.impl.UserAgent;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ExcludeFromJacocoGeneratedReport;
import org.apache.rocketmq.client.java.retry.CustomizedBackoffRetryPolicy;
import org.apache.rocketmq.client.java.retry.ExponentialBackoffRetryPolicy;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LitePushConsumerSettings extends PushSubscriptionSettings {
    private static final Logger log = LoggerFactory.getLogger(LitePushConsumerSettings.class);

    final Resource topic;
    private final Set<String/*lite topic*/> interestSet = ConcurrentHashMap.newKeySet();
    private final AtomicLong updateTime = new AtomicLong(System.currentTimeMillis());

    public LitePushConsumerSettings(
        ClientConfiguration configuration,
        ClientId clientId,
        Endpoints endpoints,
        String topic,
        String group
    ) {
        super(configuration, clientId, ClientType.LITE_PUSH_CONSUMER, endpoints, group);
        this.topic = new Resource(namespace, topic);
    }

    public void addInterest(String liteTopic) {
        if (!interestSet.add(liteTopic)) {
            return;
        }
        updateTime.set(System.currentTimeMillis());
    }

    public void removeInterest(String liteTopic) {
        if (!interestSet.remove(liteTopic)) {
            return;
        }
        updateTime.set(System.currentTimeMillis());
    }

    @Override
    public apache.rocketmq.v2.Settings toProtobuf() {
        Interest interest = Interest.newBuilder()
            .setInterestType(InterestType.ALL)
            .setTopic(topic.toProtobuf())
            .setGroup(group.toProtobuf())
            .addAllInterestSet(interestSet)
            .build();
        return apache.rocketmq.v2.Settings.newBuilder()
            .setAccessPoint(accessPoint.toProtobuf())
            .setClientType(clientType.toProtobuf())
            .setRequestTimeout(Durations.fromNanos(requestTimeout.toNanos()))
            .setUserAgent(UserAgent.INSTANCE.toProtoBuf())
            .setInterest(interest)
            .build();
    }

    /**
     * 服务端处理完 client 发上去的 setting 之后，会写回同步
     * ClientActivity#processAndWriteClientSettings
     */
    @Override
    public void sync(apache.rocketmq.v2.Settings settings) {
        final apache.rocketmq.v2.Settings.PubSubCase pubSubCase = settings.getPubSubCase();
        if (!apache.rocketmq.v2.Settings.PubSubCase.INTEREST.equals(pubSubCase)) {
            log.error("[Bug] Issued settings not match with the client type, clientId={}, pubSubCase={}, "
                + "clientType={}", clientId, pubSubCase, clientType);
            return;
        }
        if (settings.hasBackoffPolicy()) {
            final RetryPolicy backoffPolicy = settings.getBackoffPolicy();
            switch (backoffPolicy.getStrategyCase()) {
                case EXPONENTIAL_BACKOFF:
                    retryPolicy = ExponentialBackoffRetryPolicy.fromProtobuf(backoffPolicy);
                    break;
                case CUSTOMIZED_BACKOFF:
                    retryPolicy = CustomizedBackoffRetryPolicy.fromProtobuf(backoffPolicy);
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized backoff policy strategy.");
            }
        }
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("clientId", clientId)
            .add("clientType", clientType)
            .add("accessPoint", accessPoint)
            .add("requestTimeout", requestTimeout)
            .add("topic", topic)
            .add("group", group)
            .add("interestSet", interestSet)
            .toString();
    }
}
