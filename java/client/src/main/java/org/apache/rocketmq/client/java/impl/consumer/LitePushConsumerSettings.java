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
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.java.impl.ClientType;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ExcludeFromJacocoGeneratedReport;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LitePushConsumerSettings extends PushSubscriptionSettings {
    private static final Logger log = LoggerFactory.getLogger(LitePushConsumerSettings.class);
    // bindTopic for lite push consumer
    final Resource bindTopic;
    private final Set<String> liteTopicSet = ConcurrentHashMap.newKeySet();
    /**
     * client-side lite subscription quota limit
     */
    private int liteSubscriptionQuota = 1200;
    private int maxLiteTopicSize = 64;

    private final AtomicLong version = new AtomicLong(System.currentTimeMillis());

    public LitePushConsumerSettings(
        LitePushConsumerBuilderImpl builder,
        ClientId clientId,
        Endpoints endpoints
    ) {
        // to keep compatibility, lite push consumer subscribe ALL
        super(builder.clientConfiguration, clientId, ClientType.LITE_PUSH_CONSUMER, endpoints, builder.consumerGroup,
            builder.subscriptionExpressions);
        this.bindTopic = new Resource(namespace, builder.bindTopic);
        // lite push consumer is fifo consumer
        this.fifo = true;
    }

    public boolean containsLiteTopic(String liteTopic) {
        return liteTopicSet.contains(liteTopic);
    }

    public boolean addLiteTopic(String liteTopic) {
        if (liteTopicSet.contains(liteTopic)) {
            return false;
        }
        liteTopicSet.add(liteTopic);
        version.set(System.currentTimeMillis());
        return true;
    }

    public boolean removeLiteTopic(String liteTopic) {
        if (liteTopicSet.remove(liteTopic)) {
            version.set(System.currentTimeMillis());
            return true;
        }
        return false;
    }

    public Set<String> getLiteTopicSet() {
        return ImmutableSet.copyOf(liteTopicSet);
    }

    public int getLiteSubscriptionQuota() {
        return liteSubscriptionQuota;
    }

    public int getMaxLiteTopicSize() {
        return maxLiteTopicSize;
    }

    public int getLiteTopicSetSize() {
        return liteTopicSet.size();
    }

    public long getVersion() {
        return version.get();
    }

    @Override
    public boolean isFifo() {
        // lite push consumer is fifo consumer
        return true;
    }

    @Override
    public void sync(apache.rocketmq.v2.Settings settings) {
        super.sync(settings);
        final Subscription subscription = settings.getSubscription();
        if (subscription.hasLiteSubscriptionQuota()) {
            this.liteSubscriptionQuota = subscription.getLiteSubscriptionQuota();
        }
        if (subscription.hasMaxLiteTopicSize()) {
            this.maxLiteTopicSize = subscription.getMaxLiteTopicSize();
        }
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("clientId", clientId)
            .add("clientType", clientType)
            .add("accessPoint", accessPoint)
            .add("retryPolicy", retryPolicy)
            .add("requestTimeout", requestTimeout)
            .add("group", group)
            .add("receiveBatchSize", receiveBatchSize)
            .add("longPollingTimeout", longPollingTimeout)
            // for lite
            .add("bindTopic", bindTopic)
            .add("liteSubscriptionQuota", liteSubscriptionQuota)
            .add("maxLiteTopicSize", maxLiteTopicSize)
            .add("interestSet", liteTopicSet)
            .add("version", version)
            .toString();
    }
}
