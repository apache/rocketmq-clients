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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.LiteSubscriptionAction;
import apache.rocketmq.v2.NotifyUnsubscribeLiteCommand;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SyncLiteSubscriptionRequest;
import apache.rocketmq.v2.SyncLiteSubscriptionResponse;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.OffsetOption;
import org.apache.rocketmq.client.java.exception.LiteSubscriptionQuotaExceededException;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.misc.ProtobufUtils;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiteSubscriptionManager {
    private static final Logger log = LoggerFactory.getLogger(LiteSubscriptionManager.class);

    protected final ConsumerImpl consumerImpl;
    protected final Resource bindTopic;
    protected final Resource group;
    protected final Set<String> liteTopicSet = ConcurrentHashMap.newKeySet();
    // client-side lite subscription quota limit
    protected volatile int liteSubscriptionQuota;
    protected volatile int maxLiteTopicSize = 64;

    LiteSubscriptionManager(ConsumerImpl consumerImpl, Resource bindTopic, Resource group) {
        this.consumerImpl = consumerImpl;
        this.bindTopic = bindTopic;
        this.group = group;
    }

    public void startUp() {
        consumerImpl.getScheduler()
            .scheduleWithFixedDelay(this::syncAllLiteSubscription, 30, 30, TimeUnit.SECONDS);
    }

    public String getBindTopicName() {
        return bindTopic.getName();
    }

    public String getConsumerGroupName() {
        return group.getName();
    }

    public Set<String> getLiteTopicSet() {
        return ImmutableSet.copyOf(liteTopicSet);
    }

    public void sync(apache.rocketmq.v2.Settings settings) {
        if (!settings.hasSubscription()) {
            return;
        }
        Subscription subscription = settings.getSubscription();
        if (subscription.hasLiteSubscriptionQuota()) {
            this.liteSubscriptionQuota = subscription.getLiteSubscriptionQuota();
        }
        if (subscription.hasMaxLiteTopicSize()) {
            this.maxLiteTopicSize = subscription.getMaxLiteTopicSize();
        }
    }

    public void subscribeLite(String liteTopic, OffsetOption offsetOption) throws ClientException {
        consumerImpl.checkRunning();
        if (liteTopicSet.contains(liteTopic)) {
            return;
        }
        validateLiteTopic(liteTopic, maxLiteTopicSize);
        checkLiteSubscriptionQuota(1);
        ListenableFuture<Void> future =
            syncLiteSubscription(LiteSubscriptionAction.PARTIAL_ADD,
                Collections.singleton(liteTopic), offsetOption);
        try {
            consumerImpl.handleClientFuture(future);
        } catch (ClientException e) {
            log.error("Failed to subscribeLite {}", liteTopic, e);
            throw e;
        }
        liteTopicSet.add(liteTopic);
        log.info("SubscribeLite {}, topic={}, group={}, clientId={}",
            liteTopic, getBindTopicName(), getConsumerGroupName(), consumerImpl.getClientId());
    }

    public void unsubscribeLite(String liteTopic) throws ClientException {
        consumerImpl.checkRunning();
        if (!liteTopicSet.contains(liteTopic)) {
            return;
        }
        ListenableFuture<Void> future =
            syncLiteSubscription(LiteSubscriptionAction.PARTIAL_REMOVE,
                Collections.singleton(liteTopic), null);
        try {
            consumerImpl.handleClientFuture(future);
        } catch (ClientException e) {
            log.error("Failed to unsubscribeLite {}", liteTopic, e);
            throw e;
        }
        liteTopicSet.remove(liteTopic);
        log.info("UnsubscribeLite {}, topic={}, group={}, clientId={}",
            liteTopic, getBindTopicName(), getConsumerGroupName(), consumerImpl.getClientId());
    }

    public void syncAllLiteSubscription() {
        try {
            checkLiteSubscriptionQuota(0);
            ListenableFuture<Void> future =
                syncLiteSubscription(LiteSubscriptionAction.COMPLETE_ADD, liteTopicSet, null);
            consumerImpl.handleClientFuture(future);
        } catch (Throwable t) {
            log.error("Schedule syncAllLiteSubscription error, clientId={}", consumerImpl.getClientId(), t);
        }
    }

    ListenableFuture<Void> syncLiteSubscription(
        LiteSubscriptionAction action,
        Collection<String> diff,
        OffsetOption offsetOption
    ) {
        SyncLiteSubscriptionRequest.Builder builder = SyncLiteSubscriptionRequest.newBuilder()
            .setAction(action)
            .setTopic(bindTopic.toProtobuf())
            .setGroup(group.toProtobuf())
            .addAllLiteTopicSet(diff);
        if (offsetOption != null) {
            builder.setOffsetOption(ProtobufUtils.toProtobufOffsetOption(offsetOption));
        }

        final Duration requestTimeout = consumerImpl.getClientConfiguration().getRequestTimeout();
        RpcFuture<SyncLiteSubscriptionRequest, SyncLiteSubscriptionResponse> future = consumerImpl.getClientManager()
            .syncLiteSubscription(consumerImpl.getEndpoints(), builder.build(), requestTimeout);
        return Futures.transformAsync(future, response -> {
            final Status status = response.getStatus();
            StatusChecker.check(status, future);
            return Futures.immediateVoidFuture();
        }, MoreExecutors.directExecutor());
    }

    void onNotifyUnsubscribeLiteCommand(NotifyUnsubscribeLiteCommand command) {
        String liteTopic = command.getLiteTopic();
        log.info("Notify unsubscribe lite liteTopic={} group={} bindTopic={}",
            liteTopic, getConsumerGroupName(), getBindTopicName());
        if (StringUtils.isNotBlank(liteTopic)) {
            liteTopicSet.remove(liteTopic);
        }
    }

    void validateLiteTopic(String liteTopic, int maxLength) {
        if (StringUtils.isBlank(liteTopic)) {
            throw new IllegalArgumentException("liteTopic is blank");
        }
        if (liteTopic.length() > maxLength) {
            String errorMessage = String.format("liteTopic length exceeded max length %d, liteTopic: %s",
                maxLength, liteTopic);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    void checkLiteSubscriptionQuota(int delta)
        throws LiteSubscriptionQuotaExceededException {
        if (liteTopicSet.size() + delta > liteSubscriptionQuota) {
            throw new LiteSubscriptionQuotaExceededException(
                Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED_VALUE,
                null,
                "Lite subscription quota exceeded " + liteSubscriptionQuota);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("bindTopic", bindTopic)
            .add("group", group)
            .add("liteTopicSet", liteTopicSet)
            .add("liteSubscriptionQuota", liteSubscriptionQuota)
            .add("maxLiteTopicSize", maxLiteTopicSize)
            .toString();
    }
}