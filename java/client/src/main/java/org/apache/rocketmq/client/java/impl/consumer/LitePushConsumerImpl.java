package org.apache.rocketmq.client.java.impl.consumer;

import apache.rocketmq.v2.LiteSubscriptionAction;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.SyncLiteSubscriptionRequest;
import apache.rocketmq.v2.SyncLiteSubscriptionResponse;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumer;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.impl.Settings;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LitePushConsumerImpl extends PushConsumerImpl implements LitePushConsumer {
    private static final Logger log = LoggerFactory.getLogger(LitePushConsumerImpl.class);

    private volatile ScheduledFuture<?> syncAllIntersetFuture;
    private final LitePushConsumerSettings litePushConsumerSettings;

    public LitePushConsumerImpl(LitePushConsumerBuilderImpl builder) {
        super(builder.clientConfiguration, builder.consumerGroup, builder.subscriptionExpressions,
            builder.messageListener, builder.maxCacheMessageCount, builder.maxCacheMessageSizeInBytes,
            builder.consumptionThreadCount, builder.enableFifoConsumeAccelerator);
        this.litePushConsumerSettings = new LitePushConsumerSettings(builder.clientConfiguration,
            clientId, endpoints, builder.bindTopic,
            builder.consumerGroup);
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();
        syncAllIntersetFuture = getScheduler().scheduleWithFixedDelay(() -> {
            try {
                syncAllLiteSubscription();
            } catch (Throwable t) {
                log.error("schedule syncAllLiteSubscription error, clientId={}", clientId, t);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    protected void shutDown() throws InterruptedException {
        super.shutDown();
        if (null != syncAllIntersetFuture) {
            syncAllIntersetFuture.cancel(false);
        }
    }

    @Override
    public LitePushConsumer subscribeLite(Collection<String> liteTopics) throws ClientException {
        checkRunning();
        Set<String> addedSet = new HashSet<>();
        for (String liteTopic : liteTopics) {
            if (litePushConsumerSettings.subscribeLite(liteTopic)) {
                addedSet.add(liteTopic);
            }
        }
        if (!addedSet.isEmpty()) {
            ListenableFuture<Void> future = syncLiteSubscription(LiteSubscriptionAction.INCREMENTAL_ADD, addedSet);
            handleClientFuture(future);
        }
        return this;
    }

    @Override
    public void subscribeLite(String liteTopic) throws ClientException {
        checkRunning();
        if (litePushConsumerSettings.subscribeLite(liteTopic)) {
            ListenableFuture<Void> future =
                syncLiteSubscription(LiteSubscriptionAction.INCREMENTAL_ADD, Collections.singleton(liteTopic));
            handleClientFuture(future);
        }
    }

    @Override
    public LitePushConsumer unsubscribeLite(String liteTopic) throws ClientException {
        checkRunning();
        if (litePushConsumerSettings.unsubscribeLite(liteTopic)) {
            ListenableFuture<Void> future = syncLiteSubscription(LiteSubscriptionAction.INCREMENTAL_REMOVE,
                Collections.singleton(liteTopic));
            handleClientFuture(future);
        }
        return this;
    }

    private void syncAllLiteSubscription() throws ClientException {
        final Set<String> set = litePushConsumerSettings.getInterestSet();
        ListenableFuture<Void> future = syncLiteSubscription(LiteSubscriptionAction.ALL_ADD, set);
        handleClientFuture(future);
        log.info("syncAllLiteSubscription: {}", set);
    }

    protected ListenableFuture<Void> syncLiteSubscription(LiteSubscriptionAction action, Collection<String> diff) {
        SyncLiteSubscriptionRequest request = SyncLiteSubscriptionRequest.newBuilder()
            .setAction(action)
            .setTopic(litePushConsumerSettings.bindTopic.toProtobuf())
            .setGroup(litePushConsumerSettings.group.toProtobuf())
            .addAllSet(diff)
            .build();
        Endpoints endpoints = getEndpoints();
        // todo 这两个有什么区别
        //        final Set<Endpoints> totalRouteEndpoints = getTotalRouteEndpoints();
        return syncLiteSubscription0(endpoints, request);
    }

    protected ListenableFuture<Void> syncLiteSubscription0(Endpoints endpoints, SyncLiteSubscriptionRequest request) {
        final Duration requestTimeout = clientConfiguration.getRequestTimeout();
        RpcFuture<SyncLiteSubscriptionRequest, SyncLiteSubscriptionResponse> future =
            this.getClientManager().syncLiteSubscription(endpoints, request, requestTimeout);

        return Futures.transformAsync(future, response -> {
            final Status status = response.getStatus();
            StatusChecker.check(status, future);
            return Futures.immediateVoidFuture();
        }, MoreExecutors.directExecutor());
    }

    @Override
    public Settings getSettings() {
        return litePushConsumerSettings;
    }

    @Override
    public PushSubscriptionSettings getPushConsumerSettings() {
        return litePushConsumerSettings;
    }

    @Override
    ReceiveMessageRequest wrapReceiveMessageRequest(int batchSize, MessageQueueImpl mq,
        FilterExpression filterExpression, Duration longPollingTimeout, String attemptId) {
        attemptId = null == attemptId ? UUID.randomUUID().toString() : attemptId;
        return ReceiveMessageRequest.newBuilder()
            .setGroup(getProtobufGroup())
            .setMessageQueue(mq.toProtobuf())
            // .setFilterExpression(wrapFilterExpression(filterExpression))
            .setLongPollingTimeout(Durations.fromNanos(longPollingTimeout.toNanos()))
            .setBatchSize(batchSize)
            .setAutoRenew(true)
            .setAttemptId(attemptId)
            .build();
    }

    private void checkRunning() {
        if (!this.isRunning()) {
            log.error("lite push consumer not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("lite push consumer not running");
        }
    }

}
