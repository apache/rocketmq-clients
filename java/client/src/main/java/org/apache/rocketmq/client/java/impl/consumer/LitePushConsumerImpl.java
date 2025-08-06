package org.apache.rocketmq.client.java.impl.consumer;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.Interest;
import apache.rocketmq.v2.InterestType;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumer;
import org.apache.rocketmq.client.java.impl.Settings;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LitePushConsumerImpl extends PushConsumerImpl implements LitePushConsumer {
    private static final Logger log = LoggerFactory.getLogger(LitePushConsumerImpl.class);

    private volatile ScheduledFuture<?> syncAllIntersetFuture;

    private final LitePushConsumerSettings litePushConsumerSettings;

    private final String bindTopic;

    public LitePushConsumerImpl(LitePushConsumerBuilderImpl builder) {
        super(builder.clientConfiguration, builder.consumerGroup, builder.subscriptionExpressions,
            builder.messageListener, builder.maxCacheMessageCount, builder.maxCacheMessageSizeInBytes,
            builder.consumptionThreadCount, builder.enableFifoConsumeAccelerator);

        this.bindTopic = builder.bindTopic;
        this.litePushConsumerSettings = new LitePushConsumerSettings(builder.clientConfiguration, clientId, endpoints
            , bindTopic,
            builder.consumerGroup);
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();
        syncAllIntersetFuture = getScheduler().scheduleWithFixedDelay(() -> {
            try {
                syncAllInterset();
            } catch (Throwable t) {
                log.error("Exception raised during syncAllInterset, clientId={}", clientId, t);
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
    public LitePushConsumer subscribeLite(Collection<String> liteTopics) {
        if (!this.isRunning()) {
            log.error("subscribeLite failed, lite push consumer not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("lite push consumer not running");
        }
        Set<String> addedSet = new HashSet<>();
        for (String liteTopic : liteTopics) {
            if (litePushConsumerSettings.subscribeLite(liteTopic)) {
                addedSet.add(liteTopic);
            }
        }
        syncInterset(InterestType.INCREMENTAL_ADD, addedSet);
        return this;
    }

    @Override
    public LitePushConsumer subscribeLite(String liteTopic) {
        if (!this.isRunning()) {
            log.error("subscribeLite failed, lite push consumer not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("lite push consumer not running");
        }
        if (litePushConsumerSettings.subscribeLite(liteTopic)) {
            syncInterset(InterestType.INCREMENTAL_ADD, Collections.singleton(liteTopic));
        }
        return this;
    }

    @Override
    public LitePushConsumer unsubscribeLite(String liteTopic) {
        if (!this.isRunning()) {
            log.error("unsubscribeLite failed, lite push consumer not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("lite push consumer not running");
        }
        if (litePushConsumerSettings.unsubscribeLite(liteTopic)) {
            syncInterset(InterestType.INCREMENTAL_REMOVE, Collections.singleton(liteTopic));
        }
        return this;
    }

    private void syncAllInterset() {
        final Set<String> set = litePushConsumerSettings.getInterestSet();
        syncInterset(InterestType.ALL_ADD, set);
        log.info("syncAllInterset: {}", set);
    }

    private void syncInterset(InterestType interestType, Collection<String> diff) {
        Interest interest = Interest.newBuilder()
            .setInterestType(interestType)
            .setTopic(litePushConsumerSettings.bindTopic.toProtobuf())
            .setGroup(litePushConsumerSettings.group.toProtobuf())
            .addAllInterestSet(diff)
            .setVersion(litePushConsumerSettings.getVersion())
            .build();
        final TelemetryCommand command = TelemetryCommand
            .newBuilder()
            .setInterest(interest)
            .build();
        final Set<Endpoints> totalRouteEndpoints = getTotalRouteEndpoints();
        for (Endpoints endpoints : totalRouteEndpoints) {
            try {
                telemetry(endpoints, command);
            } catch (Throwable t) {
                log.error("syncInterset failed, clientId={}, endpoints={}, command={}"
                    , clientId, endpoints, command, t);
            }
        }
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

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        return HeartbeatRequest.newBuilder().setGroup(getProtobufGroup())
            .setClientType(ClientType.LITE_PUSH_CONSUMER).build();
    }

}
