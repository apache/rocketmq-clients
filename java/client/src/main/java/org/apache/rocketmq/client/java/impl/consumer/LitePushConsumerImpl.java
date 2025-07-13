package org.apache.rocketmq.client.java.impl.consumer;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Interest;
import apache.rocketmq.v2.InterestType;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumer;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.java.impl.Settings;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LitePushConsumerImpl extends PushConsumerImpl implements LitePushConsumer {
    private static final Logger log = LoggerFactory.getLogger(LitePushConsumerImpl.class);

    private final LitePushConsumerSettings litePushConsumerSettings;

    private final String bindTopic;

    public LitePushConsumerImpl(LitePushConsumerBuilderImpl builder) {
        super(builder.clientConfiguration, builder.consumerGroup, builder.subscriptionExpressions,
            builder.messageListener, builder.maxCacheMessageCount, builder.maxCacheMessageSizeInBytes,
            builder.consumptionThreadCount, builder.enableFifoConsumeAccelerator);

        this.bindTopic = builder.bindTopic;
        this.litePushConsumerSettings = new LitePushConsumerSettings(builder.clientConfiguration, clientId, endpoints, bindTopic,
            builder.consumerGroup);
    }

    @Override
    public PushConsumer addInterest(String liteTopic) {
        if (!this.isRunning()) {
            log.error("addInterest failed, lite push consumer not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("lite push consumer not running");
        }
        litePushConsumerSettings.addInterest(liteTopic);
        syncInterset(InterestType.ADD, Collections.singleton(liteTopic));
        return this;
    }

    @Override
    public PushConsumer removeInterest(String liteTopic) {
        if (!this.isRunning()) {
            log.error("removeInterest failed, lite push consumer not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("lite push consumer not running");
        }
        litePushConsumerSettings.removeInterest(liteTopic);
        syncInterset(InterestType.REMOVE, Collections.singleton(liteTopic));
        return this;
    }

    void syncInterset(InterestType interestType, Collection<String> diff) {
        Interest interest = Interest.newBuilder()
            .setInterestType(interestType)
            .setTopic(litePushConsumerSettings.topic.toProtobuf())
            .setGroup(litePushConsumerSettings.group.toProtobuf())
            .addAllInterestSet(diff)
            .build();
        final apache.rocketmq.v2.Settings settings = apache.rocketmq.v2.Settings.newBuilder()
            .setClientType(ClientType.LITE_PUSH_CONSUMER)
            .setInterest(interest)
            .build();
        final TelemetryCommand command = TelemetryCommand.newBuilder().setSettings(settings).build();
        final Set<Endpoints> totalRouteEndpoints = getTotalRouteEndpoints();
        for (Endpoints endpoints : totalRouteEndpoints) {
            try {
                telemetry(endpoints, command);
            } catch (Throwable t) {
                log.error("Failed to telemeter settings, clientId={}, endpoints={}", clientId, endpoints, t);
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
}
