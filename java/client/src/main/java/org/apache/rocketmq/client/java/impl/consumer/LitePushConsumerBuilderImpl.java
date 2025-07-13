package org.apache.rocketmq.client.java.impl.consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumer;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumerBuilder;
import org.apache.rocketmq.client.apis.consumer.MessageListener;

public class LitePushConsumerBuilderImpl extends PushConsumerBuilderImpl implements LitePushConsumerBuilder {

    String bindTopic = null;

    @Override
    public LitePushConsumerBuilder bindTopic(String topic) {
        checkArgument(StringUtils.isNotBlank(topic), "bindTopic should not be blank");
        this.bindTopic = topic;
        // 复用 PushConsumerImpl 构造函数，为了将 bindTopic 传入 ClientImpl
        subscriptionExpressions.put(topic, new FilterExpression());
        return this;
    }

    @Override
    public LitePushConsumerBuilder setSubscriptionExpressions(Map<String, FilterExpression> subscriptionExpressions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LitePushConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration) {
        return (LitePushConsumerBuilder) super.setClientConfiguration(clientConfiguration);
    }

    @Override
    public LitePushConsumerBuilder setConsumerGroup(String consumerGroup) {
        return (LitePushConsumerBuilder) super.setConsumerGroup(consumerGroup);
    }

    @Override
    public LitePushConsumerBuilder setMessageListener(MessageListener messageListener) {
        return (LitePushConsumerBuilder) super.setMessageListener(messageListener);
    }

    @Override
    public LitePushConsumerBuilder setMaxCacheMessageCount(int maxCachedMessageCount) {
        return (LitePushConsumerBuilder) super.setMaxCacheMessageCount(maxCachedMessageCount);
    }

    @Override
    public LitePushConsumerBuilder setMaxCacheMessageSizeInBytes(int maxCacheMessageSizeInBytes) {
        return (LitePushConsumerBuilder) super.setMaxCacheMessageSizeInBytes(maxCacheMessageSizeInBytes);
    }

    @Override
    public LitePushConsumerBuilder setConsumptionThreadCount(int consumptionThreadCount) {
        return (LitePushConsumerBuilder) super.setConsumptionThreadCount(consumptionThreadCount);
    }

    @Override
    public LitePushConsumerBuilder setEnableFifoConsumeAccelerator(boolean enableFifoConsumeAccelerator) {
        return (LitePushConsumerBuilder) super.setEnableFifoConsumeAccelerator(enableFifoConsumeAccelerator);
    }

    @Override
    public LitePushConsumer build() throws ClientException {
        checkNotNull(clientConfiguration, "clientConfiguration has not been set yet");
        checkNotNull(consumerGroup, "consumerGroup has not been set yet");
        checkNotNull(messageListener, "messageListener has not been set yet");
        checkNotNull(bindTopic, "bindTopic has not been set yet");
        final LitePushConsumerImpl litePushConsumer = new LitePushConsumerImpl(this);
        litePushConsumer.startAsync().awaitRunning();
        return litePushConsumer;
    }

}
