package org.apache.rocketmq.client.apis.consumer;

import java.util.Map;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;

public interface LitePushConsumerBuilder extends PushConsumerBuilder {
    LitePushConsumerBuilder bindTopic(String topic);

    @Override
    LitePushConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration);

    @Override
    LitePushConsumerBuilder setConsumerGroup(String consumerGroup);

    @Override
    LitePushConsumerBuilder setSubscriptionExpressions(Map<String, FilterExpression> subscriptionExpressions);

    @Override
    LitePushConsumerBuilder setMessageListener(MessageListener listener);

    @Override
    LitePushConsumerBuilder setMaxCacheMessageCount(int count);

    @Override
    LitePushConsumerBuilder setMaxCacheMessageSizeInBytes(int bytes);

    @Override
    LitePushConsumerBuilder setConsumptionThreadCount(int count);

    @Override
    LitePushConsumerBuilder setEnableFifoConsumeAccelerator(boolean enableFifoConsumeAccelerator);

    @Override
    LitePushConsumer build() throws ClientException;
}
