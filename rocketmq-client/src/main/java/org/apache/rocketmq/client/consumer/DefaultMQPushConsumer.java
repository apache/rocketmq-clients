package org.apache.rocketmq.client.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.constant.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientConfig;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;

public class DefaultMQPushConsumer extends ClientConfig {

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     */
    protected final DefaultMQPushConsumerImpl impl;

    @Getter
    @Setter
    private MessageModel messageModel;

    @Getter
    @Setter
    private ConsumeFromWhere consumeFromWhere;

    @Getter
    private int consumeThreadMin = 20;

    @Getter
    private int consumeThreadMax = 64;

    // TODO
    @Getter
    @Setter
    private int maxReconsumeTimes = -1;

    @Getter
    @Setter
    private int consumeMessageBatchMaxSize = 1;

    @Getter
    @Setter
    private boolean ackMessageAsync = true;

    public DefaultMQPushConsumer(final String consumerGroup) {
        super(consumerGroup);
        this.consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
        this.impl = new DefaultMQPushConsumerImpl(this);
    }

    public void setConsumerGroup(String consumerGroup) {
        if (impl.hasBeenStarted()) {
            throw new RuntimeException("Please set consumerGroup before consumer started.");
        }
        setGroupName(consumerGroup);
    }

    public String getConsumerGroup() {
        return this.getGroupName();
    }

    public void start() throws MQClientException {
        this.impl.start();
    }

    public void shutdown() throws MQClientException {
        this.impl.shutdown();
    }

    public void registerMessageListener(MessageListenerConcurrently messageListenerConcurrently) {
        this.impl.registerMessageListener(messageListenerConcurrently);
    }

    public void registerMessageListener(MessageListenerOrderly messageListenerOrderly) {
        this.impl.registerMessageListener(messageListenerOrderly);
    }

    public void subscribe(String topic, String subscribeExpression) throws MQClientException {
        this.impl.subscribe(topic, subscribeExpression);
    }


    public void unsubscribe(String topic) {
        this.impl.unsubscribe(topic);
    }
}
