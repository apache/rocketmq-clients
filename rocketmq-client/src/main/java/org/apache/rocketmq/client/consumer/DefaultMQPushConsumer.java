package org.apache.rocketmq.client.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.constant.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientConfig;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;

@Getter
@Setter
public class DefaultMQPushConsumer extends ClientConfig {

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     */
    protected final DefaultMQPushConsumerImpl impl;

    private MessageModel messageModel;

    private ConsumeFromWhere consumeFromWhere;

    private int consumeThreadMin = 20;

    private int consumeThreadMax = 64;

    // Only for order message.
    private long suspendCurrentQueueTimeMillis = 1000;

    // TODO: provide default max re-consume times here.
    private int maxReconsumeTimes = 16;

    private int consumeMessageBatchMaxSize = 1;

    private boolean ackMessageAsync = true;

    private boolean nackMessageAsync = true;

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

    // TODO: not allowed to set thead num after start
    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
        if (this.consumeThreadMax < this.consumeThreadMin) {
            this.consumeThreadMin = consumeThreadMax;
        }
    }

    // TODO: not allowed to set thead num after start
    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
        if (this.consumeThreadMax < this.consumeThreadMin) {
            this.consumeThreadMax = consumeThreadMin;
        }
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
