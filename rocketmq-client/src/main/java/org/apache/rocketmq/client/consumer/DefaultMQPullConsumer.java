package org.apache.rocketmq.client.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.impl.ClientConfig;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.message.MessageQueue;

@Getter
@Setter
public class DefaultMQPullConsumer extends ClientConfig {
    private final DefaultMQPullConsumerImpl impl;

    private long consumerPullTimeoutMillis;

    public DefaultMQPullConsumer(final String consumerGroup) {
        super(consumerGroup);
        this.impl = new DefaultMQPullConsumerImpl(this);
    }

    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums) {
        return this.pull(mq, subExpression, offset, maxNums, consumerPullTimeoutMillis);
    }

    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeoutMillis) {
        return this.impl.pull(mq, subExpression, offset, maxNums, timeoutMillis);
    }

    public PullResult pull(MessageQueue mq, PullMessageSelector messageSelector) {
        throw new UnsupportedOperationException();
    }

    public void pull(MessageQueue mq, PullMessageSelector messageSelector, PullCallback callback) {
        throw new UnsupportedOperationException();
    }

    public void pull(MessageQueue mq, PullMessageSelector messageSelector, PullCallback callback, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums) {
        throw new UnsupportedOperationException();
    }

    public void start() {
        this.impl.start();
    }

    public void shutdown() {
        this.impl.shutdown();
    }
}
