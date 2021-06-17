package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.message.MessageQueue;

public class DefaultMQPullConsumerImpl {
    private final DefaultMQPullConsumer defaultMQPullConsumer;

    public DefaultMQPullConsumerImpl(DefaultMQPullConsumer defaultMQPullConsumer) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
    }

    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    public void start() {
    }

    public void shutdown() {
    }
}



