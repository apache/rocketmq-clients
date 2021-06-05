package org.apache.rocketmq.client.consumer.listener;

import org.apache.rocketmq.client.message.MessageQueue;

public class ConsumeConcurrentlyContext {
    private final MessageQueue messageQueue;
    private final int ackIndex = Integer.MAX_VALUE;

    public ConsumeConcurrentlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }
}
