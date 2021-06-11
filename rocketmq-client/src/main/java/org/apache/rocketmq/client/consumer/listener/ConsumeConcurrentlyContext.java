package org.apache.rocketmq.client.consumer.listener;

import lombok.Getter;
import org.apache.rocketmq.client.message.MessageQueue;

@Getter
public class ConsumeConcurrentlyContext {
    private final MessageQueue messageQueue;
    // TODO:
    private final int ackIndex = Integer.MAX_VALUE;

    public ConsumeConcurrentlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }
}
