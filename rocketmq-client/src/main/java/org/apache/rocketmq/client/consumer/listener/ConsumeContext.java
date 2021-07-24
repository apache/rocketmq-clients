package org.apache.rocketmq.client.consumer.listener;

import lombok.Getter;
import org.apache.rocketmq.client.message.MessageQueue;

@Getter
public class ConsumeContext {
    private final MessageQueue messageQueue;
    // TODO:
    private final int ackIndex = Integer.MAX_VALUE;

    public ConsumeContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }
}
