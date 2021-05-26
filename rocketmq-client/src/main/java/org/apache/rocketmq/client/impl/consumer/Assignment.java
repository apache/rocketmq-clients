package org.apache.rocketmq.client.impl.consumer;

import lombok.Data;
import org.apache.rocketmq.client.message.MessageQueue;

@Data
public class Assignment {
    private final MessageQueue messageQueue;
    private final MessageRequestMode messageRequestMode;

    public Assignment(
            MessageQueue messageQueue,
            MessageRequestMode messageRequestMode) {
        this.messageQueue = messageQueue;
        this.messageRequestMode = messageRequestMode;
    }
}
