package org.apache.rocketmq.client.consumer.listener;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.message.MessageQueue;

@Getter
@Setter
public class ConsumeOrderlyContext {
    private final MessageQueue messageQueue;
    // TODO: enable autoCommit?
    private final boolean autoCommit = true;
    private long suspendCurrentQueueTimeMillis = -1;

    public ConsumeOrderlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }
}
