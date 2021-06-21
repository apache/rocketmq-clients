package org.apache.rocketmq.client.consumer;

import lombok.Getter;
import org.apache.rocketmq.client.message.MessageQueue;

@Getter
public class PullMessageQuery {
    private final MessageQueue messageQueue;
    private final long queueOffset;
    private final int batchSize;
    private final long awaitTimeMillis;
    private final long timeoutMillis;

    public PullMessageQuery(MessageQueue messageQueue, long queueOffset, int batchSize, long awaitTimeMillis,
                            long timeoutMillis) {
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
        this.batchSize = batchSize;
        this.awaitTimeMillis = awaitTimeMillis;
        this.timeoutMillis = timeoutMillis;
    }
}
