package org.apache.rocketmq.client.consumer;

import lombok.Getter;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;

@Getter
public class PullMessageQuery {
    private static final long PULL_MESSAGE_TIMEOUT_MILLIS = 3 * 1000;

    private final MessageQueue messageQueue;
    private final FilterExpression filterExpression;
    private final long queueOffset;
    private final int batchSize;
    private final long awaitTimeMillis;
    private final long timeoutMillis;

    // TODO: compare awaitTimeMillis and timeoutMillis here.
    public PullMessageQuery(MessageQueue messageQueue, FilterExpression filterExpression, long queueOffset,
                            int batchSize, long awaitTimeMillis, long timeoutMillis) {
        this.messageQueue = messageQueue;
        this.filterExpression = filterExpression;
        this.queueOffset = queueOffset;
        this.batchSize = batchSize;
        this.awaitTimeMillis = awaitTimeMillis;
        this.timeoutMillis = timeoutMillis;
    }

    // TODO: compare awaitTimeMillis and timeoutMillis here.
    public PullMessageQuery(MessageQueue messageQueue, long queueOffset, int batchSize, long awaitTimeMillis,
                            long timeoutMillis) {
        this.messageQueue = messageQueue;
        this.filterExpression = new FilterExpression();
        this.queueOffset = queueOffset;
        this.batchSize = batchSize;
        this.awaitTimeMillis = awaitTimeMillis;
        this.timeoutMillis = timeoutMillis;
    }

    // TODO: compare awaitTimeMillis and timeoutMillis here.
    public PullMessageQuery(MessageQueue messageQueue, long queueOffset, int batchSize) {
        this.messageQueue = messageQueue;
        this.filterExpression = new FilterExpression();
        this.queueOffset = queueOffset;
        this.batchSize = batchSize;
        this.awaitTimeMillis = MixAll.DEFAULT_POLL_TIME_MILLIS;
        this.timeoutMillis = PULL_MESSAGE_TIMEOUT_MILLIS;
    }

    // TODO: compare awaitTimeMillis and timeoutMillis here.
    public PullMessageQuery(MessageQueue messageQueue, long queueOffset, int batchSize, long awaitTimeMillis) {
        this.messageQueue = messageQueue;
        this.filterExpression = new FilterExpression();
        this.queueOffset = queueOffset;
        this.batchSize = batchSize;
        this.awaitTimeMillis = awaitTimeMillis;
        this.timeoutMillis = PULL_MESSAGE_TIMEOUT_MILLIS;
    }
}
