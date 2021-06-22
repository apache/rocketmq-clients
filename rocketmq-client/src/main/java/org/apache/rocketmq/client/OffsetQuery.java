package org.apache.rocketmq.client;

import lombok.Getter;
import org.apache.rocketmq.client.consumer.QueryOffsetPolicy;
import org.apache.rocketmq.client.message.MessageQueue;

@Getter
public class OffsetQuery {
    final MessageQueue messageQueue;
    final QueryOffsetPolicy queryOffsetPolicy;
    final long timePoint;

    public OffsetQuery(MessageQueue messageQueue, QueryOffsetPolicy queryOffsetPolicy, long timePoint) {
        this.messageQueue = messageQueue;
        this.queryOffsetPolicy = queryOffsetPolicy;
        this.timePoint = timePoint;
    }
}
