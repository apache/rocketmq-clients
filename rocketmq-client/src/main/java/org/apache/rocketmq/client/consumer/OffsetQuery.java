package org.apache.rocketmq.client.consumer;

import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.client.consumer.QueryOffsetPolicy;
import org.apache.rocketmq.client.message.MessageQueue;

@Getter
@ToString
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
