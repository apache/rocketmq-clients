package org.apache.rocketmq.client.message;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.client.route.Partition;

@Getter
@ToString
@EqualsAndHashCode
public class MessageQueue {
    private final String topic;
    private final String brokerName;
    private final int queueId;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final Partition partition;

    public MessageQueue(Partition partition) {
        this.topic = partition.getTopicName();
        this.brokerName = partition.getBrokerName();
        this.queueId = partition.getId();
        this.partition = partition;
    }

    public String simpleName() {
        return topic + "." + brokerName + "." + queueId;
    }
}
