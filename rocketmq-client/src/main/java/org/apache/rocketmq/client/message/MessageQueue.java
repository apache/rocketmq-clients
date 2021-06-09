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
    private Partition partition = null;

    public MessageQueue(Partition partition) {
        this.topic = partition.getTopicName();
        this.brokerName = partition.getBrokerName();
        this.queueId = partition.getPartitionId();
        this.partition = partition;
    }

    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
    }

    public String simpleName() {
        return topic + "." + brokerName + "." + queueId;
    }
}
