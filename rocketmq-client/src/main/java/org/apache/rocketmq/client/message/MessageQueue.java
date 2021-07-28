package org.apache.rocketmq.client.message;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.rocketmq.client.route.Partition;

@Getter
@EqualsAndHashCode
public class MessageQueue {
    private final String topic;
    private final String brokerName;
    private final int queueId;

    private final Partition partition;

    public MessageQueue(Partition partition) {
        this.topic = partition.getTopicResource().getName();
        this.brokerName = partition.getBroker().getName();
        this.queueId = partition.getId();
        this.partition = partition;
    }

    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
        this.partition = null;
    }

    @Override
    public String toString() {
        return topic + "." + brokerName + "." + queueId;
    }
}
