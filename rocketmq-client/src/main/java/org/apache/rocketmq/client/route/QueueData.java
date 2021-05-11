package org.apache.rocketmq.client.route;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class QueueData {
    private final String brokerName;
    private final int readQueueNum;
    private final int writeQueueNum;
    private final int permission;
    private final int topicSystemFlag;

    public QueueData(org.apache.rocketmq.proto.QueueData queueData) {
        this.brokerName = queueData.getBrokerName();
        this.readQueueNum = queueData.getReadQueueNumber();
        this.writeQueueNum = queueData.getWriteQueueNumber();
        this.permission = queueData.getPermission();
        this.topicSystemFlag = queueData.getTopicSystemFlag();
    }
}
