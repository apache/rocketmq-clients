package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;

@Slf4j
@Getter
public class TopicPublishInfo {
    private final boolean isOrderTopic;
    private final String topic;
    private List<MessageQueue> messageQueueList;
    private TopicRouteData topicRouteData;
    private final ThreadLocal<Integer> sendQueueIndex;

    public TopicPublishInfo(String topic, TopicRouteData topicRouteData) {
        this.isOrderTopic = false;
        this.topic = topic;

        this.refreshTopicRoute(topicRouteData);

        this.sendQueueIndex = new ThreadLocal<Integer>();
        this.sendQueueIndex.set(Math.abs(new Random().nextInt()));
    }

    void refreshTopicRoute(TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
        this.messageQueueList = filterWritableMessageQueueList(topicRouteData);
    }

    private List<MessageQueue> filterWritableMessageQueueList(TopicRouteData topicRouteData) {
        Set<MessageQueue> writableMessageQueueList = new HashSet<MessageQueue>();

        final List<Partition> partitions = topicRouteData.getPartitions();
        for (Partition partition : partitions) {
            if (MixAll.MASTER_BROKER_ID != partition.getBrokerId()) {
                continue;
            }
            if (!partition.getPermission().isWritable()) {
                continue;
            }
            final MessageQueue messageQueue = new MessageQueue(partition.getTopicName(), partition.getBrokerName(),
                                                               partition.getPartitionId());
            writableMessageQueueList.add(messageQueue);
        }
        return new ArrayList<MessageQueue>(writableMessageQueueList);
    }

    private int getNextSendQueueIndex() {
        Integer index = sendQueueIndex.get();
        if (null == index) {
            index = -1;
        }
        index += 1;
        index = Math.abs(index);
        sendQueueIndex.set(index);
        return index;
    }

    /**
     * Resolve target according provided message queue.
     *
     * @param brokerName provided broker name.
     * @return target address.
     */
    public String resolveTarget(String brokerName) throws MQClientException {
        final List<Partition> partitions = topicRouteData.getPartitions();
        for (Partition partition : partitions) {
            if (!brokerName.equals(partition.getBrokerName())) {
                continue;
            }
            if (MixAll.MASTER_BROKER_ID != partition.getBrokerId()) {
                continue;
            }
            return partition.selectEndpoint();
        }
        log.error("Failed to resolve target address from brokerName=" + brokerName);
        throw new MQClientException("Failed to resolve target");
    }

    public MessageQueue selectOneMessageQueue(Set<String> isolatedTargets) throws MQClientException {
        MessageQueue selectedMessageQueue;
        if (messageQueueList.isEmpty()) {
            throw new MQClientException("No writable message queue is available");
        }
        for (int i = 0; i < messageQueueList.size(); i++) {
            selectedMessageQueue =
                    messageQueueList.get(getNextSendQueueIndex() % messageQueueList.size());
            final String brokerName = selectedMessageQueue.getBrokerName();
            try {
                String target = resolveTarget(brokerName);
                if (!isolatedTargets.contains(target)) {
                    return selectedMessageQueue;
                }
            } catch (MQClientException e) {
                log.error("Exception occurs while selecting mq for sending, brokerName={}", brokerName);
            }
        }
        selectedMessageQueue = messageQueueList.get(getNextSendQueueIndex() % messageQueueList.size());
        log.warn(
                "No available target right now, selectedMessageQueue={}, isolatedTargets={}",
                selectedMessageQueue,
                isolatedTargets);
        return selectedMessageQueue;
    }
}
