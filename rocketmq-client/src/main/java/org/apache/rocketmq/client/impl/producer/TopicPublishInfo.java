package org.apache.rocketmq.client.impl.producer;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.route.Broker;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;

@Slf4j
public class TopicPublishInfo {
    private static final ThreadLocal<AtomicInteger> partitionIndex = new ThreadLocal<AtomicInteger>();

    private final List<Partition> partitions;

    public TopicPublishInfo(TopicRouteData topicRouteData) {
        this.partitions = filterPartition(topicRouteData);
    }

    public List<MessageQueue> getMessageQueues() {
        List<MessageQueue> messageQueues = new ArrayList<MessageQueue>();
        for (Partition partition : partitions) {
            messageQueues.add(new MessageQueue(partition));
        }
        return messageQueues;
    }

    @VisibleForTesting
    public static List<Partition> filterPartition(TopicRouteData topicRouteData) {
        List<Partition> partitions = new ArrayList<Partition>();
        for (Partition partition : topicRouteData.getPartitions()) {
            if (!partition.getPermission().isWritable()) {
                continue;
            }
            if (MixAll.MASTER_BROKER_ID != partition.getBroker().getId()) {
                continue;
            }
            partitions.add(partition);
        }
        if (partitions.isEmpty()) {
            log.warn("No available partition, topicRouteData={}", topicRouteData);
        }
        return partitions;
    }

    public boolean isEmpty() {
        return partitions.isEmpty();
    }

    public List<Partition> takePartitions(Set<Endpoints> isolated, int count) throws MQClientException {
        if (null == partitionIndex.get()) {
            partitionIndex.set(new AtomicInteger(RandomUtils.nextInt()));
        }
        int index = partitionIndex.get().getAndIncrement();
        List<Partition> candidatePartitions = new ArrayList<Partition>();
        Set<String> candidateBrokerNames = new HashSet<String>();
        if (partitions.isEmpty()) {
            throw new MQClientException(ErrorCode.NO_PERMISSION);
        }
        for (int i = 0; i < partitions.size(); i++) {
            final Partition partition = partitions.get((index++) % partitions.size());
            final Broker broker = partition.getBroker();
            final String brokerName = broker.getName();
            if (!isolated.contains(broker.getEndpoints()) && !candidateBrokerNames.contains(brokerName)) {
                candidateBrokerNames.add(brokerName);
                candidatePartitions.add(partition);
            }
            if (candidatePartitions.size() >= count) {
                return candidatePartitions;
            }
        }
        // If all endpoints are isolated.
        if (candidatePartitions.isEmpty()) {
            for (int i = 0; i < partitions.size(); i++) {
                final Partition partition = partitions.get((index++) % partitions.size());
                final Broker broker = partition.getBroker();
                final String brokerName = broker.getName();
                if (!candidateBrokerNames.contains(brokerName)) {
                    candidateBrokerNames.add(brokerName);
                    candidatePartitions.add(partition);
                }
                if (candidatePartitions.size() >= count) {
                    break;
                }
            }
            return candidatePartitions;
        }
        // If no enough candidates, pick up partition from isolated partition.
        for (int i = 0; i < partitions.size(); i++) {
            final Partition partition = partitions.get((index++) % partitions.size());
            final Broker broker = partition.getBroker();
            final String brokerName = broker.getName();
            if (!candidateBrokerNames.contains(brokerName)) {
                candidateBrokerNames.add(brokerName);
                candidatePartitions.add(partition);
            }
            if (candidatePartitions.size() >= count) {
                break;
            }
        }
        return partitions;
    }
}
