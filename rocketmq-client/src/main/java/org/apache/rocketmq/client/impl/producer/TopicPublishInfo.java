package org.apache.rocketmq.client.impl.producer;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;

@Slf4j
public class TopicPublishInfo {
    private static final ThreadLocal<AtomicInteger> partitionIndex = new ThreadLocal<AtomicInteger>();

    @GuardedBy("partitionsLock")
    private final List<Partition> partitions;
    private final ReadWriteLock partitionsLock;

    public TopicPublishInfo(TopicRouteData topicRouteData) {
        this.partitionsLock = new ReentrantReadWriteLock();
        this.partitions = filterPartition(topicRouteData);
    }

    public static List<Partition> filterPartition(TopicRouteData topicRouteData) {
        List<Partition> partitions = new ArrayList<Partition>();
        for (Partition partition : topicRouteData.getPartitions()) {
            if (!partition.getPermission().isWritable()) {
                continue;
            }
            if (MixAll.MASTER_BROKER_ID != partition.getBrokerId()) {
                continue;
            }
            partitions.add(partition);
        }
        if (partitions.isEmpty()) {
            log.info("No available partition for publishing, topicRouteData={}", topicRouteData);
        }
        return partitions;
    }

    public List<Partition> takePartitions(Set<Endpoints> isolated, int count) throws MQClientException {
        if (null == partitionIndex.get()) {
            partitionIndex.set(new AtomicInteger(RandomUtils.nextInt()));
        }
        int index = partitionIndex.get().getAndIncrement();
        List<Partition> candidatePartitions = new ArrayList<Partition>();
        Set<String> candidateBrokerNames = new HashSet<String>();
        partitionsLock.readLock().lock();
        try {
            if (partitions.isEmpty()) {
                throw new MQClientException(ErrorCode.NO_PERMISSION);
            }
            for (int i = 0; i < partitions.size(); i++) {
                final Partition partition = partitions.get((index++) % partitions.size());
                final String brokerName = partition.getBrokerName();
                if (!isolated.contains(partition.getTarget().getEndpoints())
                    && !candidateBrokerNames.contains(brokerName)) {
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
                    final String brokerName = partition.getBrokerName();
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
                final String brokerName = partition.getBrokerName();
                if (!candidateBrokerNames.contains(brokerName)) {
                    candidateBrokerNames.add(brokerName);
                    candidatePartitions.add(partition);
                }
                if (candidatePartitions.size() >= count) {
                    break;
                }
            }
            return partitions;
        } finally {
            partitionsLock.readLock().unlock();
        }
    }
}
