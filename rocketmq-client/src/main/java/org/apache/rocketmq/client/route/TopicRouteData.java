package org.apache.rocketmq.client.route;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.remoting.Endpoints;

@Getter
@EqualsAndHashCode
@ToString
public class TopicRouteData {
    private static final ThreadLocal<AtomicInteger> PARTITION_INDEX_THREAD_LOCAL = new ThreadLocal<AtomicInteger>();

    /**
     * Partitions of topic route
     */
    final List<Partition> partitions;

    final List<MessageQueue> writableMessageQueueList;

    /**
     * Construct topic route by partition list.
     *
     * @param partitionList partition list, should never be empty.
     */
    public TopicRouteData(List<apache.rocketmq.v1.Partition> partitionList) {
        this.partitions = new ArrayList<Partition>();
        for (apache.rocketmq.v1.Partition partition : partitionList) {
            this.partitions.add(new Partition(partition));
        }
        this.writableMessageQueueList = new ArrayList<MessageQueue>();
        for (Partition partition : partitions) {
            if (MixAll.MASTER_BROKER_ID != partition.getBrokerId()) {
                continue;
            }
            if (!partition.getPermission().isWritable()) {
                continue;
            }
            final MessageQueue messageQueue = new MessageQueue(partition);
            writableMessageQueueList.add(messageQueue);
        }
    }

    public Set<Endpoints> getAllEndpoints() {
        Set<Endpoints> endpointsSet = new HashSet<Endpoints>();
        for (Partition partition : partitions) {
            endpointsSet.add(partition.getRpcTarget().getEndpoints());
        }
        return endpointsSet;
    }

    public static int getNextPartitionIndex() {
        AtomicInteger partitionIndex = PARTITION_INDEX_THREAD_LOCAL.get();
        if (null == partitionIndex) {
            partitionIndex = new AtomicInteger(RandomUtils.nextInt());
            PARTITION_INDEX_THREAD_LOCAL.set(partitionIndex);
        }
        return partitionIndex.getAndIncrement();
    }
}
