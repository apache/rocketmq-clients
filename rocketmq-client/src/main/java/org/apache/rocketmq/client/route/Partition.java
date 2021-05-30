package org.apache.rocketmq.client.route;

import apache.rocketmq.v1.Endpoint;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.client.constant.Permission;

@Getter
@ToString
@EqualsAndHashCode
public class Partition {
    private final String topicArn;
    private final String topicName;
    private final int partitionId;
    private final Permission permission;

    private final String brokerName;
    private final int brokerId;
    private final List<String> endpoints;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final ThreadLocal<Integer> endpointIndex;

    public Partition(apache.rocketmq.v1.Partition partition) {

        this.topicArn = partition.getTopic().getArn();
        this.topicName = partition.getTopic().getName();
        this.partitionId = partition.getId();
        final apache.rocketmq.v1.Permission perm = partition.getPermission();
        switch (perm) {
            case READ:
                this.permission = Permission.READ;
                break;
            case WRITE:
                this.permission = Permission.WRITE;
                break;
            case READ_WRITE:
                this.permission = Permission.READ_WRITE;
                break;
            case NONE:
            default:
                this.permission = Permission.NONE;
                break;
        }

        this.brokerName = partition.getBroker().getName();
        this.brokerId = partition.getBroker().getId();
        this.endpoints = new ArrayList<String>();

        Set<String> endpointSet = new HashSet<String>();
        final List<Endpoint> endpointList = partition.getBroker().getEndpointsList();
        for (Endpoint endpoint : endpointList) {
            final String address = endpoint.getAddress();
            final int port = endpoint.getPort();
            endpointSet.add(address + ":" + port);
        }
        this.endpoints.addAll(endpointSet);
        this.endpointIndex = new ThreadLocal<Integer>();
        this.endpointIndex.set(Math.abs(new Random().nextInt()));
    }

    private int getNextEndpointIndex() {
        Integer index = endpointIndex.get();
        if (null == index) {
            index = -1;
        }
        index += 1;
        index = Math.abs(index);
        endpointIndex.set(index);
        return index;
    }

    public String selectEndpoint() {
        return endpoints.get(getNextEndpointIndex() % endpoints.size());
    }
}
