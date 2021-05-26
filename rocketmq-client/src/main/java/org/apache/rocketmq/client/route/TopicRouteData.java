package org.apache.rocketmq.client.route;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class TopicRouteData {
    final List<Partition> partitions;

    public TopicRouteData(List<apache.rocketmq.v1.Partition> partitionList) {
        this.partitions = new ArrayList<Partition>();
        for (apache.rocketmq.v1.Partition partition : partitionList) {
            this.partitions.add(new Partition(partition));
        }
    }

    public Set<String> getAllEndpoints() {
        Set<String> endpoints = new HashSet<String>();
        for (Partition partition : partitions) {
            endpoints.addAll(partition.getEndpoints());
        }
        return endpoints;
    }
}
