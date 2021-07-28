package org.apache.rocketmq.client.route;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.concurrent.Immutable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.client.remoting.Endpoints;

@Getter
@EqualsAndHashCode
@ToString
@Immutable
public class TopicRouteData {
    /**
     * Partitions of topic route
     */
    final List<Partition> partitions;


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
    }

    public Set<Endpoints> allEndpoints() {
        Set<Endpoints> endpointsSet = new HashSet<Endpoints>();
        for (Partition partition : partitions) {
            endpointsSet.add(partition.getBroker().getEndpoints());
        }
        return endpointsSet;
    }
}
