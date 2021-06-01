package org.apache.rocketmq.client.route;

import apache.rocketmq.v1.Address;
import apache.rocketmq.v1.Endpoints;
import java.util.ArrayList;
import java.util.List;
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
    private final String target;
    private final List<String> endpoints;

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

        final Endpoints endpoints = partition.getBroker().getEndpoints();
        final apache.rocketmq.v1.Schema schema = endpoints.getSchema();
        StringBuilder targetBuilder = new StringBuilder();
        switch (schema) {
            case IPv4:
                targetBuilder.append(Schema.IPV4.getPrefix());
                break;
            case IPv6:
                targetBuilder.append(Schema.IPV6.getPrefix());
                break;
            case DOMAIN_NAME:
            default:
                targetBuilder.append(Schema.DOMAIN.getPrefix());
                break;
        }
        for (Address address : endpoints.getAddressesList()) {
            targetBuilder.append(address.getHost());
            targetBuilder.append(":");
            targetBuilder.append(address.getPort());
        }
        this.target = targetBuilder.toString();
    }
}
