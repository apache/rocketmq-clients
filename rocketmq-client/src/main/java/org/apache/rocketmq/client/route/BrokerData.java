package org.apache.rocketmq.client.route;

import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class BrokerData {
    private final String cluster;
    private final String brokerName;
    private HashMap<Long /* brokerId */, String /* broker address */> brokerAddressTable;

    public BrokerData(org.apache.rocketmq.proto.BrokerData brokerData) {
        this.cluster = brokerData.getCluster();
        this.brokerName = brokerData.getBrokerName();
        this.brokerAddressTable = new HashMap<Long, String>(brokerData.getAddressesMap());
    }
}
