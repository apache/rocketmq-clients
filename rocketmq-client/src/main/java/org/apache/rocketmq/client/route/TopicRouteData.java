package org.apache.rocketmq.client.route;

import com.google.protobuf.ProtocolStringList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class TopicRouteData {
  private final String orderTopicConfiguration;
  private final List<QueueData> queueDataList;
  private final List<BrokerData> brokerDataList;
  private Map<String /* broker address */, List<String> /* Filter Server */> filterServerTable;

  public TopicRouteData(org.apache.rocketmq.proto.TopicRouteData topicRouteData) {
    this.orderTopicConfiguration = topicRouteData.getOrderTopicConfiguration();
    this.queueDataList = new ArrayList<>();
    for (org.apache.rocketmq.proto.QueueData queueData : topicRouteData.getQueueDataList()) {
      this.queueDataList.add(new QueueData(queueData));
    }
    this.brokerDataList = new ArrayList<>();
    for (org.apache.rocketmq.proto.BrokerData brokerData : topicRouteData.getBrokerDataList()) {
      this.brokerDataList.add(new BrokerData(brokerData));
    }
    topicRouteData
        .getFilterServerAddressesMap()
        .forEach(
            (brokerAddress, filterServerAddress) -> {
              List<String> addressList = new ArrayList<>();
              filterServerTable.put(brokerAddress, addressList);
              ProtocolStringList list = filterServerAddress.getAddressList();
              addressList.addAll(list);
            });
  }
}
