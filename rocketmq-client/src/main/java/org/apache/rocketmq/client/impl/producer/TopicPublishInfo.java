package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.BrokerData;
import org.apache.rocketmq.client.route.QueueData;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.PermissionHelper;
import org.apache.rocketmq.utility.UtilAll;

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
    List<MessageQueue> writableMessageQueueList = new ArrayList<MessageQueue>();
    final String orderTopicConfiguration = topicRouteData.getOrderTopicConfiguration();
    if (StringUtils.isNotEmpty(orderTopicConfiguration)) {
      final String[] brokers = orderTopicConfiguration.split(";");
      for (String broker : brokers) {
        final String[] items = broker.split(":");
        if (2 != items.length) {
          continue;
        }
        final String brokerName = items[0];
        final int queueNum = Integer.parseInt(items[1]);
        for (int queueId = 0; queueId < queueNum; queueId++) {
          writableMessageQueueList.add(new MessageQueue(topic, brokerName, queueId));
        }
      }
      return writableMessageQueueList;
    }

    final List<BrokerData> brokerDataList = topicRouteData.getBrokerDataList();
    final List<QueueData> queueDataList = topicRouteData.getQueueDataList();

    final Map<String /* brokerName */, BrokerData> brokerDataMap =
        new HashMap<String, BrokerData>();
    for (BrokerData brokerData : brokerDataList) {
      brokerDataMap.put(brokerData.getBrokerName(), brokerData);
    }
    for (QueueData queueData : queueDataList) {
      final String brokerName = queueData.getBrokerName();
      if (!PermissionHelper.isWriteable(queueData.getPermission())) {
        log.debug(
            "Topic was filtered cause not writable, topic={}, brokerName={}", topic, brokerName);
        continue;
      }
      // For each queue data, we need to ensure that master broker exists.
      final BrokerData brokerData = brokerDataMap.get(brokerName);
      if (null == brokerData) {
        continue;
      }
      final String brokerAddress = brokerData.getBrokerAddressTable().get(MixAll.MASTER_BROKER_ID);
      if (null == brokerAddress) {
        continue;
      }
      final int writeQueueNum = queueData.getWriteQueueNum();
      for (int queueId = 0; queueId < writeQueueNum; queueId++) {
        writableMessageQueueList.add(new MessageQueue(topic, brokerName, queueId));
      }
    }
    return writableMessageQueueList;
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
  public String resolveTarget(String brokerName) {
    for (BrokerData brokerData : topicRouteData.getBrokerDataList()) {
      if (!brokerData.getBrokerName().equals(brokerName)) {
        continue;
      }
      final HashMap<Long, String> brokerAddressTable = brokerData.getBrokerAddressTable();
      String target = brokerAddressTable.get(MixAll.MASTER_BROKER_ID);
      if (null == target) {
        return null;
      }
      target = UtilAll.shiftTargetPort(target, MixAll.SHIFT_PORT);
      return target;
    }
    return null;
  }

  public MessageQueue selectOneMessageQueue(Set<String> isolatedTargets) {
    MessageQueue selectedMessageQueue;
    for (int i = 0; i < messageQueueList.size(); i++) {
      selectedMessageQueue =
          messageQueueList.get(getNextSendQueueIndex() % messageQueueList.size());
      final String target = resolveTarget(selectedMessageQueue.getBrokerName());
      if (!isolatedTargets.contains(target)) {
        return selectedMessageQueue;
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
