package org.apache.rocketmq.client.impl.producer;

import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.proto.ProducerData;

public interface ProducerObserver {
  void onTopicRouteChanged(String topic, TopicRouteData topicRouteData);

  ProducerData prepareHeartbeatData();

  void logStats();
}
