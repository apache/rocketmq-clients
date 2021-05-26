package org.apache.rocketmq.client.impl.producer;

import apache.rocketmq.v1.HeartbeatEntry;
import org.apache.rocketmq.client.route.TopicRouteData;

public interface ProducerObserver {
    void onTopicRouteChanged(String topic, TopicRouteData topicRouteData);

    HeartbeatEntry prepareHeartbeatData();

    void logStats();
}
