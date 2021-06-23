package org.apache.rocketmq.client.impl.producer;

import apache.rocketmq.v1.HeartbeatEntry;

public interface ProducerObserver {
    HeartbeatEntry prepareHeartbeatData();

    void logStats();
}
