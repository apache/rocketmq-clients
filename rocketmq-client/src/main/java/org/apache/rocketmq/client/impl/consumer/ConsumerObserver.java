package org.apache.rocketmq.client.impl.consumer;


import apache.rocketmq.v1.HeartbeatEntry;

public interface ConsumerObserver {
    HeartbeatEntry prepareHeartbeatData();

    void scanLoadAssignments();

    void logStats();
}
