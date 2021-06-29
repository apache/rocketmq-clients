package org.apache.rocketmq.client.impl;

import apache.rocketmq.v1.HeartbeatEntry;

public interface ClientObserver {
    HeartbeatEntry prepareHeartbeatData();

    void logStats();
}
