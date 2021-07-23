package org.apache.rocketmq.client.impl;

import apache.rocketmq.v1.HeartbeatEntry;

public interface ClientObserver {
    String getClientId();

    void doHeartbeat();

    HeartbeatEntry prepareHeartbeatData();

    void doHealthCheck();

    void logStats();
}
