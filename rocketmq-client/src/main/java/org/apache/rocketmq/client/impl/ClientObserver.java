package org.apache.rocketmq.client.impl;

import apache.rocketmq.v1.HeartbeatEntry;

public interface ClientObserver {
    String getClientId();

    void doHeartbeat();

    void doHealthCheck();

    void doStats();

    HeartbeatEntry prepareHeartbeatData();
}
