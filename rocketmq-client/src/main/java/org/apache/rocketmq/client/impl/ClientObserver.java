package org.apache.rocketmq.client.impl;

public interface ClientObserver {
    String getClientId();

    void doHeartbeat();

    void doHealthCheck();

    void doStats();
}
