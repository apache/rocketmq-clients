package org.apache.rocketmq.client.impl;

import apache.rocketmq.v1.HeartbeatEntry;
import java.util.Set;
import org.apache.rocketmq.client.remoting.Endpoints;

public interface ClientObserver {
    String getClientId();

    Set<Endpoints> getEndpointsNeedHeartbeat();

    void doHeartbeat();

    HeartbeatEntry prepareHeartbeatData();

    void logStats();
}
