package org.apache.rocketmq.client.remoting;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode
public class RpcTarget {

    private final Endpoints endpoints;

    private final boolean needHeartbeat;

    @Setter
    @EqualsAndHashCode.Exclude
    private volatile boolean isolated = false;

    public RpcTarget(Endpoints endpoints) {
        this.endpoints = endpoints;
        this.needHeartbeat = false;
    }

    public RpcTarget(Endpoints endpoints, boolean needHeartbeat) {
        this.endpoints = endpoints;
        this.needHeartbeat = needHeartbeat;
    }

    @Override
    public String toString() {
        return endpoints.getFacade();
    }
}
