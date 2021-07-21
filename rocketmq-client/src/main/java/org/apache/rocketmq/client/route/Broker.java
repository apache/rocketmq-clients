package org.apache.rocketmq.client.route;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.client.remoting.RpcTarget;

@AllArgsConstructor
@Getter
public class Broker {
    private final String name;
    private final int id;
    private final RpcTarget target;
}
