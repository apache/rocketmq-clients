package org.apache.rocketmq.client.route;

import javax.annotation.concurrent.Immutable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.rocketmq.client.remoting.Endpoints;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@Immutable
public class Broker {
    private final String name;
    private final int id;
    private final Endpoints endpoints;
}
