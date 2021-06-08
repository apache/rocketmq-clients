package org.apache.rocketmq.client.remoting;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class Address {
    private final String host;
    private final int port;

    public Address(apache.rocketmq.v1.Address address) {
        this.host = address.getHost();
        this.port = address.getPort();
    }

    public String getAddress() {
        return host + ":" + port;
    }
}
