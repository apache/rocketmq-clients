package org.apache.rocketmq.client.route;


import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum Schema {
    DOMAIN("dns:"),
    IPV4("ipv4:"),
    IPV6("ipv6:");

    private final String prefix;
}
