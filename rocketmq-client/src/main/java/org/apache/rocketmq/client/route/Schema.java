package org.apache.rocketmq.client.route;


import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum Schema {
    DOMAIN_NAME("dns:"),
    IPv4("ipv4:"),
    IPv6("ipv6:");

    private final String prefix;
}
