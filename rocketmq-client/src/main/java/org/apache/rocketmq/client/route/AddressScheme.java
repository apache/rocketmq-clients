package org.apache.rocketmq.client.route;


import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum AddressScheme {
    DOMAIN_NAME("dns:"),
    IPv4("ipv4:"),
    IPv6("ipv6:");

    public static final String SCHEMA_SEPARATOR = ":";
    private final String prefix;
}
