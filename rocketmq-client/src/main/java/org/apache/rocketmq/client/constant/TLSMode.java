package org.apache.rocketmq.client.constant;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum TLSMode {
    /**
     * SSL is not supported; any incoming SSL handshake will be rejected, causing connection closed.
     */
    DISABLED("disabled"),
    /**
     * SSL is optional, aka, server in this mode can serve client connections with or without SSL.
     */
    PERMISSIVE("permissive"),
    /**
     * SSL is required, aka, non SSL connection will be rejected.
     */
    ENFORCING("enforcing");

    private static final Map<String, TLSMode> LOOK_UP = new HashMap<String, TLSMode>();

    static {
        for (TLSMode i : EnumSet.allOf(TLSMode.class)) {
            LOOK_UP.put(i.getMode(), i);
        }
    }

    private final String mode;

    public static TLSMode parse(String mode) {
        return LOOK_UP.get(mode);
    }
}
