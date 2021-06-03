package org.apache.rocketmq.client.remoting;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.route.Schema;

@ToString
@Getter
@EqualsAndHashCode
public class Endpoints {
    private static final String ADDRESS_SEPARATOR = ",";
    private final Schema schema;
    /**
     * URI path for grpc target, e.g:
     * 1. dns:rocketmq.apache.org:8080
     * 2. ipv4:127.0.0.1:10911,127.0.0.2:10912
     * 3. ipv6:1050:0000:0000:0000:0005:0600:300c:326b:10911
     */
    private final String target;
    private final List<Address> addresses;

    public Endpoints(Schema schema, List<Address> addresses) {
        // TODO: polish code here.
        if (Schema.DOMAIN_NAME == schema && addresses.size() > 1) {
            throw new UnsupportedOperationException("Multiple addresses not allowed in domain schema.");
        }
        Preconditions.checkNotNull(addresses);
        if (addresses.isEmpty()) {
            throw new UnsupportedOperationException("No available address");
        }
        // Shuffle addresses for grpc pick-first policy.
        Collections.shuffle(addresses);

        this.schema = schema;
        this.addresses = addresses;
        StringBuilder targetBuilder = new StringBuilder();
        targetBuilder.append(schema.getPrefix());
        for (Address address : addresses) {
            targetBuilder.append(address.getAddress()).append(ADDRESS_SEPARATOR);
        }
        this.target = targetBuilder.substring(0, targetBuilder.length() - 1);
    }
}
