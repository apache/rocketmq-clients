package org.apache.rocketmq.client.remoting;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.client.route.Schema;

@ToString
@Getter
@EqualsAndHashCode
public class Endpoints {
    private final Schema schema;
    /**
     * URI path for grpc target, e.g:
     * 1. dns:rocketmq.apache.org:8080
     * 2. ipv4:127.0.0.1:10911
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

        this.schema = schema;
        this.addresses = addresses;

        StringBuilder targetBuilder = new StringBuilder();
        targetBuilder.append(schema.getPrefix());
        for (Address address : addresses) {
            targetBuilder.append(address.getAddress());
        }
        this.target = targetBuilder.toString();
    }
}
