package org.apache.rocketmq.client.remoting;

import com.google.common.base.Preconditions;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.rocketmq.client.route.AddressScheme;

@Getter
@EqualsAndHashCode
public class Endpoints {
    private static final String ADDRESS_SEPARATOR = ",";
    private final AddressScheme addressScheme;

    /**
     * URI path for grpc target, e.g:
     * <p>1. dns:rocketmq.apache.org:8080
     * <p>2. ipv4:127.0.0.1:10911,127.0.0.2:10912
     * <p>3. ipv6:1050:0000:0000:0000:0005:0600:300c:326b:10911,1050:0000:0000:0000:0005:0600:300c:326b:10912
     */
    private final String facade;
    private final List<Address> addresses;

    public Endpoints(AddressScheme addressScheme, List<Address> addresses) {
        // TODO: polish code here.
        if (AddressScheme.DOMAIN_NAME == addressScheme && addresses.size() > 1) {
            throw new UnsupportedOperationException("Multiple addresses not allowed in domain schema.");
        }
        Preconditions.checkNotNull(addresses);
        if (addresses.isEmpty()) {
            throw new UnsupportedOperationException("No available address");
        }

        this.addressScheme = addressScheme;
        this.addresses = addresses;
        StringBuilder targetBuilder = new StringBuilder();
        targetBuilder.append(addressScheme.getPrefix());
        for (Address address : addresses) {
            targetBuilder.append(address.getAddress()).append(ADDRESS_SEPARATOR);
        }
        this.facade = targetBuilder.substring(0, targetBuilder.length() - 1);
    }

    public List<InetSocketAddress> convertToSocketAddresses() {
        switch (addressScheme) {
            case DOMAIN_NAME:
                return null;
            case IPv4:
            case IPv6:
            default:
                // Customize the name resolver to support multiple addresses.
                List<InetSocketAddress> socketAddresses = new ArrayList<InetSocketAddress>();
                for (Address address : addresses) {
                    socketAddresses.add(new InetSocketAddress(address.getHost(), address.getPort()));
                }
                return socketAddresses;
        }
    }

    @Override
    public String toString() {
        return facade;
    }
}
