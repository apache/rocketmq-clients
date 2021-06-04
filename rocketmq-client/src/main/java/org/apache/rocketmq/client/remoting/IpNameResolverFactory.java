package org.apache.rocketmq.client.remoting;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Name resolver factory which is customized to support multiple IPv4/IPv6 address in endpoints.
 */
@Slf4j
public class IpNameResolverFactory extends NameResolverProvider {
    private final List<EquivalentAddressGroup> addresses;
    private final String serviceAuthority = "IPAuthority";
    private NameResolver.Listener2 listener2;

    public IpNameResolverFactory(List<InetSocketAddress> socketAddresses) {
        this.addresses = convertAddresses(socketAddresses);
    }

    private List<EquivalentAddressGroup> convertAddresses(List<InetSocketAddress> socketAddresses) {
        ArrayList<EquivalentAddressGroup> addresses = new ArrayList<EquivalentAddressGroup>();
        for (InetSocketAddress socketAddress : socketAddresses) {
            addresses.add(new EquivalentAddressGroup(socketAddress));
        }
        Collections.shuffle(addresses);
        return addresses;
    }

    @Override
    public NameResolver newNameResolver(URI notUsedUri, NameResolver.Args args) {
        return new NameResolver() {

            @Override
            public String getServiceAuthority() {
                return serviceAuthority;
            }

            @Override
            public void start(Listener2 listener2) {
                IpNameResolverFactory.this.listener2 = listener2;
                listener2.onResult(ResolutionResult.newBuilder().setAddresses(addresses).build());
            }

            @Override
            public void shutdown() {
            }
        };
    }

    @Override
    public String getDefaultScheme() {
        return "IP";
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 0;
    }
}
