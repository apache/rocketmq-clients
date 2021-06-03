package org.apache.rocketmq.client.remoting;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IpNameResolverFactory extends NameResolverProvider {
    private List<EquivalentAddressGroup> addresses;
    private final String serviceAuthority = "IPAuthority";
    private NameResolver.Listener2 listener2;

    public IpNameResolverFactory(List<InetSocketAddress> socketAddresses) {
        this.addresses = convertAddresses(socketAddresses);
    }

    public void updateAddresses(List<InetSocketAddress> socketAddresses) {
        if (null == listener2) {
            log.debug("Failed to update address for name resolver cause listener is null");
            return;
        }
        addresses = convertAddresses(socketAddresses);
        listener2.onResult(NameResolver.ResolutionResult.newBuilder().setAddresses(addresses).build());
    }

    private List<EquivalentAddressGroup> convertAddresses(List<InetSocketAddress> socketAddresses) {
        ArrayList<EquivalentAddressGroup> addresses = new ArrayList<EquivalentAddressGroup>();
        for (InetSocketAddress socketAddress : socketAddresses) {
            addresses.add(new EquivalentAddressGroup(socketAddress));
        }
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
