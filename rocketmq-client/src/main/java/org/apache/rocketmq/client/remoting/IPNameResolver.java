package org.apache.rocketmq.client.remoting;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class IPNameResolver extends NameResolver.Factory {
    final List<EquivalentAddressGroup> addresses;
    final String scheme = "IP";

    public IPNameResolver(InetSocketAddress... socketAddresses) {
        this.addresses = new ArrayList<EquivalentAddressGroup>();
        for (InetSocketAddress socketAddress : socketAddresses) {
            addresses.add(new EquivalentAddressGroup(socketAddress));
        }
    }

    @Override
    public NameResolver newNameResolver(URI notUsedUri, NameResolver.Args args) {
        return new NameResolver() {


            @Override
            public String getServiceAuthority() {
                return "IPAuthority";
            }

            @Override
            public void start(Listener2 listener2) {
                listener2.onResult(ResolutionResult.newBuilder().setAddresses(addresses).build());
            }

            @Override
            public void shutdown() {
            }
        };
    }

    @Override
    public String getDefaultScheme() {
        return scheme;
    }
}
