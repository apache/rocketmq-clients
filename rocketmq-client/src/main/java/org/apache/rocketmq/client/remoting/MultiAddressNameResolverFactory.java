package org.apache.rocketmq.client.remoting;

import io.grpc.NameResolver;
import java.net.URI;

public class MultiAddressNameResolverFactory extends NameResolver.Factory {
    @Override
    public String getDefaultScheme() {
        URI uri;
        uri = URI.create("");
        return null;
    }
}
