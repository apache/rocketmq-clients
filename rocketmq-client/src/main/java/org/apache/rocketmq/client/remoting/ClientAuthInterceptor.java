package org.apache.rocketmq.client.remoting;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.impl.ClientConfig;
import org.apache.rocketmq.client.impl.Signature;

/**
 * Client auth interceptor for authentication, but only serve for message tracing actually.
 */
@Slf4j
public class ClientAuthInterceptor implements ClientInterceptor {

    private final ClientConfig clientConfig;

    public ClientAuthInterceptor(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    private void customMetadata(Metadata headers) {
        try {
            final Metadata metadata = Signature.sign(clientConfig);
            headers.merge(metadata);
        } catch (Throwable t) {
            log.error("Failed to sign headers", t);
        }
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> listener, Metadata headers) {
                customMetadata(headers);
                super.start(listener, headers);
            }
        };
    }
}

