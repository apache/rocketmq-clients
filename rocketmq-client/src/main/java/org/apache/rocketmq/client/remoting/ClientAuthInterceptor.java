package org.apache.rocketmq.client.remoting;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.impl.Signature;

/**
 * Interceptor for all gRPC request.
 *
 * <p>Interceptor is responsible for authorization and RPC tracing.</p>
 */
@Slf4j
public class ClientAuthInterceptor implements ClientInterceptor {

    private final CredentialsObservable credentialsObservable;

    public ClientAuthInterceptor(CredentialsObservable credentialsObservable) {
        this.credentialsObservable = credentialsObservable;
    }

    private void customMetadata(Metadata headers) {
        try {
            final Metadata metadata = Signature.sign(credentialsObservable);
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
            public void start(Listener<RespT> responseListener, Metadata headers) {
                customMetadata(headers);
                super.start(new SimpleForwardingClientCallListener<RespT>(responseListener) {
                    @Override
                    public void onHeaders(Metadata headers) {
                        super.onHeaders(headers);
                    }

                    @Override
                    public void onMessage(RespT response) {
                        log.trace("gRPC response: {}\n{}", response.getClass().getName(), response);
                        super.onMessage(response);
                    }
                }, headers);
            }

            @Override
            public void sendMessage(ReqT request) {
                log.trace("gRPC request: {}\n{}", request.getClass().getName(), request);
                super.sendMessage(request);
            }
        };
    }
}

