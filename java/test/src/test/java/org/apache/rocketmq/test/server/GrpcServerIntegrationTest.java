package org.apache.rocketmq.test.server;

import apache.rocketmq.v2.MessagingServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.List;
import org.junit.Rule;

public class GrpcServerIntegrationTest {
    /**
     * Let OS pick up an available port.
     */
    protected int port = 0;

    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the end of test.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    protected void setUpServer(MessagingServiceGrpc.MessagingServiceImplBase serverImpl,
        int port, ServerInterceptor... interceptors) throws IOException, CertificateException {
        SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
        ServerServiceDefinition serviceDefinition = serverImpl.bindService();
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(port)
            .directExecutor()
            .addService(serviceDefinition)
            .useTransportSecurity(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey());
        for (ServerInterceptor interceptor : interceptors) {
            serverBuilder = serverBuilder.intercept(interceptor);
        }
        Server server = serverBuilder.build()
            .start();
        this.port = server.getPort();
        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(server);
    }
}
