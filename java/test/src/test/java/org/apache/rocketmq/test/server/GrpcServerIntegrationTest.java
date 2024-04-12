/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.junit.Rule;

public class GrpcServerIntegrationTest {
    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the end of test.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    /**
     * Let OS pick up an available port.
     */
    protected int port = 0;


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
