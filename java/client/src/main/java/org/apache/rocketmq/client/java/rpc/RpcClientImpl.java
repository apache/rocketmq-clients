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

package org.apache.rocketmq.client.java.rpc;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.MessagingServiceGrpc;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.client.java.route.Endpoints;

public class RpcClientImpl implements RpcClient {
    private static final int CONNECT_TIMEOUT_MILLIS = 3 * 1000;
    private static final int GRPC_MAX_MESSAGE_SIZE = Integer.MAX_VALUE;

    private final ManagedChannel channel;
    private final MessagingServiceGrpc.MessagingServiceFutureStub futureStub;
    private final MessagingServiceGrpc.MessagingServiceStub stub;

    private long activityNanoTime;

    @SuppressWarnings("deprecation")
    public RpcClientImpl(Endpoints endpoints, boolean sslEnabled) throws SSLException {
        final NettyChannelBuilder channelBuilder =
            NettyChannelBuilder.forTarget(endpoints.getGrpcTarget())
                .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                .maxInboundMessageSize(GRPC_MAX_MESSAGE_SIZE)
                .intercept(LoggingInterceptor.getInstance());

        if (sslEnabled) {
            final SslContextBuilder builder = GrpcSslContexts.forClient();
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            SslContext sslContext = builder.build();
            channelBuilder.sslContext(sslContext);
        } else {
            channelBuilder.usePlaintext();
        }

        // Disable grpc's auto-retry here.
        channelBuilder.disableRetry();
        final List<InetSocketAddress> socketAddresses = endpoints.toSocketAddresses();
        if (null != socketAddresses) {
            final IpNameResolverFactory ipNameResolverFactory = new IpNameResolverFactory(socketAddresses);
            channelBuilder.nameResolverFactory(ipNameResolverFactory);
        }
        this.channel = channelBuilder.build();
        this.futureStub = MessagingServiceGrpc.newFutureStub(channel);
        this.stub = MessagingServiceGrpc.newStub(channel);
        this.activityNanoTime = System.nanoTime();
    }

    @Override
    public Duration idleDuration() {
        return Duration.ofNanos(System.nanoTime() - activityNanoTime);
    }

    @Override
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    @Override
    public ListenableFuture<QueryRouteResponse> queryRoute(Metadata metadata,
        QueryRouteRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        return futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).queryRoute(request);
    }

    @Override
    public ListenableFuture<HeartbeatResponse> heartbeat(Metadata metadata,
        HeartbeatRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        return futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).heartbeat(request);
    }

    @Override
    public ListenableFuture<SendMessageResponse> sendMessage(Metadata metadata,
        SendMessageRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        return futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).sendMessage(request);
    }

    @Override
    public ListenableFuture<QueryAssignmentResponse> queryAssignment(Metadata metadata,
        QueryAssignmentRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        return futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).queryAssignment(request);
    }

    @Override
    public ListenableFuture<List<ReceiveMessageResponse>> receiveMessage(Metadata metadata,
        ReceiveMessageRequest request, ExecutorService executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        SettableFuture<List<ReceiveMessageResponse>> future = SettableFuture.create();
        List<ReceiveMessageResponse> responses = new ArrayList<>();
        stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS)
            .receiveMessage(request, new StreamObserver<ReceiveMessageResponse>() {
                @Override
                public void onNext(ReceiveMessageResponse response) {
                    responses.add(response);
                }

                @Override
                public void onError(Throwable t) {
                    future.setException(t);
                }

                @Override
                public void onCompleted() {
                    future.set(responses);
                }
            });
        return future;
    }

    @Override
    public ListenableFuture<AckMessageResponse> ackMessage(Metadata metadata,
        AckMessageRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        return futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).ackMessage(request);
    }

    @Override
    public ListenableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Metadata metadata,
        ChangeInvisibleDurationRequest request, Executor executor,
        Duration duration) {
        this.activityNanoTime = System.nanoTime();
        return futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).changeInvisibleDuration(request);
    }

    @Override
    public ListenableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(
        Metadata metadata, ForwardMessageToDeadLetterQueueRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        return futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).forwardMessageToDeadLetterQueue(request);
    }

    @Override
    public ListenableFuture<EndTransactionResponse> endTransaction(Metadata metadata, EndTransactionRequest request,
        Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        return futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).endTransaction(request);
    }

    @Override
    public ListenableFuture<NotifyClientTerminationResponse> notifyClientTermination(Metadata metadata,
        NotifyClientTerminationRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        return futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).notifyClientTermination(request);
    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(Metadata metadata, Executor executor, Duration duration,
        StreamObserver<TelemetryCommand> responseObserver) {
        final ClientInterceptor interceptor = MetadataUtils.newAttachHeadersInterceptor(metadata);
        final MessagingServiceGrpc.MessagingServiceStub stub0 = this.stub.withInterceptors(interceptor)
            .withExecutor(executor).withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS);
        return stub0.telemetry(responseObserver);
    }
}
