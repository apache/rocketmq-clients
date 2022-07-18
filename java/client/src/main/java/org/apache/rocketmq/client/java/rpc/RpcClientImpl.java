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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.client.java.route.Endpoints;

@SuppressWarnings("UnstableApiUsage")
public class RpcClientImpl implements RpcClient {
    private static final Duration KEEP_ALIVE_DURATION = Duration.ofSeconds(30);
    private static final int GRPC_MAX_MESSAGE_SIZE = Integer.MAX_VALUE;

    private final Endpoints endpoints;
    private final ManagedChannel channel;
    private final MessagingServiceGrpc.MessagingServiceFutureStub futureStub;
    private final MessagingServiceGrpc.MessagingServiceBlockingStub blockingStub;
    private final MessagingServiceGrpc.MessagingServiceStub stub;

    private long activityNanoTime;

    @SuppressWarnings("deprecation")
    public RpcClientImpl(Endpoints endpoints) throws SSLException {
        this.endpoints = endpoints;
        final SslContextBuilder builder = GrpcSslContexts.forClient();
        builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        SslContext sslContext = builder.build();

        final NettyChannelBuilder channelBuilder =
            NettyChannelBuilder.forTarget(endpoints.getGrpcTarget())
                // .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .keepAliveTime(KEEP_ALIVE_DURATION.toNanos(), TimeUnit.NANOSECONDS)
                .maxInboundMessageSize(GRPC_MAX_MESSAGE_SIZE)
                .intercept(LoggingInterceptor.getInstance())
                .sslContext(sslContext);
        // Disable grpc's auto-retry here.

        final List<InetSocketAddress> socketAddresses = endpoints.toSocketAddresses();
        if (null != socketAddresses) {
            final IpNameResolverFactory ipNameResolverFactory = new IpNameResolverFactory(socketAddresses);
            channelBuilder.nameResolverFactory(ipNameResolverFactory);
        }

        this.channel = channelBuilder.build();
        this.futureStub = MessagingServiceGrpc.newFutureStub(channel);
        this.blockingStub = MessagingServiceGrpc.newBlockingStub(channel);
        this.stub = MessagingServiceGrpc.newStub(channel);
        this.activityNanoTime = System.nanoTime();
    }

    private <T> ListenableFuture<RpcInvocation<T>> wrapInvocationContext(ListenableFuture<T> future,
        Metadata header) {
        return Futures.transformAsync(future, response -> {
            final Context context = new Context(endpoints, header);
            return Futures.immediateFuture(new RpcInvocation<>(response, context));
        }, MoreExecutors.directExecutor());
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
    public ListenableFuture<RpcInvocation<QueryRouteResponse>> queryRoute(Metadata metadata,
        QueryRouteRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        final ListenableFuture<QueryRouteResponse> future = futureStub
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).queryRoute(request);
        return wrapInvocationContext(future, metadata);
    }

    @Override
    public ListenableFuture<RpcInvocation<HeartbeatResponse>> heartbeat(Metadata metadata, HeartbeatRequest request,
        Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        final ListenableFuture<HeartbeatResponse> future =
            futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
                .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).heartbeat(request);
        return wrapInvocationContext(future, metadata);
    }

    @Override
    public ListenableFuture<RpcInvocation<SendMessageResponse>> sendMessage(Metadata metadata,
        SendMessageRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        final ListenableFuture<SendMessageResponse> future =
            futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
                .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).sendMessage(request);
        return wrapInvocationContext(future, metadata);
    }

    @Override
    public ListenableFuture<RpcInvocation<QueryAssignmentResponse>> queryAssignment(Metadata metadata,
        QueryAssignmentRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        final ListenableFuture<QueryAssignmentResponse> future =
            futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
                .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).queryAssignment(request);
        return wrapInvocationContext(future, metadata);
    }

    @Override
    public ListenableFuture<RpcInvocation<Iterator<ReceiveMessageResponse>>> receiveMessage(Metadata metadata,
        ReceiveMessageRequest request, ExecutorService executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        final Callable<Iterator<ReceiveMessageResponse>> callable = () -> blockingStub
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).receiveMessage(request);
        final ListenableFuture<Iterator<ReceiveMessageResponse>> future =
            MoreExecutors.listeningDecorator(executor).submit(callable);
        return wrapInvocationContext(future, metadata);
    }

    @Override
    public ListenableFuture<RpcInvocation<AckMessageResponse>> ackMessage(Metadata metadata,
        AckMessageRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        final ListenableFuture<AckMessageResponse> future =
            futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
                .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).ackMessage(request);
        return wrapInvocationContext(future, metadata);
    }

    @Override
    public ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> changeInvisibleDuration(
        Metadata metadata, ChangeInvisibleDurationRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        final ListenableFuture<ChangeInvisibleDurationResponse> future =
            futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
                .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).changeInvisibleDuration(request);
        return wrapInvocationContext(future, metadata);
    }

    @Override
    public ListenableFuture<RpcInvocation<ForwardMessageToDeadLetterQueueResponse>> forwardMessageToDeadLetterQueue(
        Metadata metadata, ForwardMessageToDeadLetterQueueRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        final ListenableFuture<ForwardMessageToDeadLetterQueueResponse> future = futureStub
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
            .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).forwardMessageToDeadLetterQueue(request);
        return wrapInvocationContext(future, metadata);
    }

    @Override
    public ListenableFuture<RpcInvocation<EndTransactionResponse>> endTransaction(Metadata metadata,
        EndTransactionRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        final ListenableFuture<EndTransactionResponse> future =
            futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
                .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).endTransaction(request);
        return wrapInvocationContext(future, metadata);
    }

    @Override
    public ListenableFuture<RpcInvocation<NotifyClientTerminationResponse>> notifyClientTermination(
        Metadata metadata, NotifyClientTerminationRequest request, Executor executor, Duration duration) {
        this.activityNanoTime = System.nanoTime();
        final ListenableFuture<NotifyClientTerminationResponse> future =
            futureStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).withExecutor(executor)
                .withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS).notifyClientTermination(request);
        return wrapInvocationContext(future, metadata);
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
