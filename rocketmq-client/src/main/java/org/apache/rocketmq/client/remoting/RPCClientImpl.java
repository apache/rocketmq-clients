package org.apache.rocketmq.client.remoting;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.MessagingServiceGrpc;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.stub.MetadataUtils;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class RpcClientImpl implements RPCClient {

    private static final String TENANT_ID_KEY = "x-mq-tenant-id";
    private static final String ARN_KEY = "x-mq-arn";
    private static final String AUTHORIZATION = "authorization";
    private static final String DATE_TIME_KEY = "x-mq-date-time";
    private static final String TRACE_ID_KEY = "x-mq-trace-id";
    private static final String ALGORITHM_KEY = "MQv2-HMAC-SHA1";
    private static final String CREDENTIAL_KEY = "Credential";
    private static final String SIGNED_HEADERS_KEY = "SignedHeaders";
    private static final String SIGNATURE_KEY = "Signature";
    private static final String DATE_TIME_FORMAT = "yyyyMMdd'T'HHmmss'Z'";

    private String arn;
    private String tenantId;
    private AccessCredential accessCredential;

    private String regionId = "";
    private String serviceName = "";

    private final RpcTarget rpcTarget;
    private final ManagedChannel channel;

    private final MessagingServiceGrpc.MessagingServiceBlockingStub blockingStub;
    private final MessagingServiceGrpc.MessagingServiceFutureStub futureStub;

    public RpcClientImpl(RpcTarget rpcTarget) {
        this.rpcTarget = rpcTarget;

        final SslContextBuilder builder = GrpcSslContexts.forClient();
        builder.trustManager(InsecureTrustManagerFactory.INSTANCE);

        SslContext sslContext = null;
        try {
            sslContext = builder.build();
        } catch (Throwable t) {
            log.error("Failed to build ssl Context");
        }

        this.channel =
                NettyChannelBuilder.forTarget(rpcTarget.getTarget())
                                   .disableRetry()
                                   .sslContext(sslContext)
                                   .build();

        this.blockingStub = MessagingServiceGrpc.newBlockingStub(channel);
        this.futureStub = MessagingServiceGrpc.newFutureStub(channel);
    }

    @Override
    public void setArn(String arn) {
        this.arn = arn;
    }

    @Override
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public void setAccessCredential(AccessCredential accessCredential) {
        this.accessCredential = accessCredential;
    }

    private Metadata assignMetadata() {
        Metadata header = new Metadata();
        if (StringUtils.isNotBlank(tenantId)) {
            header.put(Metadata.Key.of(TENANT_ID_KEY, Metadata.ASCII_STRING_MARSHALLER), tenantId);
        }
        if (StringUtils.isNotBlank(arn)) {
            header.put(Metadata.Key.of(ARN_KEY, Metadata.ASCII_STRING_MARSHALLER), arn);
        }

        String dateTime = new SimpleDateFormat(DATE_TIME_FORMAT).format(new Date());
        header.put(Metadata.Key.of(DATE_TIME_KEY, Metadata.ASCII_STRING_MARSHALLER), dateTime);

        if (null == accessCredential) {
            return header;
        }

        final String accessKey = accessCredential.getAccessKey();
        final String accessSecret = accessCredential.getAccessSecret();

        if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(accessSecret)) {
            return header;
        }

        String regionId = "cn-hangzhou";
        String serviceName = "aone";

        final String authorization = ALGORITHM_KEY
                                     + " "
                                     + CREDENTIAL_KEY
                                     + "="
                                     + accessKey
                                     + "/"
                                     + regionId
                                     + "/"
                                     + serviceName
                                     + ", "
                                     + SIGNED_HEADERS_KEY
                                     + "="
                                     + DATE_TIME_KEY
                                     + ", "
                                     + SIGNATURE_KEY
                                     + "="
                                     + TlsHelper.sign(accessSecret, dateTime);

        header.put(Metadata.Key.of(AUTHORIZATION, Metadata.ASCII_STRING_MARSHALLER), authorization);
        return header;
    }

    @Override
    public void shutdown() {
        if (null != channel) {
            channel.shutdown();
        }
    }

    @Override
    public void setIsolated(boolean isolated) {
        rpcTarget.setIsolated(isolated);
    }

    @Override
    public boolean isIsolated() {
        return rpcTarget.isIsolated();
    }

    @Override
    public SendMessageResponse sendMessage(SendMessageRequest request, long duration, TimeUnit unit) {
        return MetadataUtils.attachHeaders(blockingStub.withDeadlineAfter(duration, unit), assignMetadata())
                            .sendMessage(request);
    }

    @Override
    public ListenableFuture<SendMessageResponse> sendMessage(
            SendMessageRequest request, Executor executor, long duration, TimeUnit unit) {
        return MetadataUtils.attachHeaders(futureStub.withExecutor(executor).withDeadlineAfter(duration, unit),
                                           assignMetadata()).sendMessage(request);
    }

    @Override
    public QueryAssignmentResponse queryAssignment(
            QueryAssignmentRequest request, long duration, TimeUnit unit) {
        return MetadataUtils.attachHeaders(blockingStub.withDeadlineAfter(duration, unit), assignMetadata())
                            .queryAssignment(request);
    }

    @Override
    public HealthCheckResponse healthCheck(HealthCheckRequest request, long duration, TimeUnit unit) {
        return MetadataUtils.attachHeaders(blockingStub.withDeadlineAfter(duration, unit), assignMetadata())
                            .healthCheck(request);
    }

    @Override
    public ListenableFuture<ReceiveMessageResponse> receiveMessage(ReceiveMessageRequest request, Executor executor,
                                                                   long duration, TimeUnit unit) {
        return MetadataUtils.attachHeaders(futureStub.withExecutor(executor).withDeadlineAfter(duration, unit),
                                           assignMetadata()).receiveMessage(request);
    }

    @Override
    public AckMessageResponse ackMessage(AckMessageRequest request, long duration, TimeUnit unit) {
        return MetadataUtils.attachHeaders(blockingStub.withDeadlineAfter(duration, unit), assignMetadata())
                            .ackMessage(request);
    }

    @Override
    public ListenableFuture<AckMessageResponse> ackMessage(
            AckMessageRequest request, Executor executor, long duration, TimeUnit unit) {
        return MetadataUtils.attachHeaders(futureStub.withExecutor(executor).withDeadlineAfter(duration, unit),
                                           assignMetadata()).ackMessage(request);
    }

    @Override
    public NackMessageResponse nackMessage(
            NackMessageRequest request, long duration, TimeUnit unit) {
        return MetadataUtils.attachHeaders(blockingStub.withDeadlineAfter(duration, unit), assignMetadata())
                            .nackMessage(request);
    }

    @Override
    public HeartbeatResponse heartbeat(HeartbeatRequest request, long duration, TimeUnit unit) {
        return MetadataUtils.attachHeaders(blockingStub.withDeadlineAfter(duration, unit), assignMetadata())
                            .heartbeat(request);
    }

    @Override
    public QueryRouteResponse queryRoute(
            QueryRouteRequest request, long duration, TimeUnit unit) {
        return MetadataUtils.attachHeaders(blockingStub.withDeadlineAfter(duration, unit), assignMetadata())
                            .queryRoute(request);
    }
}
