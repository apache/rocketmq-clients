package org.apache.rocketmq.client.remoting;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public interface RpcClient {
    void shutdown();

    void setArn(String arn);

    String getArn();

    void setTenantId(String tenantId);

    String getTenantId();

    void setAccessCredential(AccessCredential accessCredential);

    AccessCredential getAccessCredential();

    SendMessageResponse sendMessage(SendMessageRequest request, long duration, TimeUnit unit);

    ListenableFuture<SendMessageResponse> sendMessage(
            SendMessageRequest request, Executor executor, long duration, TimeUnit unit);

    QueryAssignmentResponse queryAssignment(
            QueryAssignmentRequest request, long duration, TimeUnit unit);

    HealthCheckResponse healthCheck(HealthCheckRequest request, long duration, TimeUnit unit);

    ListenableFuture<ReceiveMessageResponse> receiveMessage(
            ReceiveMessageRequest request, Executor executor, long duration, TimeUnit unit);

    AckMessageResponse ackMessage(AckMessageRequest request, long duration, TimeUnit unit);

    ListenableFuture<AckMessageResponse> ackMessage(
            AckMessageRequest request, Executor executor, long duration, TimeUnit unit);

    NackMessageResponse nackMessage(NackMessageRequest request, long duration, TimeUnit unit);


    HeartbeatResponse heartbeat(HeartbeatRequest request, long duration, TimeUnit unit);

    QueryRouteResponse queryRoute(QueryRouteRequest request, long duration, TimeUnit unit);
}
