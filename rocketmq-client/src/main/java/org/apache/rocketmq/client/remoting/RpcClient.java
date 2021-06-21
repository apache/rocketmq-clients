package org.apache.rocketmq.client.remoting;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.PollOrphanTransactionRequest;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.producer.OrphanTransactionCallback;

/**
 * Client for all explicit RPC in RocketMQ.
 */
public interface RpcClient {
    /**
     * Shutdown the client.
     */
    void shutdown();

    /**
     * Send message synchronously.
     *
     * @param request  send message request.
     * @param duration request max duration
     * @param unit     duration time unit
     * @return response of sending message.
     */
    SendMessageResponse sendMessage(SendMessageRequest request, long duration, TimeUnit unit);

    /**
     * Send message asynchronously
     *
     * @param request  send message request.
     * @param executor gRPC asynchronous executor.
     * @param duration request max duration
     * @param unit     duration time unit
     * @return response future of sending message.
     */
    ListenableFuture<SendMessageResponse> sendMessage(
            SendMessageRequest request, Executor executor, long duration, TimeUnit unit);

    /**
     * Query load assignment of consumer.
     *
     * @param request  query assignment request.
     * @param duration request max duration.
     * @param unit     duration time unit
     * @return response of query assignment.
     */
    QueryAssignmentResponse queryAssignment(
            QueryAssignmentRequest request, long duration, TimeUnit unit);

    /**
     * For health check.
     *
     * @param request  health check request.
     * @param duration request max duration.
     * @param unit     duration time unit
     * @return response of health check
     */
    HealthCheckResponse healthCheck(HealthCheckRequest request, long duration, TimeUnit unit);

    /**
     * Receiving message from server.
     *
     * @param request  receiving message request.
     * @param executor gRPC asynchronous executor.
     * @param duration request max duration.
     * @param unit     duration time unit
     * @return response of receiving message
     */
    ListenableFuture<ReceiveMessageResponse> receiveMessage(
            ReceiveMessageRequest request, Executor executor, long duration, TimeUnit unit);

    /**
     * Ack message synchronously after consuming success.
     *
     * @param request  ack message request.
     * @param duration request max duration.
     * @param unit     duration time unit
     * @return response of ack message.
     */
    AckMessageResponse ackMessage(AckMessageRequest request, long duration, TimeUnit unit);


    /**
     * Ack message asynchronously after consuming
     *
     * @param request  ack message request.
     * @param executor gRPC asynchronous executor
     * @param duration request max duration.
     * @param unit     duration time unit
     * @return response future of ack message.
     */
    ListenableFuture<AckMessageResponse> ackMessage(
            AckMessageRequest request, Executor executor, long duration, TimeUnit unit);

    /**
     * Nack message synchronously after consuming failure
     *
     * @param request  ack message request.
     * @param duration request max duration.
     * @param unit     duration time unit
     * @return response of nack message.
     */
    NackMessageResponse nackMessage(NackMessageRequest request, long duration, TimeUnit unit);

    /**
     * Nack message asynchronously after consuming failure
     *
     * @param request  nack message request.
     * @param executor gRPC asynchronous executor
     * @param duration request max duration.
     * @param unit     duration time unit
     * @return response future of ack message.
     */
    ListenableFuture<NackMessageResponse> nackMessage(
            NackMessageRequest request, Executor executor, long duration, TimeUnit unit);

    /**
     * Send heart beat.
     *
     * @param request  heart beat request.
     * @param duration request max duration.
     * @param unit     duration time unit.
     * @return response of heart beat.
     */
    HeartbeatResponse heartbeat(HeartbeatRequest request, long duration, TimeUnit unit);

    /**
     * Query topic route
     *
     * @param request  query topic route request.
     * @param duration request max duration.
     * @param unit     duration time unit.
     * @return response of query topic route.
     */
    QueryRouteResponse queryRoute(QueryRouteRequest request, long duration, TimeUnit unit);

    /**
     * Submit transaction resolution
     *
     * @param request  end transaction request.
     * @param duration request max duration.
     * @param unit     duration time unit.
     * @return response of submitting transaction resolution.
     */
    EndTransactionResponse endTransaction(EndTransactionRequest request, long duration, TimeUnit unit);


    /**
     * Poll orphan transaction
     *
     * @param request  poll orphan transaction request.
     * @param callback callback of orphan transaction.
     * @param duration request max duration.
     * @param unit     duration time unit.
     */
    void pollOrphanTransaction(PollOrphanTransactionRequest request, OrphanTransactionCallback callback,
                               long duration, TimeUnit unit);


    QueryOffsetResponse queryOffset(QueryOffsetRequest request, long duration, TimeUnit unit);

    PullMessageResponse pullMessage(PullMessageRequest request, long duration, TimeUnit unit);
}
