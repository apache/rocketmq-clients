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

package org.apache.rocketmq.client.java.impl;

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
import com.google.common.util.concurrent.AbstractIdleService;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.RpcFuture;

/**
 * Client manager supplies a series of unified APIs to execute remote procedure calls for {@link Client}.
 */
public abstract class ClientManager extends AbstractIdleService {
    /**
     * Provide for the client to share the scheduler.
     *
     * @return shared scheduler.
     */
    public abstract ScheduledExecutorService getScheduler();

    /**
     * Query topic route asynchronously, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param request   query route request.
     * @param duration  request max duration.
     * @return invocation of response future.
     */
    public abstract RpcFuture<QueryRouteRequest, QueryRouteResponse> queryRoute(Endpoints endpoints,
        QueryRouteRequest request, Duration duration);

    /**
     * Heart beat asynchronously, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param request   heartbeat request.
     * @param duration  request max duration.
     * @return invocation of response future.
     */
    public abstract RpcFuture<HeartbeatRequest, HeartbeatResponse> heartbeat(Endpoints endpoints,
        HeartbeatRequest request, Duration duration);

    /**
     * Send message asynchronously, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param request   send message request.
     * @param duration  request max duration.
     * @return invocation of response future.
     */
    public abstract RpcFuture<SendMessageRequest, SendMessageResponse> sendMessage(Endpoints endpoints,
        SendMessageRequest request, Duration duration);

    /**
     * Query assignment asynchronously, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param request   query assignment request.
     * @param duration  request max duration.
     * @return invocation of response future.
     */
    public abstract RpcFuture<QueryAssignmentRequest, QueryAssignmentResponse> queryAssignment(Endpoints endpoints,
        QueryAssignmentRequest request, Duration duration);

    /**
     * Receiving messages asynchronously from the server, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param request   receive message request.
     * @param duration  request max duration.
     * @return invocation of response future.
     */
    public abstract RpcFuture<ReceiveMessageRequest, List<ReceiveMessageResponse>> receiveMessage(Endpoints endpoints,
        ReceiveMessageRequest request, Duration duration);

    /**
     * Ack message asynchronously after the success of consumption, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param request   ack message request.
     * @param duration  request max duration.
     * @return invocation of response future.
     */
    public abstract RpcFuture<AckMessageRequest, AckMessageResponse> ackMessage(Endpoints endpoints,
        AckMessageRequest request, Duration duration);

    /**
     * Nack message asynchronously after the failure of consumption, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param request   nack message request.
     * @param duration  request max duration.
     * @return invocation of response future.
     */
    public abstract RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse>
    changeInvisibleDuration(Endpoints endpoints, ChangeInvisibleDurationRequest request, Duration duration);

    /**
     * Send a message to the dead letter queue asynchronously, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param request   request of sending a message to DLQ.
     * @param duration  request max duration.
     * @return invocation of response future.
     */
    public abstract RpcFuture<ForwardMessageToDeadLetterQueueRequest,
        ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Endpoints endpoints,
        ForwardMessageToDeadLetterQueueRequest request, Duration duration);

    /**
     * Submit transaction resolution asynchronously, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param request   end transaction request.
     * @param duration  request max duration.
     * @return invocation of response future.
     */
    public abstract RpcFuture<EndTransactionRequest, EndTransactionResponse> endTransaction(Endpoints endpoints,
        EndTransactionRequest request, Duration duration);

    /**
     * Asynchronously notify the server that client is terminated, the method ensures no throwable.
     *
     * @param endpoints request endpoints.
     * @param request   notify client termination request.
     * @param duration  request max duration.
     * @return response future of notification of client termination.
     */
    @SuppressWarnings("UnusedReturnValue")
    public abstract RpcFuture<NotifyClientTerminationRequest, NotifyClientTerminationResponse>
    notifyClientTermination(Endpoints endpoints, NotifyClientTerminationRequest request, Duration duration);

    /**
     * Establish telemetry session stream to server.
     *
     * @param endpoints        request endpoints.
     * @param duration         stream max duration.
     * @param responseObserver response observer.
     * @return request observer.
     * @throws ClientException if failed to establish telemetry session stream.
     */
    public abstract StreamObserver<TelemetryCommand> telemetry(Endpoints endpoints, Duration duration,
        StreamObserver<TelemetryCommand> responseObserver) throws ClientException;
}
