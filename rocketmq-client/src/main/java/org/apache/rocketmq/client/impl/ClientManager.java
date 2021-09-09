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

package org.apache.rocketmq.client.impl;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.MultiplexingRequest;
import apache.rocketmq.v1.MultiplexingResponse;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.NotifyClientTerminationResponse;
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
import io.grpc.Metadata;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.route.Endpoints;

public interface ClientManager {
    /**
     * Register client.
     *
     * @param client client.
     */
    void registerClient(Client client);

    /**
     * Unregister client.
     *
     * @param client client.
     */
    void unregisterClient(Client client);

    /**
     * Returns {@code true} if manager contains no {@link Client}.
     *
     * @return {@code true} if this map contains no {@link Client}.
     */
    boolean isEmpty();

    /**
     * Provide for client to share the scheduler.
     *
     * @return shared scheduler.
     */
    ScheduledExecutorService getScheduler();

    /**
     * Query topic route asynchronously, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   query route request.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of topic route.
     */
    ListenableFuture<QueryRouteResponse> queryRoute(Endpoints endpoints, Metadata metadata, QueryRouteRequest request,
                                                    long duration, TimeUnit timeUnit);

    /**
     * Heart beat asynchronously, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   heart beat request.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of heart beat.
     */
    ListenableFuture<HeartbeatResponse> heartbeat(Endpoints endpoints, Metadata metadata, HeartbeatRequest request,
                                                  long duration, TimeUnit timeUnit);

    /**
     * Asynchronous health check for producer, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   health check request.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of health check.
     */
    ListenableFuture<HealthCheckResponse> healthCheck(Endpoints endpoints, Metadata metadata,
                                                      HealthCheckRequest request, long duration, TimeUnit timeUnit);

    /**
     * Send message asynchronously, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   send message request.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of sending message.
     */
    ListenableFuture<SendMessageResponse> sendMessage(Endpoints endpoints, Metadata metadata,
                                                      SendMessageRequest request, long duration, TimeUnit timeUnit);

    /**
     * Query assignment asynchronously, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   query assignment request.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of query assignment.
     */
    ListenableFuture<QueryAssignmentResponse> queryAssignment(Endpoints endpoints, Metadata metadata,
                                                              QueryAssignmentRequest request, long duration,
                                                              TimeUnit timeUnit);

    /**
     * Receiving message asynchronously from server, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   receiving message request.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of receiving message.
     */
    ListenableFuture<ReceiveMessageResponse> receiveMessage(Endpoints endpoints, Metadata metadata,
                                                            ReceiveMessageRequest request, long duration,
                                                            TimeUnit timeUnit);

    /**
     * Ack message asynchronously after success of consumption, the method ensures no throwable.
     *
     * @param endpoints requested endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   ack message request.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of ack message.
     */
    ListenableFuture<AckMessageResponse> ackMessage(Endpoints endpoints, Metadata metadata, AckMessageRequest request,
                                                    long duration, TimeUnit timeUnit);

    /**
     * Nack message asynchronously after failure of consumption, the method ensures no throwable.
     *
     * @param endpoints request endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   nack message request.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of nack message.
     */
    ListenableFuture<NackMessageResponse> nackMessage(Endpoints endpoints, Metadata metadata,
                                                      NackMessageRequest request, long duration, TimeUnit timeUnit);

    /**
     * Send message to dead letter queue asynchronously, the method ensures no throwable.
     *
     * @param endpoints request endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   request of sending message to DLQ.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of sending message to DLQ.
     */
    ListenableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(
            Endpoints endpoints, Metadata metadata, ForwardMessageToDeadLetterQueueRequest request, long duration,
            TimeUnit timeUnit);

    /**
     * Submit transaction resolution asynchronously, the method ensures no throwable.
     *
     * @param endpoints request endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   end transaction request.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of submitting transaction resolution.
     */
    ListenableFuture<EndTransactionResponse> endTransaction(Endpoints endpoints, Metadata metadata,
                                                            EndTransactionRequest request, long duration,
                                                            TimeUnit timeUnit);

    /**
     * Query offset asynchronously for pull, the method ensures no throwable.
     *
     * @param endpoints request endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   query offset request.
     * @param duration  gRPC asynchronous executor.
     * @param timeUnit  duration time unit.
     * @return response future of query offset.
     */
    ListenableFuture<QueryOffsetResponse> queryOffset(Endpoints endpoints, Metadata metadata,
                                                      QueryOffsetRequest request, long duration, TimeUnit timeUnit);

    /**
     * Pull message from remote asynchronously, the method ensures no throwable.
     *
     * @param endpoints request endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   pull message request.
     * @param duration  duration time unit.
     * @param timeUnit  duration time unit.
     * @return response future of pull message.
     */
    ListenableFuture<PullMessageResponse> pullMessage(Endpoints endpoints, Metadata metadata,
                                                      PullMessageRequest request, long duration, TimeUnit timeUnit);

    /**
     * Multiplexing call asynchronously for composited request, the method ensures no throwable.
     *
     * @param endpoints request endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   multiplexing call request.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of multiplexing call.
     */
    ListenableFuture<MultiplexingResponse> multiplexingCall(Endpoints endpoints, Metadata metadata,
                                                            MultiplexingRequest request, long duration,
                                                            TimeUnit timeUnit);

    /**
     * Asynchronously notify server that client is terminated, the method ensures no throwable.
     *
     * @param endpoints request endpoints.
     * @param metadata  gRPC request header metadata.
     * @param request   notify client termination request.
     * @param duration  request max duration.
     * @param timeUnit  duration time unit.
     * @return response future of notification of client termination.
     */
    @SuppressWarnings("UnusedReturnValue")
    ListenableFuture<NotifyClientTerminationResponse> notifyClientTermination(Endpoints endpoints, Metadata metadata,
                                                                              NotifyClientTerminationRequest request,
                                                                              long duration, TimeUnit timeUnit);
}
