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

using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using Apache.Rocketmq.V2;
using Grpc.Core;

namespace Org.Apache.Rocketmq
{
    public interface IClientManager
    {
        /// <summary>
        /// Establish a telemetry channel between client and remote endpoints.
        /// </summary>
        /// <param name="endpoints">The target endpoints.</param>
        /// <returns>gRPC bi-directional stream.</returns>
        AsyncDuplexStreamingCall<TelemetryCommand, TelemetryCommand> Telemetry(Endpoints endpoints);

        /// <summary>
        /// Query topic route info from remote.
        /// </summary>
        /// <param name="endpoints">The target endpoints.</param>
        /// <param name="request">gRPC request of querying topic route.</param>
        /// <param name="timeout">Request max duration.</param>
        /// <returns>Task of response.</returns>
        Task<RpcInvocation<QueryRouteRequest, QueryRouteResponse>> QueryRoute(Endpoints endpoints,
            QueryRouteRequest request, TimeSpan timeout);

        /// <summary>
        /// Send heartbeat to remote endpoints.
        /// </summary>
        /// <param name="endpoints">The target endpoints.</param>
        /// <param name="request">gRPC request of heartbeat.</param>
        /// <param name="timeout">Request max duration.</param>
        /// <returns>Task of response.</returns>
        Task<RpcInvocation<HeartbeatRequest, HeartbeatResponse>> Heartbeat(Endpoints endpoints,
            HeartbeatRequest request, TimeSpan timeout);

        /// <summary>
        /// Notify client's termination.
        /// </summary>
        /// <param name="endpoints">The target endpoints.</param>
        /// <param name="request">gRPC request of notifying client's termination.</param>
        /// <param name="timeout">Request max duration.</param>
        /// <returns>Task of response.</returns>
        Task<RpcInvocation<NotifyClientTerminationRequest, NotifyClientTerminationResponse>> NotifyClientTermination(
            Endpoints endpoints, NotifyClientTerminationRequest request, TimeSpan timeout);

        /// <summary>
        /// Send message to remote endpoints.
        /// </summary>
        /// <param name="endpoints">The target endpoints.</param>
        /// <param name="request">gRPC request for message publishing.</param>
        /// <param name="timeout">Request max duration.</param>
        /// <returns></returns>
        Task<RpcInvocation<SendMessageRequest, SendMessageResponse>> SendMessage(Endpoints endpoints,
            SendMessageRequest request, TimeSpan timeout);

        /// <summary>
        /// Query assignment to receive message for push consumer.
        /// </summary>
        /// <param name="endpoints">The target endpoints.</param>
        /// <param name="request">gRPC request for querying assignment.</param>
        /// <param name="timeout">Request max duration.</param>
        /// <returns></returns>
        Task<RpcInvocation<QueryAssignmentRequest, QueryAssignmentResponse>> QueryAssignment(Endpoints endpoints,
            QueryAssignmentRequest request, TimeSpan timeout);

        /// <summary>
        /// Receive message from remote endpoints.
        /// </summary>
        /// <param name="endpoints">The target endpoints.</param>
        /// <param name="request">gRPC request for message receiving.</param>
        /// <param name="timeout">Request max duration.</param>
        /// <returns></returns>
        Task<RpcInvocation<ReceiveMessageRequest, List<ReceiveMessageResponse>>> ReceiveMessage(Endpoints endpoints,
            ReceiveMessageRequest request, TimeSpan timeout);

        /// <summary>
        /// Message acknowledgement towards remote endpoints.
        /// </summary>
        /// <param name="endpoints">The target endpoints.</param>
        /// <param name="request">gRPC request for message acknowledgement.</param>
        /// <param name="timeout">Request max duration.</param>
        /// <returns></returns>
        Task<RpcInvocation<AckMessageRequest, AckMessageResponse>> AckMessage(Endpoints endpoints,
            AckMessageRequest request, TimeSpan timeout);

        /// <summary>
        /// Change message invisible duration.
        /// </summary>
        /// <param name="endpoints">The target endpoints.</param>
        /// <param name="request">gRPC request of changing message invisible duration.</param>
        /// <param name="timeout">Request max duration.</param>
        /// <returns></returns>
        Task<RpcInvocation<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse>> ChangeInvisibleDuration(
            Endpoints endpoints, ChangeInvisibleDurationRequest request, TimeSpan timeout);

        /// <summary>
        /// Transaction ending request.
        /// </summary>
        /// <param name="endpoints">The target endpoints.</param>
        /// <param name="request">gRPC request of ending transaction.</param>
        /// <param name="timeout">Request max duration.</param>
        /// <returns></returns>
        Task<RpcInvocation<EndTransactionRequest, EndTransactionResponse>> EndTransaction(Endpoints endpoints,
            EndTransactionRequest request, TimeSpan timeout);

        Task Shutdown();
    }
}