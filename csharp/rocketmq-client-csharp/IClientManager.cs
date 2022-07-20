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
using grpc = global::Grpc.Core;
using rmq = Apache.Rocketmq.V2;


namespace Org.Apache.Rocketmq
{
    public interface IClientManager
    {
        IRpcClient GetRpcClient(string target);

        grpc::AsyncDuplexStreamingCall<rmq::TelemetryCommand, rmq::TelemetryCommand> Telemetry(string target, grpc::Metadata metadata);

        Task<TopicRouteData> ResolveRoute(string target, grpc::Metadata metadata, rmq::QueryRouteRequest request, TimeSpan timeout);

        Task<Boolean> Heartbeat(string target, grpc::Metadata metadata, rmq::HeartbeatRequest request, TimeSpan timeout);

        Task<Boolean> NotifyClientTermination(string target, grpc::Metadata metadata, rmq::NotifyClientTerminationRequest request, TimeSpan timeout);

        Task<rmq::SendMessageResponse> SendMessage(string target, grpc::Metadata metadata, rmq::SendMessageRequest request, TimeSpan timeout);

        Task<List<rmq::Assignment>> QueryLoadAssignment(string target, grpc::Metadata metadata, rmq::QueryAssignmentRequest request, TimeSpan timeout);

        Task<List<Message>> ReceiveMessage(string target, grpc::Metadata metadata, rmq::ReceiveMessageRequest request, TimeSpan timeout);

        Task<Boolean> Ack(string target, grpc::Metadata metadata, rmq::AckMessageRequest request, TimeSpan timeout);

        Task<Boolean> ChangeInvisibleDuration(string target, grpc::Metadata metadata, rmq::ChangeInvisibleDurationRequest request, TimeSpan timeout);

        Task Shutdown();
    }
}