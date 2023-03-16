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

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Proto = Apache.Rocketmq.V2;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using NLog;

namespace Org.Apache.Rocketmq
{
    public class RpcClient : IRpcClient
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();
        private readonly Proto::MessagingService.MessagingServiceClient _stub;
        private readonly GrpcChannel _channel;
        private readonly string _target;

        public RpcClient(Endpoints endpoints, bool sslEnabled)
        {
            _target = endpoints.GrpcTarget(sslEnabled);
            _channel = GrpcChannel.ForAddress(_target, new GrpcChannelOptions
            {
                HttpHandler = CreateHttpHandler(),
                // Disable auto-retry.
                MaxRetryAttempts = 0
            });
            var invoker = _channel.Intercept(new ClientLoggerInterceptor());
            _stub = new Proto::MessagingService.MessagingServiceClient(invoker);
        }

        public async Task Shutdown()
        {
            if (null != _channel)
            {
                await _channel.ShutdownAsync();
            }
        }

        private static bool CertValidator(
            object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            // Always return true to disable server certificate validation
            return true;
        }

        internal static HttpMessageHandler CreateHttpHandler()
        {
            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = CertValidator,
            };
            return handler;
        }

        public AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand> Telemetry(Metadata metadata)
        {
            var deadline = DateTime.UtcNow.Add(TimeSpan.FromDays(3650));
            var callOptions = new CallOptions(metadata, deadline);
            return _stub.Telemetry(callOptions);
        }

        public async Task<Proto::QueryRouteResponse> QueryRoute(Metadata metadata, Proto::QueryRouteRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.QueryRouteAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<Proto::HeartbeatResponse> Heartbeat(Metadata metadata, Proto::HeartbeatRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.HeartbeatAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<Proto::SendMessageResponse> SendMessage(Metadata metadata, Proto::SendMessageRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.SendMessageAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<Proto::QueryAssignmentResponse> QueryAssignment(Metadata metadata,
            Proto::QueryAssignmentRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.QueryAssignmentAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<List<Proto::ReceiveMessageResponse>> ReceiveMessage(Metadata metadata,
            Proto::ReceiveMessageRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);
            var call = _stub.ReceiveMessage(request, callOptions);
            Logger.Debug($"ReceiveMessageRequest has been written to {_target}");
            var result = new List<Proto::ReceiveMessageResponse>();
            var stream = call.ResponseStream;
            while (await stream.MoveNext())
            {
                var entry = stream.Current;
                Logger.Debug($"Got ReceiveMessageResponse {entry} from {_target}");
                result.Add(entry);
            }

            Logger.Debug($"Receiving messages from {_target} completed");
            return result;
        }

        public async Task<Proto::AckMessageResponse> AckMessage(Metadata metadata, Proto::AckMessageRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.AckMessageAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<Proto::ChangeInvisibleDurationResponse> ChangeInvisibleDuration(Metadata metadata,
            Proto::ChangeInvisibleDurationRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.ChangeInvisibleDurationAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<Proto::ForwardMessageToDeadLetterQueueResponse> ForwardMessageToDeadLetterQueue(
            Metadata metadata, Proto::ForwardMessageToDeadLetterQueueRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.ForwardMessageToDeadLetterQueueAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<Proto::EndTransactionResponse> EndTransaction(Metadata metadata,
            Proto::EndTransactionRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.EndTransactionAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<Proto::NotifyClientTerminationResponse> NotifyClientTermination(Metadata metadata,
            Proto::NotifyClientTerminationRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.NotifyClientTerminationAsync(request, callOptions);
            return await call.ResponseAsync;
        }
    }
}