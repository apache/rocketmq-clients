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
using System.Threading;
using System.Threading.Tasks;
using rmq = Apache.Rocketmq.V2;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using NLog;

namespace Org.Apache.Rocketmq
{
    public class RpcClient : IRpcClient
    {
        protected static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();
        private readonly rmq::MessagingService.MessagingServiceClient _stub;
        private readonly GrpcChannel _channel;

        public RpcClient(string target)
        {
            _channel = GrpcChannel.ForAddress(target, new GrpcChannelOptions
            {
                HttpHandler = CreateHttpHandler()
            });
            var invoker = _channel.Intercept(new ClientLoggerInterceptor());
            _stub = new rmq::MessagingService.MessagingServiceClient(invoker);
        }

        public async Task Shutdown()
        {
            if (null != _channel)
            {
                await _channel.ShutdownAsync();
            }
        }

        /**
         * See https://docs.microsoft.com/en-us/aspnet/core/grpc/performance?view=aspnetcore-6.0 for performance consideration and
         * why parameters are configured this way.
         */
        private HttpMessageHandler CreateHttpHandler()
        {
            var sslOptions = new SslClientAuthenticationOptions();
            // Disable server certificate validation during development phase.
            // Comment out the following line if server certificate validation is required. 
            sslOptions.RemoteCertificateValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
            var handler = new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                KeepAlivePingDelay = TimeSpan.FromSeconds(60),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
                EnableMultipleHttp2Connections = true,
                SslOptions = sslOptions,
            };
            return handler;
        }

        public AsyncDuplexStreamingCall<rmq::TelemetryCommand, rmq::TelemetryCommand> Telemetry(Metadata metadata)
        {
            var deadline = DateTime.UtcNow.Add(TimeSpan.FromSeconds(3));
            var callOptions = new CallOptions(metadata, deadline);
            return _stub.Telemetry(callOptions);
        }

        public async Task<rmq::QueryRouteResponse> QueryRoute(Metadata metadata, rmq::QueryRouteRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.QueryRouteAsync(request, callOptions);
            return await call.ResponseAsync;
        }


        public async Task<rmq::HeartbeatResponse> Heartbeat(Metadata metadata, rmq::HeartbeatRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.HeartbeatAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<rmq::SendMessageResponse> SendMessage(Metadata metadata, rmq::SendMessageRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.SendMessageAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<rmq::QueryAssignmentResponse> QueryAssignment(Metadata metadata, rmq::QueryAssignmentRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.QueryAssignmentAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<List<rmq::ReceiveMessageResponse>> ReceiveMessage(Metadata metadata, 
            rmq::ReceiveMessageRequest request, TimeSpan timeout) {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);
            var call = _stub.ReceiveMessage(request, callOptions);
            var result = new List<rmq::ReceiveMessageResponse>();
            var stream = call.ResponseStream;
            while (await stream.MoveNext())
            {
                var entry = stream.Current;
                Logger.Debug($"Got ReceiveMessageResponse {entry}");
                result.Add(entry);
            }
            Logger.Debug($"Receiving of messages completed");
            return result;
        }

        public async Task<rmq::AckMessageResponse> AckMessage(Metadata metadata, rmq::AckMessageRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.AckMessageAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<rmq::ChangeInvisibleDurationResponse> ChangeInvisibleDuration(Metadata metadata, rmq::ChangeInvisibleDurationRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.ChangeInvisibleDurationAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<rmq::ForwardMessageToDeadLetterQueueResponse> ForwardMessageToDeadLetterQueue(Metadata metadata,
            rmq::ForwardMessageToDeadLetterQueueRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.ForwardMessageToDeadLetterQueueAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<rmq::EndTransactionResponse> EndTransaction(Metadata metadata, rmq::EndTransactionRequest request,
            TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.EndTransactionAsync(request, callOptions);
            return await call.ResponseAsync;
        }

        public async Task<rmq::NotifyClientTerminationResponse> NotifyClientTermination(Metadata metadata,
            rmq::NotifyClientTerminationRequest request, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new CallOptions(metadata, deadline);

            var call = _stub.NotifyClientTerminationAsync(request, callOptions);
            return await call.ResponseAsync;
        }
    }
}