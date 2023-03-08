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

using Proto = Apache.Rocketmq.V2;
using System;
using System.Threading;
using System.Threading.Tasks;
using grpc = Grpc.Core;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.Rocketmq
{
    public class ClientManager : IClientManager
    {
        private readonly Client _client;
        private readonly Dictionary<Endpoints, IRpcClient> _rpcClients;
        private readonly ReaderWriterLockSlim _clientLock;

        public ClientManager(Client client)
        {
            _client = client;
            _rpcClients = new Dictionary<Endpoints, IRpcClient>();
            _clientLock = new ReaderWriterLockSlim();
        }

        private IRpcClient GetRpcClient(Endpoints endpoints)
        {
            _clientLock.EnterReadLock();
            try
            {
                // client exists, return in advance.
                if (_rpcClients.TryGetValue(endpoints, out var cachedClient))
                {
                    return cachedClient;
                }
            }
            finally
            {
                _clientLock.ExitReadLock();
            }

            _clientLock.EnterWriteLock();
            try
            {
                // client exists, return in advance.
                if (_rpcClients.TryGetValue(endpoints, out var cachedClient))
                {
                    return cachedClient;
                }

                // client does not exist, generate a new one
                var client = new RpcClient(endpoints, _client.GetClientConfig().SslEnabled);
                _rpcClients.Add(endpoints, client);
                return client;
            }
            finally
            {
                _clientLock.ExitWriteLock();
            }
        }

        public async Task Shutdown()
        {
            _clientLock.EnterReadLock();
            try
            {
                var tasks = _rpcClients.Select(item => item.Value.Shutdown()).ToList();

                await Task.WhenAll(tasks);
            }
            finally
            {
                _clientLock.ExitReadLock();
            }
        }

        public grpc::AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand> Telemetry(
            Endpoints endpoints)
        {
            return GetRpcClient(endpoints).Telemetry(_client.Sign());
        }

        public async Task<RpcInvocation<Proto.QueryRouteRequest, Proto.QueryRouteResponse>> QueryRoute(
            Endpoints endpoints, Proto.QueryRouteRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await GetRpcClient(endpoints).QueryRoute(metadata, request, timeout);
            return new RpcInvocation<Proto.QueryRouteRequest, Proto.QueryRouteResponse>(request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.HeartbeatRequest, Proto.HeartbeatResponse>> Heartbeat(Endpoints endpoints,
            Proto.HeartbeatRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await GetRpcClient(endpoints).Heartbeat(metadata, request, timeout);
            return new RpcInvocation<Proto.HeartbeatRequest, Proto.HeartbeatResponse>(request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.NotifyClientTerminationRequest, Proto.NotifyClientTerminationResponse>>
            NotifyClientTermination(Endpoints endpoints, Proto.NotifyClientTerminationRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await GetRpcClient(endpoints).NotifyClientTermination(metadata, request, timeout);
            return new RpcInvocation<Proto.NotifyClientTerminationRequest, Proto.NotifyClientTerminationResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.SendMessageRequest, Proto.SendMessageResponse>> SendMessage(
            Endpoints endpoints, Proto::SendMessageRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await GetRpcClient(endpoints).SendMessage(metadata, request, timeout);
            return new RpcInvocation<Proto.SendMessageRequest, Proto.SendMessageResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.QueryAssignmentRequest, Proto.QueryAssignmentResponse>> QueryAssignment(
            Endpoints endpoints, Proto.QueryAssignmentRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await GetRpcClient(endpoints).QueryAssignment(metadata, request, timeout);
            return new RpcInvocation<Proto.QueryAssignmentRequest, Proto.QueryAssignmentResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.ReceiveMessageRequest, List<Proto.ReceiveMessageResponse>>>
            ReceiveMessage(Endpoints endpoints, Proto.ReceiveMessageRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await GetRpcClient(endpoints).ReceiveMessage(metadata, request, timeout);
            return new RpcInvocation<Proto.ReceiveMessageRequest, List<Proto.ReceiveMessageResponse>>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>> AckMessage(
            Endpoints endpoints, Proto.AckMessageRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await GetRpcClient(endpoints).AckMessage(metadata, request, timeout);
            return new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>>
            ChangeInvisibleDuration(Endpoints endpoints,
                Proto.ChangeInvisibleDurationRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await GetRpcClient(endpoints).ChangeInvisibleDuration(metadata, request, timeout);
            return new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.EndTransactionRequest, Proto.EndTransactionResponse>> EndTransaction(
            Endpoints endpoints, Proto.EndTransactionRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await GetRpcClient(endpoints).EndTransaction(metadata, request, timeout);
            return new RpcInvocation<Proto.EndTransactionRequest, Proto.EndTransactionResponse>(
                request, response, metadata);
        }
    }
}