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

namespace Org.Apache.Rocketmq
{
    public class ClientManager : IClientManager
    {
        private readonly Client _client;
        private readonly Dictionary<Endpoints, RpcClient> _rpcClients;
        private readonly ReaderWriterLockSlim _clientLock;

        public ClientManager(Client client)
        {
            _client = client;
            _rpcClients = new Dictionary<Endpoints, RpcClient>();
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
                var client = new RpcClient(endpoints);
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
                var tasks = new List<Task>();
                foreach (var item in _rpcClients)
                {
                    tasks.Add(item.Value.Shutdown());
                }

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

        public async Task<Proto.QueryRouteResponse> QueryRoute(Endpoints endpoints, Proto.QueryRouteRequest request,
            TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).QueryRoute(_client.Sign(), request, timeout);
        }

        public async Task<Proto.HeartbeatResponse> Heartbeat(Endpoints endpoints, Proto.HeartbeatRequest request,
            TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).Heartbeat(_client.Sign(), request, timeout);
        }

        public async Task<Proto.NotifyClientTerminationResponse> NotifyClientTermination(Endpoints endpoints,
            Proto.NotifyClientTerminationRequest request, TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).NotifyClientTermination(_client.Sign(), request, timeout);
        }

        public async Task<Proto::SendMessageResponse> SendMessage(Endpoints endpoints,
            Proto::SendMessageRequest request,
            TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).SendMessage(_client.Sign(), request, timeout);
        }

        public async Task<Proto::QueryAssignmentResponse> QueryAssignment(Endpoints endpoints,
            Proto.QueryAssignmentRequest request, TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).QueryAssignment(_client.Sign(), request, timeout);
        }

        public async Task<List<Proto::ReceiveMessageResponse>> ReceiveMessage(Endpoints endpoints,
            Proto.ReceiveMessageRequest request, TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).ReceiveMessage(_client.Sign(), request, timeout);
        }

        public async Task<Proto::AckMessageResponse> AckMessage(Endpoints endpoints,
            Proto.AckMessageRequest request, TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).AckMessage(_client.Sign(), request, timeout);
        }

        public async Task<Proto::ChangeInvisibleDurationResponse> ChangeInvisibleDuration(Endpoints endpoints,
            Proto.ChangeInvisibleDurationRequest request, TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).ChangeInvisibleDuration(_client.Sign(), request, timeout);
        }

        public async Task<Proto.EndTransactionResponse> EndTransaction(Endpoints endpoints,
            Proto.EndTransactionRequest request, TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).EndTransaction(_client.Sign(), request, timeout);
        }
    }
}