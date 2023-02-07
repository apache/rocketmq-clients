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

using rmq = Apache.Rocketmq.V2;
using System;
using System.Threading;
using System.Threading.Tasks;
using grpc = Grpc.Core;
using System.Collections.Generic;

namespace Org.Apache.Rocketmq
{
    public class ClientManager : IClientManager
    {
        private readonly IClient _client;
        private readonly Dictionary<Endpoints, RpcClient> _rpcClients;
        private readonly ReaderWriterLockSlim _clientLock;

        public ClientManager(IClient client)
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
                List<Task> tasks = new List<Task>();
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

        public grpc::AsyncDuplexStreamingCall<rmq::TelemetryCommand, rmq::TelemetryCommand> Telemetry(
            Endpoints endpoints)
        {
            return GetRpcClient(endpoints).Telemetry(_client.Sign());
        }

        public async Task<rmq.QueryRouteResponse> QueryRoute(Endpoints endpoints, rmq.QueryRouteRequest request,
            TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).QueryRoute(_client.Sign(), request, timeout);
        }

        public async Task<rmq.HeartbeatResponse> Heartbeat(Endpoints endpoints, rmq.HeartbeatRequest request,
            TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).Heartbeat(_client.Sign(), request, timeout);
        }

        public async Task<rmq.NotifyClientTerminationResponse> NotifyClientTermination(Endpoints endpoints,
            rmq.NotifyClientTerminationRequest request, TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).NotifyClientTermination(_client.Sign(), request, timeout);
        }

        public async Task<rmq::SendMessageResponse> SendMessage(Endpoints endpoints, rmq::SendMessageRequest request,
            TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).SendMessage(_client.Sign(), request, timeout);
        }

        public async Task<rmq::QueryAssignmentResponse> QueryAssignment(Endpoints endpoints,
            rmq.QueryAssignmentRequest request, TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).QueryAssignment(_client.Sign(), request, timeout);
        }

        public async Task<List<rmq::ReceiveMessageResponse>> ReceiveMessage(Endpoints endpoints,
            rmq.ReceiveMessageRequest request, TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).ReceiveMessage(_client.Sign(), request, timeout);
        }

        public async Task<rmq::AckMessageResponse> AckMessage(Endpoints endpoints,
            rmq.AckMessageRequest request, TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).AckMessage(_client.Sign(), request, timeout);
        }

        public async Task<rmq::ChangeInvisibleDurationResponse> ChangeInvisibleDuration(Endpoints endpoints,
            rmq.ChangeInvisibleDurationRequest request, TimeSpan timeout)
        {
            return await GetRpcClient(endpoints).ChangeInvisibleDuration(_client.Sign(), request, timeout);
        }
    }
}