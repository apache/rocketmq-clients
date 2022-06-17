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

using System.Collections.Concurrent;

using rmq = global::apache.rocketmq.v1;
using Grpc.Net.Client;
using System;
using System.Threading;
using System.Threading.Tasks;
using grpc = global::Grpc.Core;
using System.Collections.Generic;
using Grpc.Core.Interceptors;
using System.Net.Http;

namespace org.apache.rocketmq {
    public class ClientManager : IClientManager {

        public ClientManager() {
            rpcClients = new ConcurrentDictionary<string, RpcClient>();
        }

        public IRpcClient getRpcClient(string target) {
            if (!rpcClients.ContainsKey(target)) {
                var channel = GrpcChannel.ForAddress(target, new GrpcChannelOptions {
                    HttpHandler = createHttpHandler()
                });
                var invoker = channel.Intercept(new ClientLoggerInterceptor());
                var client = new rmq::MessagingService.MessagingServiceClient(invoker);
                var rpcClient = new RpcClient(client);
                if(rpcClients.TryAdd(target, rpcClient)) {
                    return rpcClient;
                }
            }
            return rpcClients[target];
        }

        /**
         * See https://docs.microsoft.com/en-us/aspnet/core/grpc/performance?view=aspnetcore-6.0 for performance consideration and
         * why parameters are configured this way.
         */
        public static HttpMessageHandler createHttpHandler()
        {
            var sslOptions = new System.Net.Security.SslClientAuthenticationOptions();
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

        public async Task<TopicRouteData> resolveRoute(string target, grpc::Metadata metadata, rmq::QueryRouteRequest request, TimeSpan timeout)
        {
            var rpcClient = getRpcClient(target);
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new grpc::CallOptions(metadata, deadline);
            var queryRouteResponse = await rpcClient.queryRoute(request, callOptions);

            if (queryRouteResponse.Common.Status.Code != ((int)Google.Rpc.Code.Ok)) {
                // Raise an application layer exception

            }

            var partitions = new List<Partition>();
            // Translate protobuf object to domain specific one
            foreach (var partition in queryRouteResponse.Partitions)
            {
                var topic = new Topic(partition.Topic.ResourceNamespace, partition.Topic.Name);
                var id = partition.Id;
                Permission permission = Permission.READ_WRITE;
                switch (partition.Permission) {
                    case rmq::Permission.None:
                    {
                        permission = Permission.NONE;
                        break;
                    }
                    case rmq::Permission.Read:
                    {
                        permission = Permission.READ;
                        break;
                    }
                    case rmq::Permission.Write:
                    {
                        permission = Permission.WRITE;
                        break;
                    }
                    case rmq::Permission.ReadWrite:
                    {
                        permission = Permission.READ_WRITE;
                        break;
                    }
                }

                AddressScheme scheme = AddressScheme.IPv4;
                switch(partition.Broker.Endpoints.Scheme) {
                    case rmq::AddressScheme.Ipv4:
                        {
                        scheme = AddressScheme.IPv4;
                        break;
                    }
                    case rmq::AddressScheme.Ipv6:
                        {
                        scheme = AddressScheme.IPv6;
                        break;
                    }
                    case rmq::AddressScheme.DomainName:
                        {
                        scheme = AddressScheme.DOMAIN_NAME;
                        break;
                    }
                }

                List<Address> addresses = new List<Address>();
                foreach(var item in partition.Broker.Endpoints.Addresses) {
                    addresses.Add(new Address(item.Host, item.Port));
                }
                ServiceAddress serviceAddress = new ServiceAddress(scheme, addresses);
                Broker broker = new Broker(partition.Broker.Name, id, serviceAddress);
                partitions.Add(new Partition(topic, broker, id, permission));
            }

            var topicRouteData = new TopicRouteData(partitions);
            return topicRouteData;
        }

        public async Task<Boolean> heartbeat(string target, grpc::Metadata metadata, rmq::HeartbeatRequest request, TimeSpan timeout)
        {
            var rpcClient = getRpcClient(target);
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new grpc.CallOptions(metadata, deadline);
            var response = await rpcClient.heartbeat(request, callOptions);
            if (null == response)
            {
                return false;
            }

            return response.Common.Status.Code == (int)Google.Rpc.Code.Ok;
        }

        public async Task<rmq::SendMessageResponse> sendMessage(string target, grpc::Metadata metadata, rmq::SendMessageRequest request, TimeSpan timeout)
        {
            var rpcClient = getRpcClient(target);
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new grpc::CallOptions(metadata, deadline);
            var response = await rpcClient.sendMessage(request, callOptions);
            return response;
        }

        public async Task<Boolean> notifyClientTermination(string target, grpc::Metadata metadata, rmq::NotifyClientTerminationRequest request, TimeSpan timeout)
        {
            var rpcClient = getRpcClient(target);
            var deadline = DateTime.UtcNow.Add(timeout);
            var callOptions = new grpc::CallOptions(metadata, deadline);
            rmq::NotifyClientTerminationResponse response = await rpcClient.notifyClientTermination(request, callOptions);
            return response.Common.Status.Code == ((int)Google.Rpc.Code.Ok);
        }

        private ConcurrentDictionary<string, RpcClient> rpcClients;

    }
}