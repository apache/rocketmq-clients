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
using System.Threading.Tasks;
using grpc = global::Grpc.Core;
using System.Collections.Generic;

namespace org.apache.rocketmq {
    public class ClientManager : IClientManager {

        public ClientManager() {
            rpcClients = new ConcurrentDictionary<string, RpcClient>();
        }

        public IRpcClient getRpcClient(string target) {
            if (!rpcClients.ContainsKey(target)) {
                using var channel = GrpcChannel.ForAddress(target);
                var client = new rmq.MessagingService.MessagingServiceClient(channel);
                var rpcClient = new RpcClient(client);
                if(rpcClients.TryAdd(target, rpcClient)) {
                    return rpcClient;
                }
            }
            return rpcClients[target];
        }

        public async Task<TopicRouteData> resolveRoute(string target, grpc::Metadata metadata, rmq.QueryRouteRequest request, TimeSpan timeout) {
            var rpcClient = getRpcClient(target);
            var callOptions = new grpc::CallOptions();
            callOptions.WithDeadline(DateTime.Now.Add(timeout))
                .WithHeaders(metadata);
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
                    case rmq.Permission.None:
                    {
                        permission = Permission.NONE;
                        break;
                    }
                    case rmq.Permission.Read:
                    {
                        permission = Permission.READ;
                        break;
                    }
                    case rmq.Permission.Write:
                    {
                        permission = Permission.WRITE;
                        break;
                    }
                    case rmq.Permission.ReadWrite:
                    {
                        permission = Permission.READ_WRITE;
                        break;
                    }
                }

                AddressScheme scheme = AddressScheme.IPv4;
                switch(partition.Broker.Endpoints.Scheme) {
                    case rmq.AddressScheme.Ipv4: {
                        scheme = AddressScheme.IPv4;
                        break;
                    }
                    case rmq.AddressScheme.Ipv6: {
                        scheme = AddressScheme.IPv6;
                        break;
                    }
                    case rmq.AddressScheme.DomainName: {
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

        private ConcurrentDictionary<string, RpcClient> rpcClients;

    }
}