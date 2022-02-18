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

using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using System;
using rmq = apache.rocketmq.v1;
using grpc = global::Grpc.Core;


namespace org.apache.rocketmq
{
    public abstract class Client : ClientConfig, IClient
    {

        public Client(INameServerResolver resolver)
        {
            this.nameServerResolver = resolver;
            this.clientManager = ClientManagerFactory.getClientManager(resourceNamespace());
            this.nameServerResolverCTS = new CancellationTokenSource();

            this.topicRouteTable = new ConcurrentDictionary<string, TopicRouteData>();
            this.updateTopicRouteCTS = new CancellationTokenSource();
        }

        public virtual void start()
        {
            schedule(async () =>
            {
                await updateNameServerList();
            }, 30, nameServerResolverCTS.Token);

            schedule(async () =>
            {
                await updateTopicRoute();

            }, 30, updateTopicRouteCTS.Token);

        }

        public virtual void shutdown()
        {
            updateTopicRouteCTS.Cancel();
            nameServerResolverCTS.Cancel();
        }

        private async Task updateNameServerList()
        {
            List<string> nameServers = await nameServerResolver.resolveAsync();
            if (0 == nameServers.Count)
            {
                // Whoops, something should be wrong. We got an empty name server list.
                return;
            }

            if (nameServers.Equals(this.nameServers))
            {
                return;
            }

            // Name server list is updated. 
            // TODO: Locking is required
            this.nameServers = nameServers;
            this.currentNameServerIndex = 0;
        }

        private async Task updateTopicRoute()
        {
            if (null == nameServers || 0 == nameServers.Count)
            {
                List<string> list = await nameServerResolver.resolveAsync();
                if (null != list && 0 != list.Count)
                {
                    this.nameServers = list;
                }
                else
                {
                    // TODO: log warning here.
                    return;
                }
            }

            // We got one or more name servers available.
            string nameServer = nameServers[currentNameServerIndex];

            List<Task<TopicRouteData>> tasks = new List<Task<TopicRouteData>>();
            foreach (var item in topicRouteTable)
            {
                tasks.Add(getRouteFor(item.Key, true));
            }

            // Update topic route data
            TopicRouteData[] result = await Task.WhenAll(tasks);
            foreach (var item in result)
            {
                if (null == item)
                {
                    continue;
                }

                if (0 == item.Partitions.Count)
                {
                    continue;
                }

                var topicName = item.Partitions[0].Topic.Name;
                var existing = topicRouteTable[topicName];
                if (!existing.Equals(item))
                {
                    topicRouteTable[topicName] = item;
                }
            }
        }

        public void schedule(Action action, int seconds, CancellationToken token)
        {
            if (null == action)
            {
                // TODO: log warning
                return;
            }

            Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    action();
                    await Task.Delay(TimeSpan.FromSeconds(seconds), token);
                }
            });
        }

        /**
         * Parameters:
         * topic
         *    Topic to query
         * direct
         *    Indicate if we should by-pass cache and fetch route entries from name server.
         */
        public async Task<TopicRouteData> getRouteFor(string topic, bool direct)
        {
            if (!direct && topicRouteTable.ContainsKey(topic))
            {
                return topicRouteTable[topic];
            }

            if (null == nameServers || 0 == nameServers.Count)
            {
                List<string> list = await nameServerResolver.resolveAsync();
                if (null != list && 0 != list.Count)
                {
                    this.nameServers = list;
                }
                else
                {
                    // TODO: log warning here.
                    return null;
                }
            }

            // We got one or more name servers available.
            string nameServer = nameServers[currentNameServerIndex];
            var metadata = new grpc.Metadata();
            Signature.sign(this, metadata);
            var request = new rmq::QueryRouteRequest();
            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = resourceNamespace();
            request.Topic.Name = topic;
            request.Endpoints = new rmq::Endpoints();
            request.Endpoints.Scheme = rmq::AddressScheme.Ipv4;
            var address = new rmq::Address();
            string[] segments = nameServer.Split(":");
            address.Host = segments[0];
            address.Port = Int32.Parse(segments[1]);
            request.Endpoints.Addresses.Add(address);
            var target = string.Format("https://{0}:{1}", segments[0], segments[1]);
            var topicRouteData = await clientManager.resolveRoute(target, metadata, request, getIoTimeout());
            return topicRouteData;
        }

        public abstract void prepareHeartbeatData(rmq::HeartbeatRequest request);

        public void heartbeat()
        {
            List<string> endpoints = endpointsInUse();
            if (0 == endpoints.Count)
            {
                return;
            }

            var heartbeatRequest = new rmq::HeartbeatRequest();
            prepareHeartbeatData(heartbeatRequest);

            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
        }

        public void healthCheck()
        {

        }

        public async Task<bool> notifyClientTermination()
        {
            List<string> endpoints = endpointsInUse();
            var request = new rmq::NotifyClientTerminationRequest();
            request.ClientId = clientId();

            var metadata = new grpc.Metadata();
            Signature.sign(this, metadata);

            List<Task<Boolean>> tasks = new List<Task<Boolean>>();

            foreach (var endpoint in endpoints)
            {
                tasks.Add(clientManager.notifyClientTermination(endpoint, metadata, request, getIoTimeout()));
            }

            bool[] results = await Task.WhenAll(tasks);
            foreach (bool b in results)
            {
                if (!b)
                {
                    return false;
                }
            }
            return true;
        }

        private List<string> endpointsInUse()
        {
            //TODO: gather endpoints from route entries.
            return new List<string>();
        }

        protected IClientManager clientManager;
        private INameServerResolver nameServerResolver;
        private CancellationTokenSource nameServerResolverCTS;
        private List<string> nameServers;
        private int currentNameServerIndex;

        private ConcurrentDictionary<string, TopicRouteData> topicRouteTable;
        private CancellationTokenSource updateTopicRouteCTS;
    }
}