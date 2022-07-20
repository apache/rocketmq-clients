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
using System.Diagnostics;
using System;
using rmq = Apache.Rocketmq.V2;
using grpc = global::Grpc.Core;
using NLog;
using System.Diagnostics.Metrics;
using OpenTelemetry;
using OpenTelemetry.Metrics;


namespace Org.Apache.Rocketmq
{
    public abstract class Client : ClientConfig, IClient
    {
        protected static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        protected Client(AccessPoint accessPoint, string resourceNamespace)
        {
            _accessPoint = accessPoint;

            // Support IPv4 for now
            AccessPointScheme = rmq::AddressScheme.Ipv4;
            var serviceEndpoint = new rmq::Address();
            serviceEndpoint.Host = accessPoint.Host;
            serviceEndpoint.Port = accessPoint.Port;
            AccessPointEndpoints = new List<rmq::Address> { serviceEndpoint };

            _resourceNamespace = resourceNamespace;

            _clientSettings = new rmq::Settings();

            _clientSettings.AccessPoint = new rmq::Endpoints();
            _clientSettings.AccessPoint.Scheme = rmq::AddressScheme.Ipv4;
            _clientSettings.AccessPoint.Addresses.Add(serviceEndpoint);

            _clientSettings.RequestTimeout = Google.Protobuf.WellKnownTypes.Duration.FromTimeSpan(TimeSpan.FromSeconds(3));

            _clientSettings.UserAgent = new rmq.UA();
            _clientSettings.UserAgent.Language = rmq::Language.DotNet;
            _clientSettings.UserAgent.Version = "5.0.0";
            _clientSettings.UserAgent.Platform = Environment.OSVersion.ToString();
            _clientSettings.UserAgent.Hostname = System.Net.Dns.GetHostName();

            Manager = ClientManagerFactory.getClientManager(resourceNamespace);

            _topicRouteTable = new ConcurrentDictionary<string, TopicRouteData>();
            _updateTopicRouteCts = new CancellationTokenSource();

            _healthCheckCts = new CancellationTokenSource();

            telemetryCts_ = new CancellationTokenSource();
        }

        public virtual async Task Start()
        {
            schedule(async () =>
            {
                await UpdateTopicRoute();

            }, 30, _updateTopicRouteCts.Token);

            // Get routes for topics of interest.
            await UpdateTopicRoute();

            string accessPointUrl = _accessPoint.TargetUrl();
            createSession(accessPointUrl);

            await _sessions[accessPointUrl].AwaitSettingNegotiationCompletion();

            await Heartbeat();
        }

        public virtual async Task Shutdown()
        {
            Logger.Info($"Shutdown client[resource-namespace={_resourceNamespace}");
            _updateTopicRouteCts.Cancel();
            telemetryCts_.Cancel();
            await Manager.Shutdown();
        }

        protected string FilterBroker(Func<string, bool> acceptor)
        {
            foreach (var item in _topicRouteTable)
            {
                foreach (var partition in item.Value.MessageQueues)
                {
                    string target = Utilities.TargetUrl(partition);
                    if (acceptor(target))
                    {
                        return target;
                    }
                }
            }
            return null;
        }

        /**
         * Return all endpoints of brokers in route table.
         */
        private List<string> AvailableBrokerEndpoints()
        {
            List<string> endpoints = new List<string>();
            foreach (var item in _topicRouteTable)
            {
                foreach (var partition in item.Value.MessageQueues)
                {
                    string endpoint = Utilities.TargetUrl(partition);
                    if (!endpoints.Contains(endpoint))
                    {
                        endpoints.Add(endpoint);
                    }
                }
            }
            return endpoints;
        }

        private async Task UpdateTopicRoute()
        {
            HashSet<string> topics = new HashSet<string>();
            foreach (var topic in topicsOfInterest_)
            {
                topics.Add(topic);
            }

            foreach (var item in _topicRouteTable)
            {
                topics.Add(item.Key);
            }
            Logger.Debug($"Fetch topic route for {topics.Count} topics");

            // Wrap topics into list such that we can map async result to topic 
            List<string> topicList = new List<string>();
            topicList.AddRange(topics);

            List<Task<TopicRouteData>> tasks = new List<Task<TopicRouteData>>();
            foreach (var item in topicList)
            {
                tasks.Add(GetRouteFor(item, true));
            }

            // Update topic route data
            TopicRouteData[] result = await Task.WhenAll(tasks);
            var i = 0;
            foreach (var item in result)
            {
                if (null == item)
                {
                    Logger.Warn($"Failed to fetch route for {topicList[i]}, null response");
                    ++i;
                    continue;
                }

                if (0 == item.MessageQueues.Count)
                {
                    Logger.Warn($"Failed to fetch route for {topicList[i]}, empty message queue");
                    ++i;
                    continue;
                }

                var topicName = item.MessageQueues[0].Topic.Name;

                // Make assertion
                Debug.Assert(topicName.Equals(topicList[i]));

                var existing = _topicRouteTable[topicName];
                if (!existing.Equals(item))
                {
                    _topicRouteTable[topicName] = item;
                }
                ++i;
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
        public async Task<TopicRouteData> GetRouteFor(string topic, bool direct)
        {
            if (!direct && _topicRouteTable.ContainsKey(topic))
            {
                return _topicRouteTable[topic];
            }

            // We got one or more name servers available.
            var request = new rmq::QueryRouteRequest();
            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = _resourceNamespace;
            request.Topic.Name = topic;
            request.Endpoints = new rmq::Endpoints();
            request.Endpoints.Scheme = AccessPointScheme;
            foreach (var address in AccessPointEndpoints)
            {
                request.Endpoints.Addresses.Add(address);
            }

            var metadata = new grpc.Metadata();
            Signature.sign(this, metadata);
            int index = _random.Next(0, AccessPointEndpoints.Count);
            var serviceEndpoint = AccessPointEndpoints[index];
            // AccessPointAddresses.Count
            string target = $"https://{serviceEndpoint.Host}:{serviceEndpoint.Port}";
            TopicRouteData topicRouteData;
            try
            {
                Logger.Debug($"Resolving route for topic={topic}");
                topicRouteData = await Manager.ResolveRoute(target, metadata, request, RequestTimeout);
                if (null != topicRouteData)
                {
                    Logger.Debug($"Got route entries for {topic} from name server");
                    _topicRouteTable.TryAdd(topic, topicRouteData);
                    return topicRouteData;
                }
                else
                {
                    Logger.Warn($"Failed to query route of {topic} from {target}");
                }
            }
            catch (Exception e)
            {
                Logger.Warn(e, "Failed when querying route");
            }

            return null;
        }

        protected abstract void PrepareHeartbeatData(rmq::HeartbeatRequest request);

        public async Task Heartbeat()
        {
            List<string> endpoints = AvailableBrokerEndpoints();
            if (0 == endpoints.Count)
            {
                Logger.Debug("No broker endpoints available in topic route");
                return;
            }

            var request = new rmq::HeartbeatRequest();
            PrepareHeartbeatData(request);

            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);

            List<Task> tasks = new List<Task>();
            foreach (var endpoint in endpoints)
            {
                tasks.Add(Manager.Heartbeat(endpoint, metadata, request, RequestTimeout));
            }

            await Task.WhenAll(tasks);
        }

        private List<string> BlockedBrokerEndpoints()
        {
            List<string> endpoints = new List<string>();
            return endpoints;
        }

        private void RemoveFromBlockList(string endpoint)
        {

        }

        protected async Task<List<rmq::Assignment>> scanLoadAssignment(string topic, string group)
        {
            // Pick a broker randomly
            string target = FilterBroker((s) => true);
            var request = new rmq::QueryAssignmentRequest();
            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = _resourceNamespace;
            request.Topic.Name = topic;
            request.Group = new rmq::Resource();
            request.Group.ResourceNamespace = _resourceNamespace;
            request.Group.Name = group;
            request.Endpoints = new rmq::Endpoints();
            request.Endpoints.Scheme = AccessPointScheme;
            foreach (var endpoint in AccessPointEndpoints)
            {
                request.Endpoints.Addresses.Add(endpoint);
            }
            try
            {
                var metadata = new grpc::Metadata();
                Signature.sign(this, metadata);
                return await Manager.QueryLoadAssignment(target, metadata, request, RequestTimeout);
            }
            catch (System.Exception e)
            {
                Logger.Warn(e, $"Failed to acquire load assignments from {target}");
            }
            // Just return an empty list.
            return new List<rmq.Assignment>();
        }

        private string TargetUrl(rmq::Assignment assignment)
        {
            var broker = assignment.MessageQueue.Broker;
            var addresses = broker.Endpoints.Addresses;
            // TODO: use the first address for now. 
            var address = addresses[0];
            return $"https://{address.Host}:{address.Port}";
        }

        public virtual void BuildClientSetting(rmq::Settings settings)
        {
            settings.MergeFrom(_clientSettings);
        }

        public void createSession(string url)
        {
            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
            var stream = Manager.Telemetry(url, metadata);
            var session = new Session(url, stream, this);
            _sessions.TryAdd(url, session);
            Task.Run(async () =>
            {
                await session.Loop();
            });
        }


        public async Task<List<Message>> ReceiveMessage(rmq::Assignment assignment, string group)
        {
            var targetUrl = TargetUrl(assignment);
            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
            var request = new rmq::ReceiveMessageRequest();
            request.Group = new rmq::Resource();
            request.Group.ResourceNamespace = _resourceNamespace;
            request.Group.Name = group;
            request.MessageQueue = assignment.MessageQueue;
            var messages = await Manager.ReceiveMessage(targetUrl, metadata, request, getLongPollingTimeout());
            return messages;
        }

        public async Task<Boolean> Ack(string target, string group, string topic, string receiptHandle, String messageId)
        {
            var request = new rmq::AckMessageRequest();
            request.Group = new rmq::Resource();
            request.Group.ResourceNamespace = _resourceNamespace;
            request.Group.Name = group;

            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = _resourceNamespace;
            request.Topic.Name = topic;

            var entry = new rmq::AckMessageEntry();
            entry.ReceiptHandle = receiptHandle;
            entry.MessageId = messageId;
            request.Entries.Add(entry);

            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
            return await Manager.Ack(target, metadata, request, RequestTimeout);
        }

        public async Task<Boolean> ChangeInvisibleDuration(string target, string group, string topic, string receiptHandle, String messageId)
        {
            var request = new rmq::ChangeInvisibleDurationRequest();
            request.ReceiptHandle = receiptHandle;
            request.Group = new rmq::Resource();
            request.Group.ResourceNamespace = _resourceNamespace;
            request.Group.Name = group;

            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = _resourceNamespace;
            request.Topic.Name = topic;

            request.MessageId = messageId;

            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);
            return await Manager.ChangeInvisibleDuration(target, metadata, request, RequestTimeout);
        }

        public async Task<bool> NotifyClientTermination()
        {
            List<string> endpoints = AvailableBrokerEndpoints();
            var request = new rmq::NotifyClientTerminationRequest();


            var metadata = new grpc.Metadata();
            Signature.sign(this, metadata);

            List<Task<Boolean>> tasks = new List<Task<Boolean>>();

            foreach (var endpoint in endpoints)
            {
                tasks.Add(Manager.NotifyClientTermination(endpoint, metadata, request, RequestTimeout));
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

        public virtual void OnSettingsReceived(rmq::Settings settings)
        {
            if (null != settings.Metric)
            {
                _clientSettings.Metric = new rmq::Metric();
                _clientSettings.Metric.MergeFrom(settings.Metric);
            }

            if (null != settings.BackoffPolicy)
            {
                _clientSettings.BackoffPolicy = new rmq::RetryPolicy();
                _clientSettings.BackoffPolicy.MergeFrom(settings.BackoffPolicy);
            }
        }

        protected readonly IClientManager Manager;

        private readonly HashSet<string> topicsOfInterest_ = new HashSet<string>();

        public void AddTopicOfInterest(string topic)
        {
            topicsOfInterest_.Add(topic);
        }

        private readonly ConcurrentDictionary<string, TopicRouteData> _topicRouteTable;
        private readonly CancellationTokenSource _updateTopicRouteCts;

        private readonly CancellationTokenSource _healthCheckCts;

        private readonly CancellationTokenSource telemetryCts_ = new CancellationTokenSource();

        public CancellationTokenSource TelemetryCts()
        {
            return telemetryCts_;
        }

        protected readonly AccessPoint _accessPoint;

        // This field is subject changes from servers.
        protected readonly rmq::Settings _clientSettings;

        private readonly Random _random = new Random();
        
        protected readonly ConcurrentDictionary<string, Session> _sessions = new ConcurrentDictionary<string, Session>();

        public static readonly string MeterName = "Apache.RocketMQ.Client";
        
        protected static readonly Meter MetricMeter = new(MeterName, "1.0");
    }
}