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
using grpc = Grpc.Core;
using NLog;
using System.Diagnostics.Metrics;

namespace Org.Apache.Rocketmq
{
    public abstract class Client : ClientConfig, IClient
    {
        protected static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        protected Client(string accessUrl)
        {
            AccessPoint = new AccessPoint(accessUrl);

            AccessPointScheme = AccessPoint.HostScheme();
            var serviceEndpoint = new rmq::Address
            {
                Host = AccessPoint.Host,
                Port = AccessPoint.Port
            };
            AccessPointEndpoints = new List<rmq::Address> { serviceEndpoint };

            _resourceNamespace = "";

            ClientSettings = new rmq::Settings
            {
                AccessPoint = new rmq::Endpoints
                {
                    Scheme = AccessPoint.HostScheme()
                }
            };

            ClientSettings.AccessPoint.Addresses.Add(serviceEndpoint);

            ClientSettings.RequestTimeout = Google.Protobuf.WellKnownTypes.Duration.FromTimeSpan(TimeSpan.FromSeconds(3));

            ClientSettings.UserAgent = new rmq.UA
            {
                Language = rmq::Language.DotNet,
                Version = MetadataConstants.CLIENT_VERSION,
                Platform = Environment.OSVersion.ToString(),
                Hostname = System.Net.Dns.GetHostName()
            };

            Manager = new ClientManager();

            _topicRouteTable = new ConcurrentDictionary<string, TopicRouteData>();
            _updateTopicRouteCts = new CancellationTokenSource();
            _HeartbeatCts = new CancellationTokenSource();
            _telemetryCts = new CancellationTokenSource();
        }

        public virtual async Task Start()
        {
            Schedule(async () =>
            {
                Logger.Debug("Update topic route by schedule");
                await UpdateTopicRoute();

            }, 30, _updateTopicRouteCts.Token);

            // Get routes for topics of interest.
            Logger.Debug("Step of #Start: get route for topics of interest");
            await UpdateTopicRoute();

            string accessPointUrl = AccessPoint.TargetUrl();
            CreateSession(accessPointUrl);
            await _sessions[accessPointUrl].AwaitSettingNegotiationCompletion();
            Logger.Debug($"Session has been created for {accessPointUrl}");

            Schedule(async () =>
            {
                Logger.Debug("Sending heartbeat by schedule");
                await Heartbeat();

            }, 10, _HeartbeatCts.Token);
            await Heartbeat();
        }

        public virtual async Task Shutdown()
        {
            Logger.Info($"Shutdown client");
            _updateTopicRouteCts.Cancel();
            _HeartbeatCts.Cancel();
            _telemetryCts.Cancel();
            await Manager.Shutdown();
        }

        private string FilterBroker(Func<string, bool> acceptor)
        {
            foreach (var item in _topicRouteTable)
            {
                foreach (var partition in item.Value.MessageQueues)
                {
                    var target = Utilities.TargetUrl(partition);
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
            var endpoints = new List<string>();
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
            HashSet<string> topics = new HashSet<string>(_topicsOfInterest.Keys);

            foreach (var item in _topicRouteTable)
            {
                topics.Add(item.Key);
            }
            Logger.Debug($"Fetch topic route for {topics.Count} topics");

            // Wrap topics into list such that we can map async result to topic 
            List<string> topicList = new List<string>();
            topicList.AddRange(topics);

            var tasks = new List<Task<TopicRouteData>>();
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

        protected void Schedule(Action action, int seconds, CancellationToken token)
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
        protected async Task<TopicRouteData> GetRouteFor(string topic, bool direct)
        {
            Logger.Debug($"Get route for topic={topic}, direct={direct}");
            if (!direct && _topicRouteTable.TryGetValue(topic, out var routeData))
            {
                Logger.Debug($"Return cached route for {topic}");
                return routeData;
            }

            // We got one or more name servers available.
            var request = new rmq::QueryRouteRequest
            {
                Topic = new rmq::Resource
                {
                    ResourceNamespace = _resourceNamespace,
                    Name = topic
                },
                Endpoints = new rmq::Endpoints
                {
                    Scheme = AccessPointScheme,
                    Addresses = { AccessPointEndpoints },
                }
            };

            var metadata = new grpc.Metadata();
            Signature.Sign(this, metadata);
            int index = _random.Next(0, AccessPointEndpoints.Count);
            var serviceEndpoint = AccessPointEndpoints[index];
            // AccessPointAddresses.Count
            string target = $"https://{serviceEndpoint.Host}:{serviceEndpoint.Port}";
            try
            {
                Logger.Debug($"Resolving route for topic={topic}");
                var topicRouteData = await Manager.ResolveRoute(target, metadata, request, RequestTimeout);
                if (null != topicRouteData)
                {
                    Logger.Debug($"Got route entries for {topic} from name server");
                    _topicRouteTable.TryAdd(topic, topicRouteData);
                    Logger.Debug($"Got route for {topic} from {target}");
                    return topicRouteData;
                }
                Logger.Warn($"Failed to query route of {topic} from {target}");
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

            var request = new rmq::HeartbeatRequest
            {
                Group = null,
                ClientType = rmq.ClientType.Unspecified
            };
            PrepareHeartbeatData(request);

            var metadata = new grpc::Metadata();
            Signature.Sign(this, metadata);

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

        protected async Task<List<rmq::Assignment>> ScanLoadAssignment(string topic, string group)
        {
            // Pick a broker randomly
            string target = FilterBroker((s) => true);
            var request = new rmq::QueryAssignmentRequest
            {
                Topic = new rmq::Resource
                {
                    ResourceNamespace = _resourceNamespace,
                    Name = topic
                },
                Group = new rmq::Resource
                {
                    ResourceNamespace = _resourceNamespace,
                    Name = group
                },
                Endpoints = new rmq::Endpoints
                {
                    Scheme = AccessPointScheme,
                    Addresses = { AccessPointEndpoints },
                }
            };
            
            try
            {
                var metadata = new grpc::Metadata();
                Signature.Sign(this, metadata);
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
            settings.MergeFrom(ClientSettings);
        }

        private async Task CreateSession(string url)
        {
            Logger.Debug($"Create session for url={url}");
            var metadata = new grpc::Metadata();
            Signature.Sign(this, metadata);
            var stream = Manager.Telemetry(url, metadata);
            var session = new Session(url, stream, this);
            _sessions.TryAdd(url, session);
            await session.Loop();
        }

        internal async Task<List<Message>> ReceiveMessage(rmq::Assignment assignment, string group)
        {
            var targetUrl = TargetUrl(assignment);
            var metadata = new grpc::Metadata();
            Signature.Sign(this, metadata);
            var request = new rmq::ReceiveMessageRequest
            {
                Group = new rmq::Resource
                {
                    ResourceNamespace = _resourceNamespace,
                    Name = group
                },
                MessageQueue = assignment.MessageQueue
            };
            var messages = await Manager.ReceiveMessage(targetUrl, metadata, request, 
                ClientSettings.Subscription.LongPollingTimeout.ToTimeSpan());
            return messages;
        }

        public async Task<Boolean> Ack(string target, string group, string topic, string receiptHandle, String messageId)
        {
            var request = new rmq::AckMessageRequest
            {
                Group = new rmq::Resource
                {
                    ResourceNamespace = _resourceNamespace,
                    Name = group
                },
                Topic = new rmq::Resource
                {
                    ResourceNamespace = _resourceNamespace,
                    Name = topic
                }
            };

            var entry = new rmq::AckMessageEntry
            {
                ReceiptHandle = receiptHandle,
                MessageId = messageId
            };
            request.Entries.Add(entry);

            var metadata = new grpc::Metadata();
            Signature.Sign(this, metadata);
            return await Manager.Ack(target, metadata, request, RequestTimeout);
        }

        public async Task<Boolean> ChangeInvisibleDuration(string target, string group, string topic, string receiptHandle, String messageId)
        {
            var request = new rmq::ChangeInvisibleDurationRequest
            {
                ReceiptHandle = receiptHandle,
                Group = new rmq::Resource
                {
                    ResourceNamespace = _resourceNamespace,
                    Name = group
                },
                Topic = new rmq::Resource
                {
                    ResourceNamespace = _resourceNamespace,
                    Name = topic
                },
                MessageId = messageId
            };

            var metadata = new grpc::Metadata();
            Signature.Sign(this, metadata);
            return await Manager.ChangeInvisibleDuration(target, metadata, request, RequestTimeout);
        }

        public async Task<bool> NotifyClientTermination(rmq.Resource group)
        {
            List<string> endpoints = AvailableBrokerEndpoints();
            var request = new rmq::NotifyClientTerminationRequest
            {
                Group = group
            };
            var metadata = new grpc.Metadata();
            Signature.Sign(this, metadata);

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

        internal virtual void OnSettingsReceived(rmq::Settings settings)
        {
            if (null != settings.Metric)
            {
                ClientSettings.Metric = new rmq::Metric();
                ClientSettings.Metric.MergeFrom(settings.Metric);
            }

            if (null != settings.BackoffPolicy)
            {
                ClientSettings.BackoffPolicy = new rmq::RetryPolicy();
                ClientSettings.BackoffPolicy.MergeFrom(settings.BackoffPolicy);
            }

            switch (settings.PubSubCase)
            {
                case rmq.Settings.PubSubOneofCase.Publishing:
                {
                    ClientSettings.Publishing = settings.Publishing;
                    break;
                }

                case rmq.Settings.PubSubOneofCase.Subscription:
                {
                    ClientSettings.Subscription = settings.Subscription;
                    break;
                }
            }
        }

        protected readonly IClientManager Manager;

        protected readonly ConcurrentDictionary<string, bool> _topicsOfInterest = new ();

        public void AddTopicOfInterest(string topic)
        {
            _topicsOfInterest.TryAdd(topic, true);
        }

        public void RemoveTopicOfInterest(string topic)
        {
            _topicsOfInterest.TryRemove(topic, out var _);
        }

        private readonly ConcurrentDictionary<string, TopicRouteData> _topicRouteTable;
        private readonly CancellationTokenSource _updateTopicRouteCts;
        private readonly CancellationTokenSource _HeartbeatCts;
        private readonly CancellationTokenSource _telemetryCts;

        public CancellationTokenSource TelemetryCts()
        {
            return _telemetryCts;
        }

        protected readonly AccessPoint AccessPoint;

        // This field is subject changes from servers.
        protected readonly rmq::Settings ClientSettings;

        private readonly Random _random = new Random();

        private readonly ConcurrentDictionary<string, Session> _sessions = new ConcurrentDictionary<string, Session>();

        protected const string MeterName = "Apache.RocketMQ.Client";

        protected static readonly Meter MetricMeter = new(MeterName, "1.0");
    }
}