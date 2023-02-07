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
using System.Linq;
using Proto = Apache.Rocketmq.V2;
using grpc = Grpc.Core;
using NLog;

namespace Org.Apache.Rocketmq
{
    public abstract class Client : IClient
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        private static readonly TimeSpan HeartbeatScheduleDelay = TimeSpan.FromSeconds(10);
        private readonly CancellationTokenSource _heartbeatCts;

        private static readonly TimeSpan TopicRouteUpdateScheduleDelay = TimeSpan.FromSeconds(30);
        private readonly CancellationTokenSource _topicRouteUpdateCtx;

        private static readonly TimeSpan SettingsSyncScheduleDelay = TimeSpan.FromMinutes(5);
        private readonly CancellationTokenSource _settingsSyncCtx;

        protected readonly ClientConfig ClientConfig;
        protected readonly IClientManager Manager;
        protected readonly string ClientId;

        protected readonly ConcurrentDictionary<string, bool> Topics;

        private readonly ConcurrentDictionary<string, TopicRouteData> _topicRouteCache;
        private readonly CancellationTokenSource _telemetryCts;

        private readonly Dictionary<Endpoints, Session> _sessionsTable;
        private readonly ReaderWriterLockSlim _sessionLock;

        protected Client(ClientConfig clientConfig, ConcurrentDictionary<string, bool> topics)
        {
            ClientConfig = clientConfig;
            Topics = topics;
            ClientId = Utilities.GetClientId();

            Manager = new ClientManager(this);

            _topicRouteCache = new ConcurrentDictionary<string, TopicRouteData>();

            _topicRouteUpdateCtx = new CancellationTokenSource();
            _settingsSyncCtx = new CancellationTokenSource();
            _heartbeatCts = new CancellationTokenSource();
            _telemetryCts = new CancellationTokenSource();

            _sessionsTable = new Dictionary<Endpoints, Session>();
            _sessionLock = new ReaderWriterLockSlim();
        }

        public virtual async Task Start()
        {
            Logger.Debug($"Begin to start the rocketmq client, clientId={ClientId}");
            ScheduleWithFixedDelay(UpdateTopicRouteCache, TopicRouteUpdateScheduleDelay, _topicRouteUpdateCtx.Token);
            ScheduleWithFixedDelay(Heartbeat, HeartbeatScheduleDelay, _heartbeatCts.Token);
            ScheduleWithFixedDelay(SyncSettings, SettingsSyncScheduleDelay, _settingsSyncCtx.Token);
            foreach (var topic in Topics.Keys)
            {
                await FetchTopicRoute(topic);
            }

            Logger.Debug($"Start the rocketmq client successfully, clientId={ClientId}");
        }

        public virtual async Task Shutdown()
        {
            Logger.Debug($"Begin to shutdown rocketmq client, clientId={ClientId}");
            _topicRouteUpdateCtx.Cancel();
            _heartbeatCts.Cancel();
            _telemetryCts.Cancel();
            await Manager.Shutdown();
            Logger.Debug($"Shutdown the rocketmq client successfully, clientId={ClientId}");
        }

        private (bool, Session) GetSession(Endpoints endpoints)
        {
            _sessionLock.EnterReadLock();
            try
            {
                // Session exists, return in advance.
                if (_sessionsTable.TryGetValue(endpoints, out var session))
                {
                    return (false, session);
                }
            }
            finally
            {
                _sessionLock.ExitReadLock();
            }

            _sessionLock.EnterWriteLock();
            try
            {
                // Session exists, return in advance.
                if (_sessionsTable.TryGetValue(endpoints, out var session))
                {
                    return (false, session);
                }

                var stream = Manager.Telemetry(endpoints);
                var created = new Session(endpoints, stream, this);
                _sessionsTable.Add(endpoints, created);
                return (true, created);
            }
            finally
            {
                _sessionLock.ExitWriteLock();
            }
        }

        protected abstract Proto::HeartbeatRequest WrapHeartbeatRequest();


        protected abstract void OnTopicDataFetched0(string topic, TopicRouteData topicRouteData);


        private async Task OnTopicRouteDataFetched(string topic, TopicRouteData topicRouteData)
        {
            var routeEndpoints = new HashSet<Endpoints>();
            foreach (var mq in topicRouteData.MessageQueues)
            {
                routeEndpoints.Add(mq.Broker.Endpoints);
            }

            var existedRouteEndpoints = GetTotalRouteEndpoints();
            var newEndpoints = routeEndpoints.Except(existedRouteEndpoints);

            foreach (var endpoints in newEndpoints)
            {
                var (created, session) = GetSession(endpoints);
                if (created)
                {
                    await session.SyncSettings(true);
                }
            }

            _topicRouteCache[topic] = topicRouteData;
            OnTopicDataFetched0(topic, topicRouteData);
        }


        /**
         * Return all endpoints of brokers in route table.
         */
        private HashSet<Endpoints> GetTotalRouteEndpoints()
        {
            var endpoints = new HashSet<Endpoints>();
            foreach (var item in _topicRouteCache)
            {
                foreach (var endpoint in item.Value.MessageQueues.Select(mq => mq.Broker.Endpoints))
                {
                    endpoints.Add(endpoint);
                }
            }

            return endpoints;
        }

        private async void UpdateTopicRouteCache()
        {
            foreach (var topic in Topics.Keys)
            {
                var topicRouteData = await FetchTopicRoute(topic);
                _topicRouteCache[topic] = topicRouteData;
            }
        }

        private async void SyncSettings()
        {
            var totalRouteEndpoints = GetTotalRouteEndpoints();
            foreach (var (_, session) in totalRouteEndpoints.Select(GetSession))
            {
                await session.SyncSettings(false);
            }
        }

        private static void ScheduleWithFixedDelay(Action action, TimeSpan period, CancellationToken token)
        {
            Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    action();
                    await Task.Delay(period, token);
                }
            });
        }

        protected async Task<TopicRouteData> FetchTopicRoute(string topic)
        {
            var topicRouteData = await FetchTopicRoute0(topic);
            await OnTopicRouteDataFetched(topic, topicRouteData);
            Logger.Info(
                $"Fetch topic route successfully, clientId={ClientId}, topic={topic}, topicRouteData={topicRouteData}");
            return topicRouteData;
        }


        private async Task<TopicRouteData> FetchTopicRoute0(string topic)
        {
            var request = new Proto::QueryRouteRequest
            {
                Topic = new Proto::Resource
                {
                    Name = topic
                },
                Endpoints = ClientConfig.Endpoints.ToProtobuf()
            };

            var queryRouteResponse =
                await Manager.QueryRoute(ClientConfig.Endpoints, request, ClientConfig.RequestTimeout);
            var messageQueues = queryRouteResponse.MessageQueues.ToList();
            return new TopicRouteData(messageQueues);
        }

        public async void Heartbeat()
        {
            var endpoints = GetTotalRouteEndpoints();
            if (0 == endpoints.Count)
            {
                Logger.Debug("No broker endpoints available in topic route");
                return;
            }

            var request = WrapHeartbeatRequest();

            var tasks = endpoints.Select(endpoint => Manager.Heartbeat(endpoint, request, ClientConfig.RequestTimeout))
                .Cast<Task>().ToList();

            await Task.WhenAll(tasks);
        }


        public grpc.Metadata Sign()
        {
            var metadata = new grpc::Metadata();
            Signature.Sign(ClientConfig, metadata);
            return metadata;
        }

        public async void NotifyClientTermination(Proto.Resource group)
        {
            var endpoints = GetTotalRouteEndpoints();
            var request = new Proto::NotifyClientTerminationRequest
            {
                Group = group
            };
            foreach (var item in endpoints)
            {
                var response = await Manager.NotifyClientTermination(item, request, ClientConfig.RequestTimeout);
                try
                {
                    StatusChecker.Check(response.Status, request);
                }
                catch (Exception e)
                {
                    Logger.Error(e, $"Failed to notify client's termination, clientId=${ClientId}, " +
                                    $"endpoints=${item}");
                }
            }
        }

        public CancellationTokenSource TelemetryCts()
        {
            return _telemetryCts;
        }

        public abstract Proto.Settings GetSettings();

        public string GetClientId()
        {
            return ClientId;
        }

        public void OnRecoverOrphanedTransactionCommand(Endpoints endpoints,
            Proto.RecoverOrphanedTransactionCommand command)
        {
        }

        public void OnVerifyMessageCommand(Endpoints endpoints, Proto.VerifyMessageCommand command)
        {
        }

        public void OnPrintThreadStackTraceCommand(Endpoints endpoints, Proto.PrintThreadStackTraceCommand command)
        {
        }

        public abstract void OnSettingsCommand(Endpoints endpoints, Proto.Settings settings);
    }
}