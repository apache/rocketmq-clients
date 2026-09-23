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
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Proto = Apache.Rocketmq.V2;
using grpcLib = Grpc.Core;

[assembly: InternalsVisibleTo("tests")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]
namespace Org.Apache.Rocketmq
{
    public abstract class Client
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<Client>();

        private static readonly TimeSpan HeartbeatScheduleDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan HeartbeatSchedulePeriod = TimeSpan.FromSeconds(10);
        private readonly CancellationTokenSource _heartbeatCts;

        private static readonly TimeSpan TopicRouteUpdateScheduleDelay = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan TopicRouteUpdateSchedulePeriod = TimeSpan.FromSeconds(30);
        private readonly CancellationTokenSource _topicRouteUpdateCts;

        private static readonly TimeSpan SettingsSyncScheduleDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan SettingsSyncSchedulePeriod = TimeSpan.FromMinutes(5);
        private readonly CancellationTokenSource _settingsSyncCts;

        private static readonly TimeSpan StatsScheduleDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan StatsSchedulePeriod = TimeSpan.FromSeconds(60);
        private readonly CancellationTokenSource _statsCts;

        // Larger than the close timeout of a single session, which force closes itself: this one only backs it up.
        internal static readonly TimeSpan SessionCloseTimeout = TimeSpan.FromSeconds(10);

        protected readonly ClientConfig ClientConfig;
        protected readonly Endpoints Endpoints;
        protected IClientManager ClientManager;
        protected readonly string ClientId;
        protected readonly ClientMeterManager ClientMeterManager;

        protected readonly ConcurrentDictionary<Endpoints, bool> Isolated;
        private readonly ConcurrentDictionary<string, TopicRouteData> _topicRouteCache;

        private readonly Dictionary<Endpoints, Session> _sessionsTable;
        private readonly Dictionary<Endpoints, int> _initializingEndpoints = new Dictionary<Endpoints, int>();
        private readonly ReaderWriterLockSlim _sessionLock;
        private bool _sessionsClosed;

        internal volatile State State;

        protected Client(ClientConfig clientConfig)
        {
            ClientConfig = clientConfig;
            Endpoints = new Endpoints(clientConfig.Endpoints);
            ClientId = Utilities.GetClientId();
            ClientMeterManager = new ClientMeterManager(this);

            ClientManager = new ClientManager(this);
            Isolated = new ConcurrentDictionary<Endpoints, bool>();
            _topicRouteCache = new ConcurrentDictionary<string, TopicRouteData>();

            _topicRouteUpdateCts = new CancellationTokenSource();
            _settingsSyncCts = new CancellationTokenSource();
            _heartbeatCts = new CancellationTokenSource();
            _statsCts = new CancellationTokenSource();

            _sessionsTable = new Dictionary<Endpoints, Session>();
            _sessionLock = new ReaderWriterLockSlim();

            State = State.New;
        }

        protected virtual async Task Start()
        {
            Logger.LogDebug($"Begin to start the rocketmq client, clientId={ClientId}");
            foreach (var topic in GetTopics())
            {
                await FetchTopicRoute(topic);
            }

            ScheduleWithFixedDelay(UpdateTopicRouteCache, TopicRouteUpdateScheduleDelay,
                TopicRouteUpdateSchedulePeriod, _topicRouteUpdateCts.Token);
            ScheduleWithFixedDelay(Heartbeat, HeartbeatScheduleDelay, HeartbeatSchedulePeriod, _heartbeatCts.Token);
            ScheduleWithFixedDelay(SyncSettings, SettingsSyncScheduleDelay, SettingsSyncSchedulePeriod,
                _settingsSyncCts.Token);
            ScheduleWithFixedDelay(Stats, StatsScheduleDelay, StatsSchedulePeriod, _statsCts.Token);
            Logger.LogDebug($"Start the rocketmq client successfully, clientId={ClientId}");
        }

        protected virtual async Task Shutdown()
        {
            Logger.LogDebug($"Begin to shutdown rocketmq client, clientId={ClientId}");
            _heartbeatCts.Cancel();
            _topicRouteUpdateCts.Cancel();
            _settingsSyncCts.Cancel();
            _statsCts.Cancel();
            NotifyClientTermination();

            List<Session> sessions;
            _sessionLock.EnterWriteLock();
            try
            {
                _sessionsClosed = true;
                sessions = _sessionsTable.Values.ToList();
                foreach (var session in sessions)
                {
                    session.MarkClosed();
                }

                _sessionsTable.Clear();
            }
            finally
            {
                _sessionLock.ExitWriteLock();
            }

            // Disposing streams can resume their readers; never do it while holding the session table lock.
            try
            {
                await Task.WhenAll(sessions.Select(session => session.CloseAsync()))
                    .WaitAsync(SessionCloseTimeout);
            }
            catch (Exception e)
            {
                Logger.LogWarning(e, $"Failed to close the client sessions in {SessionCloseTimeout}, " +
                                     $"clientId={ClientId}");
            }

            await ClientManager.Shutdown();
            ClientMeterManager.Shutdown();
            Logger.LogDebug($"Shutdown the rocketmq client successfully, clientId={ClientId}");
        }

        private protected (bool, Session) GetSession(Endpoints endpoints)
        {
            _sessionLock.EnterReadLock();
            try
            {
                ThrowIfSessionsClosed();
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
                ThrowIfSessionsClosed();
                // Session exists, return in advance.
                if (_sessionsTable.TryGetValue(endpoints, out var session))
                {
                    return (false, session);
                }

                var stream = ClientManager.Telemetry(endpoints);
                var created = new Session(endpoints, stream, this);
                _sessionsTable.Add(endpoints, created);
                return (true, created);
            }
            finally
            {
                _sessionLock.ExitWriteLock();
            }
        }

        private bool SessionsClosed => _sessionsClosed || State == State.Stopping
            || State == State.Terminated || State == State.Failed;

        private void ThrowIfSessionsClosed()
        {
            if (SessionsClosed)
            {
                throw new ObjectDisposedException(nameof(Client));
            }
        }

        internal Session GetSessionIfPresent(Endpoints endpoints)
        {
            _sessionLock.EnterReadLock();
            try
            {
                return !SessionsClosed && _sessionsTable.TryGetValue(endpoints, out var session) ? session : null;
            }
            finally
            {
                _sessionLock.ExitReadLock();
            }
        }

        protected abstract IEnumerable<string> GetTopics();

        internal virtual bool IsEndpointsDeprecated(Endpoints endpoints)
        {
            _sessionLock.EnterReadLock();
            try
            {
                return SessionsClosed || (!_initializingEndpoints.ContainsKey(endpoints)
                    && !GetTotalRouteEndpoints().Contains(endpoints));
            }
            finally
            {
                _sessionLock.ExitReadLock();
            }
        }

        internal virtual async Task<bool> RemoveSession(Endpoints endpoints, Session session)
        {
            _sessionLock.EnterWriteLock();
            try
            {
                if (!_sessionsTable.TryGetValue(endpoints, out var existing) || !ReferenceEquals(existing, session)
                    || (!SessionsClosed && (_initializingEndpoints.ContainsKey(endpoints)
                        || GetTotalRouteEndpoints().Contains(endpoints))))
                {
                    return false;
                }

                session.MarkClosed();
                _sessionsTable.Remove(endpoints);
            }
            finally
            {
                _sessionLock.ExitWriteLock();
            }

            await session.CloseAsync();
            return true;
        }

        internal abstract Proto::HeartbeatRequest WrapHeartbeatRequest();

        protected abstract void OnTopicRouteDataUpdated0(string topic, TopicRouteData topicRouteData);

        internal async Task OnTopicRouteDataFetched(string topic, TopicRouteData topicRouteData)
        {
            var routeEndpoints = new HashSet<Endpoints>(topicRouteData.MessageQueues.Select(mq => mq.Broker.Endpoints));
            _sessionLock.EnterWriteLock();
            try
            {
                ThrowIfSessionsClosed();
                foreach (var endpoints in routeEndpoints)
                {
                    _initializingEndpoints.TryGetValue(endpoints, out var count);
                    _initializingEndpoints[endpoints] = count + 1;
                }
            }
            finally
            {
                _sessionLock.ExitWriteLock();
            }

            try
            {
                foreach (var endpoints in routeEndpoints)
                {
                    var (_, session) = GetSession(endpoints);
                    await session.InitializeAsync();
                }

                _sessionLock.EnterWriteLock();
                try
                {
                    ThrowIfSessionsClosed();
                    _topicRouteCache[topic] = topicRouteData;
                    OnTopicRouteDataUpdated0(topic, topicRouteData);
                }
                finally
                {
                    _sessionLock.ExitWriteLock();
                }
            }
            finally
            {
                List<Session> retired;
                _sessionLock.EnterWriteLock();
                try
                {
                    foreach (var endpoints in routeEndpoints)
                    {
                        var count = _initializingEndpoints[endpoints] - 1;
                        if (count == 0)
                        {
                            _initializingEndpoints.Remove(endpoints);
                        }
                        else
                        {
                            _initializingEndpoints[endpoints] = count;
                        }
                    }

                    var active = GetTotalRouteEndpoints();
                    retired = new List<Session>();
                    foreach (var entry in _sessionsTable.ToArray())
                    {
                        if (active.Contains(entry.Key) || _initializingEndpoints.ContainsKey(entry.Key))
                        {
                            continue;
                        }

                        entry.Value.MarkClosed();
                        _sessionsTable.Remove(entry.Key);
                        retired.Add(entry.Value);
                    }
                }
                finally
                {
                    _sessionLock.ExitWriteLock();
                }

                if (ClientManager is ClientManager manager)
                {
                    manager.PruneTransportRecoveryStates();
                }

                await Task.WhenAll(retired.Select(session => session.CloseAsync()));
            }
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
            try
            {
                Logger.LogInformation($"Start to update topic route cache for a new round, clientId={ClientId}");
                Dictionary<string, Task<TopicRouteData>> responses = new Dictionary<string, Task<TopicRouteData>>();

                foreach (var topic in GetTopics())
                {
                    var task = FetchTopicRoute(topic);
                    responses[topic] = task;
                }

                foreach (var item in responses.Keys)
                {
                    try
                    {
                        await responses[item];
                    }
                    catch (Exception e)
                    {
                        Logger.LogError(e, $"Failed to update topic route cache, topic={item}");
                    }
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"[Bug] unexpected exception raised during topic route cache update, " +
                                $"clientId={ClientId}");
            }
        }

        private async void SyncSettings()
        {
            try
            {
                var totalRouteEndpoints = GetTotalRouteEndpoints();
                foreach (var endpoints in totalRouteEndpoints)
                {
                    var (_, session) = GetSession(endpoints);
                    await session.SyncSettings(false);
                    Logger.LogInformation($"Sync settings to remote, endpoints={endpoints}");
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"[Bug] unexpected exception raised during setting sync, clientId={ClientId}");
            }
        }

        private void Stats()
        {
            ThreadPool.GetAvailableThreads(out var availableWorker, out var availableIo);
            Logger.LogInformation(
                $"ClientId={ClientId}, ClientVersion={MetadataConstants.Instance.ClientVersion}, " +
                $".NET Version={Environment.Version}, ThreadCount={ThreadPool.ThreadCount}, " +
                $"CompletedWorkItemCount={ThreadPool.CompletedWorkItemCount}, " +
                $"PendingWorkItemCount={ThreadPool.PendingWorkItemCount}, AvailableWorkerThreads={availableWorker}, " +
                $"AvailableCompletionPortThreads={availableIo}");
        }

        private protected void ScheduleWithFixedDelay(Action action, TimeSpan delay, TimeSpan period, CancellationToken token)
        {
            Task.Run(async () =>
            {
                await Task.Delay(delay, token);
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        action();
                    }
                    catch (Exception e)
                    {
                        Logger.LogError(e, $"Failed to execute scheduled task, ClientId={ClientId}");
                    }
                    finally
                    {
                        await Task.Delay(period, token);
                    }
                }
            }, token);
        }

        protected async Task<TopicRouteData> GetRouteData(string topic)
        {
            if (_topicRouteCache.TryGetValue(topic, out var topicRouteData))
            {
                return topicRouteData;
            }

            topicRouteData = await FetchTopicRoute(topic);
            return topicRouteData;
        }

        private async Task<TopicRouteData> FetchTopicRoute(string topic)
        {
            var topicRouteData = await FetchTopicRoute0(topic);
            await OnTopicRouteDataFetched(topic, topicRouteData);
            Logger.LogInformation(
                $"Fetch topic route successfully, clientId={ClientId}, topic={topic}, topicRouteData={topicRouteData}");
            return topicRouteData;
        }


        private async Task<TopicRouteData> FetchTopicRoute0(string topic)
        {
            try
            {
                var request = new Proto::QueryRouteRequest
                {
                    Topic = new Proto::Resource
                    {
                        ResourceNamespace = ClientConfig.Namespace,
                        Name = topic
                    },
                    Endpoints = Endpoints.ToProtobuf()
                };

                var invocation =
                    await ClientManager.QueryRoute(Endpoints, request, ClientConfig.RequestTimeout);
                var code = invocation.Response.Status.Code;
                if (!Proto.Code.Ok.Equals(code))
                {
                    Logger.LogError($"Failed to fetch topic route, clientId={ClientId}, topic={topic}, code={code}, " +
                                 $"statusMessage={invocation.Response.Status.Message}");
                }

                StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);

                var messageQueues = invocation.Response.MessageQueues.ToList();
                return new TopicRouteData(messageQueues);
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Failed to fetch topic route, clientId={ClientId}, topic={topic}");
                throw;
            }
        }

        private async void Heartbeat()
        {
            try
            {
                var endpoints = GetTotalRouteEndpoints();
                var request = WrapHeartbeatRequest();
                var invocations =
                    new Dictionary<Endpoints, Task<RpcInvocation<Proto.HeartbeatRequest, Proto.HeartbeatResponse>>>();

                // Collect task into a map.
                foreach (var item in endpoints)
                {
                    var task = ClientManager.Heartbeat(item, request, ClientConfig.RequestTimeout);
                    invocations[item] = task;
                }

                foreach (var item in invocations.Keys)
                {
                    try
                    {
                        var invocation = await invocations[item];
                        var code = invocation.Response.Status.Code;

                        if (code.Equals(Proto.Code.Ok))
                        {
                            Logger.LogInformation($"Send heartbeat successfully, endpoints={item}, clientId={ClientId}");
                            if (Isolated.TryRemove(item, out _))
                            {
                                Logger.LogInformation($"Rejoin endpoints which was isolated before, endpoints={item}, " +
                                            $"clientId={ClientId}");
                            }

                            return;
                        }

                        var statusMessage = invocation.Response.Status.Message;
                        Logger.LogInformation($"Failed to send heartbeat, endpoints={item}, code={code}, " +
                                    $"statusMessage={statusMessage}, clientId={ClientId}");
                    }
                    catch (Exception e)
                    {
                        Logger.LogError(e, $"Failed to send heartbeat, endpoints={item}");
                    }
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"[Bug] unexpected exception raised during heartbeat, clientId={ClientId}");
            }
        }

        internal grpcLib.Metadata Sign()
        {
            var metadata = new grpcLib::Metadata();
            Signature.Sign(this, metadata);
            return metadata;
        }

        internal abstract Proto::NotifyClientTerminationRequest WrapNotifyClientTerminationRequest();

        private async void NotifyClientTermination()
        {
            Logger.LogInformation($"Notify remote endpoints that current client is terminated, clientId={ClientId}");
            var endpoints = GetTotalRouteEndpoints();
            var request = WrapNotifyClientTerminationRequest();
            foreach (var item in endpoints)
            {
                try
                {
                    var invocation =
                        await ClientManager.NotifyClientTermination(item, request, ClientConfig.RequestTimeout);
                    StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);
                }
                catch (Exception e)
                {
                    Logger.LogError(e, $"Failed to notify client's termination, clientId=${ClientId}, " +
                                    $"endpoints=${item}");
                }
            }
        }

        internal abstract Settings GetSettings();

        internal string GetClientId()
        {
            return ClientId;
        }

        internal ClientConfig GetClientConfig()
        {
            return ClientConfig;
        }

        internal IClientManager GetClientManager()
        {
            return ClientManager;
        }

        /// <summary>
        /// Rebuilds the telemetry stream of the specified endpoints, invoked right after the gRPC transport has been
        /// replaced.
        /// </summary>
        internal virtual void ReconnectTelemetry(Endpoints endpoints, Session expectedSession = null)
        {
            var session = GetSessionIfPresent(endpoints);
            if (null != expectedSession && !ReferenceEquals(session, expectedSession))
            {
                return;
            }

            if (null == session)
            {
                Logger.LogWarning($"Failed to rebuild telemetry because client session does not exist, " +
                                  $"endpoints={endpoints}, clientId={ClientId}");
                return;
            }

            session.Reconnect();
        }

        // Only for testing
        internal void SetClientManager(IClientManager clientManager)
        {
            ClientManager = clientManager;
        }

        internal virtual void OnRecoverOrphanedTransactionCommand(Endpoints endpoints,
            Proto.RecoverOrphanedTransactionCommand command)
        {
            Logger.LogWarning($"Ignore orphaned transaction recovery command from remote, which is not expected, " +
                              $"clientId={ClientId}, endpoints={endpoints}");
        }

        internal virtual async Task OnVerifyMessageCommand(Endpoints endpoints, Proto.VerifyMessageCommand command)
        {
            try
            {
                var session = GetSessionIfPresent(endpoints);
                if (null == session)
                {
                    return;
                }

                var telemetryCommand = new Proto.TelemetryCommand
                {
                    VerifyMessageResult = new Proto.VerifyMessageResult { Nonce = command.Nonce },
                    Status = new Proto.Status
                    {
                        Code = Proto.Code.Unsupported,
                        Message = "Message consumption verification is not supported"
                    }
                };
                await session.WriteAsync(telemetryCommand);
            }
            catch (Exception e)
            {
                Logger.LogWarning(e, $"Failed to reply to message verification, endpoints={endpoints}, clientId={ClientId}");
            }
        }

        internal async Task OnPrintThreadStackTraceCommand(Endpoints endpoints,
            Proto.PrintThreadStackTraceCommand command)
        {
            try
            {
                var session = GetSessionIfPresent(endpoints);
                if (null == session)
                {
                    return;
                }

                var telemetryCommand = new Proto.TelemetryCommand
                {
                    ThreadStackTrace = new Proto.ThreadStackTrace { Nonce = command.Nonce },
                    Status = new Proto.Status
                    {
                        Code = Proto.Code.Unsupported,
                        Message = "C# don't support thread stack trace printing"
                    }
                };
                await session.WriteAsync(telemetryCommand);
            }
            catch (Exception e)
            {
                Logger.LogWarning(e, $"Failed to reply to thread stack trace request, endpoints={endpoints}, clientId={ClientId}");
            }
        }

        internal virtual void OnReconnectEndpointsCommand(Endpoints endpoints, Proto.ReconnectEndpointsCommand command)
        {
            Logger.LogInformation($"Receive reconnect endpoints command from remote, endpoints={endpoints}, " +
                                  $"command={command}, clientId={ClientId}");
            ClientManager.Reconnect(endpoints);
        }

        internal virtual void OnSettingsCommand(Endpoints endpoints, Proto.Settings settings)
        {
            var metric = new Metric(settings.Metric ?? new Proto.Metric());
            ClientMeterManager.Reset(metric);
            GetSettings().Sync(settings);
        }

        internal virtual void OnNotifyUnsubscribeLiteCommand(Endpoints endpoints, Proto.NotifyUnsubscribeLiteCommand command)
        {
            // Default implementation does nothing
            // LitePushConsumer will override this to handle lite topic unsubscription
        }
    }
}