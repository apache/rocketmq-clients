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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using grpcLib = Grpc.Core;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Org.Apache.Rocketmq.Error;

namespace Org.Apache.Rocketmq
{
    public class ClientManager : IClientManager
    {
        internal const int HeartbeatFailureThreshold = 2;
        internal static readonly TimeSpan TransportRecoveryCooldown = TimeSpan.FromSeconds(30);

        private static readonly ILogger Logger = MqLogManager.CreateLogger<ClientManager>();

        private readonly Client _client;
        private readonly Dictionary<Endpoints, IRpcClient> _rpcClients;
        private readonly ReaderWriterLockSlim _clientLock;
        private readonly object _transportRecoveryLock = new object();
        private readonly Dictionary<Endpoints, TransportRecoveryState> _transportRecoveryStates =
            new Dictionary<Endpoints, TransportRecoveryState>();
        // A retired state must not release a reset which is still using the endpoint's cached RPC client.
        private readonly HashSet<Endpoints> _recoveringEndpoints = new HashSet<Endpoints>();
        private bool _stopped;

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

        private IRpcClient GetRpcClientIfPresent(Endpoints endpoints)
        {
            _clientLock.EnterReadLock();
            try
            {
                return _rpcClients.TryGetValue(endpoints, out var cachedClient) ? cachedClient : null;
            }
            finally
            {
                _clientLock.ExitReadLock();
            }
        }

        public void Reconnect(Endpoints endpoints)
        {
            var rpcClient = GetRpcClientIfPresent(endpoints);
            if (null == rpcClient)
            {
                Logger.LogWarning($"Failed to reconnect because rpc client does not exist, endpoints={endpoints}, " +
                                  $"clientId={_client.GetClientId()}");
                return;
            }

            Reconnect(endpoints, rpcClient);
        }

        internal void Reconnect(Endpoints endpoints, IRpcClient rpcClient)
        {
            var recoveryState = GetTransportRecoveryState(endpoints);
            lock (_transportRecoveryLock)
            {
                if (!TryAdmitTransportRecovery(endpoints, recoveryState, false))
                {
                    return;
                }
            }

            RecoverTransport(endpoints, rpcClient, recoveryState, "server reconnect command");
        }

        private TransportRecoveryState GetTransportRecoveryState(Endpoints endpoints)
        {
            lock (_transportRecoveryLock)
            {
                if (_stopped || _client.IsEndpointsDeprecated(endpoints))
                {
                    RetireTransportRecoveryState(endpoints);
                    return null;
                }

                var session = _client.GetSessionIfPresent(endpoints);
                if (!_transportRecoveryStates.TryGetValue(endpoints, out var recoveryState)
                    || !ReferenceEquals(recoveryState.Session, session))
                {
                    RetireTransportRecoveryState(endpoints);
                    recoveryState = new TransportRecoveryState(session);
                    _transportRecoveryStates.Add(endpoints, recoveryState);
                }

                return recoveryState;
            }
        }

        // Called outside Client's session lock: the lock order is recovery -> session, never the reverse.
        internal void PruneTransportRecoveryStates()
        {
            lock (_transportRecoveryLock)
            {
                foreach (var endpoints in _transportRecoveryStates.Keys.ToArray())
                {
                    if (_client.IsEndpointsDeprecated(endpoints)
                        || !ReferenceEquals(_transportRecoveryStates[endpoints].Session, _client.GetSessionIfPresent(endpoints)))
                    {
                        RetireTransportRecoveryState(endpoints);
                    }
                }
            }
        }

        // The following state helpers are only used while holding _transportRecoveryLock.
        private void RetireTransportRecoveryState(Endpoints endpoints)
        {
            if (_transportRecoveryStates.Remove(endpoints, out var recoveryState))
            {
                recoveryState.Heartbeats.Clear();
            }
        }

        private bool IsTransportRecoveryStateActive(Endpoints endpoints, TransportRecoveryState recoveryState)
        {
            if (_stopped || null == recoveryState
                || !_transportRecoveryStates.TryGetValue(endpoints, out var activeState)
                || !ReferenceEquals(activeState, recoveryState))
            {
                return false;
            }

            if (_client.IsEndpointsDeprecated(endpoints)
                || !ReferenceEquals(recoveryState.Session, _client.GetSessionIfPresent(endpoints)))
            {
                RetireTransportRecoveryState(endpoints);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Watches the outcome of a heartbeat and drives transport recovery from connectivity failures only. Ordinary
        /// RPC failures never reset the transport, the native reconnection of gRPC stays in charge of those.
        /// </summary>
        internal Task MonitorHeartbeat(Endpoints endpoints, IRpcClient rpcClient,
            Task<Proto.HeartbeatResponse> future)
        {
            return MonitorHeartbeat(endpoints, rpcClient, GetTransportRecoveryState(endpoints), future);
        }

        private Task MonitorHeartbeat(Endpoints endpoints, IRpcClient rpcClient, TransportRecoveryState recoveryState,
            Task<Proto.HeartbeatResponse> future)
        {
            lock (_transportRecoveryLock)
            {
                if (IsTransportRecoveryStateActive(endpoints, recoveryState))
                {
                    // Register before attaching the callback, including for an already completed heartbeat.
                    recoveryState.Heartbeats.Enqueue(future);
                }
            }

            return future.ContinueWith(task =>
            {
                // Even a retired heartbeat must have its exception observed.
                _ = task.Exception;
                string recoveryReason = null;
                try
                {
                    lock (_transportRecoveryLock)
                    {
                        if (!IsTransportRecoveryStateActive(endpoints, recoveryState))
                        {
                            return Task.CompletedTask;
                        }

                        // Completion callbacks may run out of order. Only completed queue heads affect the counter.
                        while (recoveryState.Heartbeats.Count > 0 && recoveryState.Heartbeats.Peek().IsCompleted)
                        {
                            var completed = recoveryState.Heartbeats.Dequeue();
                            var statusCode =
                                (completed.Exception?.Flatten().InnerException as grpcLib.RpcException)?.StatusCode;
                            var recover = false;
                            switch (statusCode)
                            {
                                case grpcLib.StatusCode.Unavailable:
                                    recoveryState.HeartbeatFailureAttempts = 0;
                                    // Other states mean gRPC is already reconnecting on its own.
                                    recover = grpcLib.ConnectivityState.Ready == rpcClient.State;
                                    break;
                                case grpcLib.StatusCode.DeadlineExceeded:
                                    recoveryState.HeartbeatFailureAttempts++;
                                    recover = recoveryState.HeartbeatFailureAttempts >= HeartbeatFailureThreshold;
                                    break;
                                default:
                                    recoveryState.HeartbeatFailureAttempts = 0;
                                    break;
                            }

                            if (recover && TryAdmitTransportRecovery(endpoints, recoveryState, true))
                            {
                                recoveryReason = $"heartbeat failure, statusCode={statusCode}";
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Logger.LogError(e, $"[Bug] unexpected exception raised while monitoring heartbeat, " +
                                       $"endpoints={endpoints}, clientId={_client.GetClientId()}");
                }

                // Only admitted recovery goes to the pool. The heartbeat caller never waits for transport or streams.
                return null == recoveryReason ? Task.CompletedTask : Task.Run(() =>
                    RecoverTransport(endpoints, rpcClient, recoveryState, recoveryReason));
            }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default).Unwrap();
        }

        private bool TryAdmitTransportRecovery(Endpoints endpoints, TransportRecoveryState recoveryState,
            bool respectHeartbeatCooldown)
        {
            if (!IsTransportRecoveryStateActive(endpoints, recoveryState) || State.Running != _client.State
                || _recoveringEndpoints.Contains(endpoints))
            {
                return false;
            }

            var now = Stopwatch.GetTimestamp();
            var previous = recoveryState.LastRecoveryTimestamp;
            if (respectHeartbeatCooldown && TransportRecoveryState.NoRecoveryTimestamp != previous
                && Stopwatch.GetElapsedTime(previous, now) < TransportRecoveryCooldown)
            {
                // Keep failures accumulated during cooldown so the next failure after expiry can recover.
                return false;
            }

            _recoveringEndpoints.Add(endpoints);
            recoveryState.LastRecoveryTimestamp = now;
            recoveryState.HeartbeatFailureAttempts = 0;
            return true;
        }

        private void RecoverTransport(Endpoints endpoints, IRpcClient rpcClient, TransportRecoveryState recoveryState,
            string reason)
        {
            try
            {
                lock (_transportRecoveryLock)
                {
                    if (!IsTransportRecoveryStateActive(endpoints, recoveryState) || State.Running != _client.State)
                    {
                        return;
                    }
                }

                Logger.LogWarning($"Try to recover transport, endpoints={endpoints}, reason={reason}, " +
                                  $"clientId={_client.GetClientId()}");
                try
                {
                    rpcClient.ResetTransport();
                }
                catch (Exception e)
                {
                    Logger.LogWarning(e, $"Failed to reset transport while recovering, endpoints={endpoints}, " +
                                         $"reason={reason}, clientId={_client.GetClientId()}");
                }

                lock (_transportRecoveryLock)
                {
                    if (!IsTransportRecoveryStateActive(endpoints, recoveryState) || State.Running != _client.State)
                    {
                        return;
                    }
                }

                try
                {
                    // Never call Client's session-writing operations while holding the recovery lock.
                    _client.ReconnectTelemetry(endpoints, recoveryState.Session);
                }
                catch (Exception e)
                {
                    Logger.LogWarning(e, $"Failed to rebuild telemetry while recovering transport, " +
                                         $"endpoints={endpoints}, reason={reason}, clientId={_client.GetClientId()}");
                }
            }
            finally
            {
                lock (_transportRecoveryLock)
                {
                    _recoveringEndpoints.Remove(endpoints);
                }
            }
        }

        /// <summary>
        /// Maps a transport level RESOURCE_EXHAUSTED failure onto the throttling exception of the SDK so that the
        /// retry and the backoff logic of the callers can recognize it. Every other outcome is passed through.
        /// </summary>
        internal static async Task<TResponse> NormalizeTransportException<TResponse>(grpcLib.Metadata metadata,
            Task<TResponse> task)
        {
            try
            {
                return await task.ConfigureAwait(false);
            }
            catch (grpcLib.RpcException e) when (grpcLib.StatusCode.ResourceExhausted == e.StatusCode)
            {
                var requestId = metadata?.GetValue(MetadataConstants.RequestIdKey);
                var description = string.IsNullOrEmpty(e.Status.Detail) ? e.Message : e.Status.Detail;
                throw new TooManyRequestsException((int)Proto.Code.TooManyRequests, requestId, description, e);
            }
        }

        public async Task Shutdown()
        {
            lock (_transportRecoveryLock)
            {
                _stopped = true;
                foreach (var recoveryState in _transportRecoveryStates.Values)
                {
                    recoveryState.Heartbeats.Clear();
                }

                _transportRecoveryStates.Clear();
            }

            List<IRpcClient> rpcClients;
            _clientLock.EnterReadLock();
            try
            {
                rpcClients = _rpcClients.Values.ToList();
            }
            finally
            {
                _clientLock.ExitReadLock();
            }

            await Task.WhenAll(rpcClients.Select(rpcClient => rpcClient.Shutdown())).ConfigureAwait(false);
        }

        public grpcLib::AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand> Telemetry(
            Endpoints endpoints)
        {
            return GetRpcClient(endpoints).Telemetry(_client.Sign());
        }

        public async Task<RpcInvocation<Proto.QueryRouteRequest, Proto.QueryRouteResponse>> QueryRoute(
            Endpoints endpoints, Proto.QueryRouteRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await NormalizeTransportException(metadata,
                GetRpcClient(endpoints).QueryRoute(metadata, request, timeout));
            return new RpcInvocation<Proto.QueryRouteRequest, Proto.QueryRouteResponse>(request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.HeartbeatRequest, Proto.HeartbeatResponse>> Heartbeat(Endpoints endpoints,
            Proto.HeartbeatRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var rpcClient = GetRpcClient(endpoints);
            var recoveryState = GetTransportRecoveryState(endpoints);
            var future = rpcClient.Heartbeat(metadata, request, timeout);
            _ = MonitorHeartbeat(endpoints, rpcClient, recoveryState, future);
            var response = await NormalizeTransportException(metadata, future);
            return new RpcInvocation<Proto.HeartbeatRequest, Proto.HeartbeatResponse>(request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.NotifyClientTerminationRequest, Proto.NotifyClientTerminationResponse>>
            NotifyClientTermination(Endpoints endpoints, Proto.NotifyClientTerminationRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await NormalizeTransportException(metadata,
                GetRpcClient(endpoints).NotifyClientTermination(metadata, request, timeout));
            return new RpcInvocation<Proto.NotifyClientTerminationRequest, Proto.NotifyClientTerminationResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.RecallMessageRequest, Proto.RecallMessageResponse>>
            RecallMessage(Endpoints endpoints, Proto.RecallMessageRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await NormalizeTransportException(metadata,
                GetRpcClient(endpoints).RecallMessage(metadata, request, timeout));
            return new RpcInvocation<Proto.RecallMessageRequest, Proto.RecallMessageResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.SendMessageRequest, Proto.SendMessageResponse>> SendMessage(
            Endpoints endpoints, Proto::SendMessageRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await NormalizeTransportException(metadata,
                GetRpcClient(endpoints).SendMessage(metadata, request, timeout));
            return new RpcInvocation<Proto.SendMessageRequest, Proto.SendMessageResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.QueryAssignmentRequest, Proto.QueryAssignmentResponse>> QueryAssignment(
            Endpoints endpoints, Proto.QueryAssignmentRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await NormalizeTransportException(metadata,
                GetRpcClient(endpoints).QueryAssignment(metadata, request, timeout));
            return new RpcInvocation<Proto.QueryAssignmentRequest, Proto.QueryAssignmentResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.ReceiveMessageRequest, List<Proto.ReceiveMessageResponse>>>
            ReceiveMessage(Endpoints endpoints, Proto.ReceiveMessageRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await NormalizeTransportException(metadata,
                GetRpcClient(endpoints).ReceiveMessage(metadata, request, timeout));

            return new RpcInvocation<Proto.ReceiveMessageRequest, List<Proto.ReceiveMessageResponse>>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>> AckMessage(
            Endpoints endpoints, Proto.AckMessageRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await NormalizeTransportException(metadata,
                GetRpcClient(endpoints).AckMessage(metadata, request, timeout));
            return new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>>
            ChangeInvisibleDuration(Endpoints endpoints,
                Proto.ChangeInvisibleDurationRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await NormalizeTransportException(metadata,
                GetRpcClient(endpoints).ChangeInvisibleDuration(metadata, request, timeout));
            return new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.ForwardMessageToDeadLetterQueueRequest, Proto.ForwardMessageToDeadLetterQueueResponse>>
            ForwardMessageToDeadLetterQueue(Endpoints endpoints,
                Proto.ForwardMessageToDeadLetterQueueRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await NormalizeTransportException(metadata,
                GetRpcClient(endpoints).ForwardMessageToDeadLetterQueue(metadata, request, timeout));
            return new RpcInvocation<Proto.ForwardMessageToDeadLetterQueueRequest, Proto.ForwardMessageToDeadLetterQueueResponse>(
                    request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.EndTransactionRequest, Proto.EndTransactionResponse>> EndTransaction(
            Endpoints endpoints, Proto.EndTransactionRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await NormalizeTransportException(metadata,
                GetRpcClient(endpoints).EndTransaction(metadata, request, timeout));
            return new RpcInvocation<Proto.EndTransactionRequest, Proto.EndTransactionResponse>(
                request, response, metadata);
        }

        public async Task<RpcInvocation<Proto.SyncLiteSubscriptionRequest, Proto.SyncLiteSubscriptionResponse>> SyncLiteSubscription(
            Endpoints endpoints, Proto.SyncLiteSubscriptionRequest request, TimeSpan timeout)
        {
            var metadata = _client.Sign();
            var response = await NormalizeTransportException(metadata,
                GetRpcClient(endpoints).SyncLiteSubscription(metadata, request, timeout));
            return new RpcInvocation<Proto.SyncLiteSubscriptionRequest, Proto.SyncLiteSubscriptionResponse>(
                request, response, metadata);
        }

        private sealed class TransportRecoveryState
        {
            internal const long NoRecoveryTimestamp = long.MinValue;

            internal readonly Session Session;
            internal readonly Queue<Task<Proto.HeartbeatResponse>> Heartbeats = new Queue<Task<Proto.HeartbeatResponse>>();
            internal int HeartbeatFailureAttempts;
            internal long LastRecoveryTimestamp = NoRecoveryTimestamp;

            internal TransportRecoveryState(Session session)
            {
                Session = session;
            }
        }
    }
}
