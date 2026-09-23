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

using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    // refer to  https://learn.microsoft.com/en-us/aspnet/core/grpc/client?view=aspnetcore-7.0#bi-directional-streaming-call.
    public class Session
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<Session>();

        private static readonly TimeSpan SettingsInitializationTimeout = TimeSpan.FromSeconds(3);
        private static readonly TimeSpan StreamingCallRenewBackoffDelay = TimeSpan.FromSeconds(1);

        // A healthy writer holds the stream lock for the duration of a single small command only, so exceeding this
        // means the write is stuck on an unusable connection and will never complete on its own.
        internal static readonly TimeSpan CloseTimeout = TimeSpan.FromSeconds(3);

        private readonly ManualResetEventSlim _event = new ManualResetEventSlim(false);

        private readonly Client _client;
        private readonly Endpoints _endpoints;
        private readonly SemaphoreSlim _semaphore;
        private readonly Lazy<Task> _initialization;

        // Serializes the writers against the replacement of the streaming call. It is never held while acquiring
        // _semaphore, and _semaphore is held across a blocking wait, so the two must never be nested the other way.
        private readonly SemaphoreSlim _streamLock = new SemaphoreSlim(1);

        private AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand> _streamingCall;
        private int _closed;
        private int _reconnecting;

        public Session(Endpoints endpoints,
            AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand> streamingCall,
            Client client)
        {
            _endpoints = endpoints;
            _semaphore = new SemaphoreSlim(1);
            _streamingCall = streamingCall;
            _client = client;
            _initialization = new Lazy<Task>(() => SyncSettings(true));
            StartReadLoop(streamingCall);
        }

        internal Task InitializeAsync()
        {
            return _initialization.Value;
        }

        public Task WriteAsync(Proto.TelemetryCommand telemetryCommand)
        {
            return WriteAsync(telemetryCommand, null);
        }

        private async Task WriteAsync(Proto.TelemetryCommand telemetryCommand,
            AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand> expectedCall)
        {
            await _streamLock.WaitAsync();
            try
            {
                var streamingCall = Volatile.Read(ref _streamingCall);
                if (1 == Volatile.Read(ref _closed) || null == streamingCall
                    || (null != expectedCall && !ReferenceEquals(streamingCall, expectedCall)))
                {
                    return;
                }

                await streamingCall.RequestStream.WriteAsync(telemetryCommand);
            }
            finally
            {
                _streamLock.Release();
            }
        }

        public Task SyncSettings(bool awaitResp)
        {
            return SyncSettings(awaitResp, null);
        }

        private async Task SyncSettings(bool awaitResp,
            AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand> expectedCall)
        {
            await _semaphore.WaitAsync();
            try
            {
                if (1 == Volatile.Read(ref _closed)
                    || (null != expectedCall && !ReferenceEquals(Volatile.Read(ref _streamingCall), expectedCall)))
                {
                    return;
                }

                var telemetryCommand = new Proto.TelemetryCommand
                {
                    Settings = _client.GetSettings().ToProtobuf()
                };
                await WriteAsync(telemetryCommand, expectedCall);
                if (awaitResp)
                {
                    _event.Wait(_client.GetClientConfig().RequestTimeout.Add(SettingsInitializationTimeout));
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        internal void Reconnect()
        {
            TryReconnect(null);
        }

        private bool TryReconnect(
            AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand> expectedCall)
        {
            if (0 != Interlocked.CompareExchange(ref _reconnecting, 1, 0))
            {
                return false;
            }

            _ = ReconnectAsync(expectedCall);
            return true;
        }

        private async Task ReconnectAsync(
            AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand> expectedCall)
        {
            AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand> renewed = null;
            try
            {
                if (null != expectedCall && !ReferenceEquals(Volatile.Read(ref _streamingCall), expectedCall))
                {
                    return;
                }

                while (null == renewed)
                {
                    if (1 == Volatile.Read(ref _closed)
                        || (_client.State != State.Running && _client.State != State.Starting))
                    {
                        return;
                    }

                    if (_client.IsEndpointsDeprecated(_endpoints)
                        && await _client.RemoveSession(_endpoints, this))
                    {
                        return;
                    }

                    var previous = Volatile.Read(ref _streamingCall);
                    // A terminated reader can leave a write blocked; disposing the call releases its write lock.
                    CancelStreamingCall(previous);
                    try
                    {
                        var streamingCall = _client.GetClientManager().Telemetry(_endpoints);
                        await _streamLock.WaitAsync();
                        try
                        {
                            if (1 == Volatile.Read(ref _closed))
                            {
                                CancelStreamingCall(streamingCall);
                                return;
                            }

                            Interlocked.Exchange(ref _streamingCall, streamingCall);
                            if (1 == Volatile.Read(ref _closed))
                            {
                                Interlocked.CompareExchange(ref _streamingCall, null, streamingCall);
                                CancelStreamingCall(streamingCall);
                                return;
                            }

                            renewed = streamingCall;
                        }
                        finally
                        {
                            _streamLock.Release();
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogWarning(e, $"Failed to renew telemetry, endpoints={_endpoints}, " +
                                             $"delay={StreamingCallRenewBackoffDelay}, clientId={_client.GetClientId()}");
                        await Task.Delay(StreamingCallRenewBackoffDelay);
                    }
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Failed to reconnect telemetry, endpoints={_endpoints}, " +
                                   $"clientId={_client.GetClientId()}");
            }
            finally
            {
                Interlocked.Exchange(ref _reconnecting, 0);
            }

            if (null == renewed)
            {
                return;
            }

            // Release the renewal guard before the new reader can signal its own termination.
            StartReadLoop(renewed);
            try
            {
                await SyncSettings(false, renewed);
            }
            catch (Exception e)
            {
                Logger.LogWarning(e, $"Failed to sync settings on renewed telemetry, endpoints={_endpoints}, " +
                                     $"clientId={_client.GetClientId()}");
            }
        }

        internal void MarkClosed()
        {
            Interlocked.Exchange(ref _closed, 1);
            _event.Set();
        }

        internal async Task CloseAsync()
        {
            MarkClosed();

            if (await _streamLock.WaitAsync(CloseTimeout))
            {
                AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand> streamingCall;
                try
                {
                    streamingCall = _streamingCall;
                    _streamingCall = null;
                }
                finally
                {
                    _streamLock.Release();
                }

                CancelStreamingCall(streamingCall);
                return;
            }

            Logger.LogWarning($"A writer holds the streaming call, force closing it, endpoints={_endpoints}, " +
                              $"timeout={CloseTimeout}, clientId={_client.GetClientId()}");
            // Disposing the call is what releases the stuck writer, its await faults and drops the lock afterwards.
            CancelStreamingCall(Interlocked.Exchange(ref _streamingCall, null));
        }

        private void CancelStreamingCall(
            AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand> streamingCall)
        {
            if (null == streamingCall)
            {
                return;
            }

            try
            {
                streamingCall.Dispose();
            }
            catch (Exception e)
            {
                Logger.LogWarning(e, $"Failed to cancel the replaced streaming call, endpoints={_endpoints}, " +
                                     $"clientId={_client.GetClientId()}");
            }
        }

        private void StartReadLoop(
            AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand> streamingCall)
        {
            Task.Run(async () =>
            {
                try
                {
                    await foreach (var response in streamingCall.ResponseStream.ReadAllAsync())
                    {
                        if (1 == Volatile.Read(ref _closed)
                            || !ReferenceEquals(Volatile.Read(ref _streamingCall), streamingCall))
                        {
                            return;
                        }

                        switch (response.CommandCase)
                        {
                            case Proto.TelemetryCommand.CommandOneofCase.Settings:
                                {
                                    Logger.LogInformation(
                                        $"Receive setting from remote, endpoints={_endpoints}, clientId={_client.GetClientId()}");
                                    _client.OnSettingsCommand(_endpoints, response.Settings);
                                    _event.Set();
                                    break;
                                }
                            case Proto.TelemetryCommand.CommandOneofCase.RecoverOrphanedTransactionCommand:
                                {
                                    Logger.LogInformation(
                                        $"Receive orphaned transaction recovery command from remote, endpoints={_endpoints}, clientId={_client.GetClientId()}");
                                    _client.OnRecoverOrphanedTransactionCommand(_endpoints,
                                        response.RecoverOrphanedTransactionCommand);
                                    break;
                                }
                            case Proto.TelemetryCommand.CommandOneofCase.VerifyMessageCommand:
                                {
                                    Logger.LogInformation(
                                        $"Receive message verification command from remote, endpoints={_endpoints}, clientId={_client.GetClientId()}");
                                    _ = _client.OnVerifyMessageCommand(_endpoints, response.VerifyMessageCommand);
                                    break;
                                }
                            case Proto.TelemetryCommand.CommandOneofCase.PrintThreadStackTraceCommand:
                                {
                                    Logger.LogInformation(
                                        $"Receive thread stack print command from remote, endpoints={_endpoints}, clientId={_client.GetClientId()}");
                                    _ = _client.OnPrintThreadStackTraceCommand(_endpoints, response.PrintThreadStackTraceCommand);
                                    break;
                                }
                            case Proto.TelemetryCommand.CommandOneofCase.ReconnectEndpointsCommand:
                                {
                                    Logger.LogInformation(
                                        $"Receive reconnect endpoints command from remote, endpoints={_endpoints}, clientId={_client.GetClientId()}");
                                    _client.OnReconnectEndpointsCommand(_endpoints, response.ReconnectEndpointsCommand);
                                    break;
                                }
                            case Proto.TelemetryCommand.CommandOneofCase.NotifyUnsubscribeLiteCommand:
                                {
                                    Logger.LogInformation(
                                        $"Receive notify unsubscribe lite command from remote, endpoints={_endpoints}, clientId={_client.GetClientId()}");
                                    _client.OnNotifyUnsubscribeLiteCommand(_endpoints, response.NotifyUnsubscribeLiteCommand);
                                    break;
                                }
                            default:
                                {
                                    Logger.LogWarning(
                                        $"Receive unrecognized command from remote, endpoints={_endpoints}, command={response}, clientId={_client.GetClientId()}");
                                    break;
                                }
                        }
                    }
                }
                catch (Exception e)
                {
                    // The loop of a replaced or closed session is cancelled on purpose.
                    if (ReferenceEquals(Volatile.Read(ref _streamingCall), streamingCall) && 0 == Volatile.Read(ref _closed))
                    {
                        Logger.LogError(e, $"Exception raised from the telemetry stream, endpoints={_endpoints}, " +
                                           $"clientId={_client.GetClientId()}");
                    }
                    else
                    {
                        Logger.LogInformation($"Telemetry stream is cancelled for reconnecting, endpoints={_endpoints}, " +
                                              $"clientId={_client.GetClientId()}");
                    }
                }

                try
                {
                    await RenewAfterTerminationAsync(streamingCall);
                }
                catch (Exception e)
                {
                    Logger.LogError(e, $"[Bug] unexpected exception raised while handling the termination of the " +
                                       $"telemetry stream, endpoints={_endpoints}, clientId={_client.GetClientId()}");
                }
            });
        }

        private bool IsRenewalObsolete(
            AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand> streamingCall)
        {
            // The loop of a replaced or closed session ends on purpose, and a client which is not running must not be
            // kept alive by a renewal of its telemetry stream.
            return !ReferenceEquals(Volatile.Read(ref _streamingCall), streamingCall)
                   || 1 == Volatile.Read(ref _closed)
                   || (_client.State != State.Running && _client.State != State.Starting);
        }

        private async Task RenewAfterTerminationAsync(
            AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand> streamingCall)
        {
            if (IsRenewalObsolete(streamingCall))
            {
                return;
            }

            Logger.LogWarning($"Telemetry stream terminated, attempt to renew it later, endpoints={_endpoints}, " +
                              $"delay={StreamingCallRenewBackoffDelay}, clientId={_client.GetClientId()}");
            // A stale callback may briefly own the guard without renewing the current stream.
            do
            {
                await Task.Delay(StreamingCallRenewBackoffDelay);
                if (IsRenewalObsolete(streamingCall) || TryReconnect(streamingCall))
                {
                    return;
                }
            }
            while (!IsRenewalObsolete(streamingCall));
        }
    }
}
