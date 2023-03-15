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
using grpc = Grpc.Core;
using NLog;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    // refer to  https://learn.microsoft.com/en-us/aspnet/core/grpc/client?view=aspnetcore-7.0#bi-directional-streaming-call.
    public class Session
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        private static readonly TimeSpan SettingsInitializationTimeout = TimeSpan.FromSeconds(3);
        private readonly ManualResetEventSlim _event = new ManualResetEventSlim(false);

        private readonly AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand>
            _streamingCall;

        private readonly Client _client;
        private readonly Endpoints _endpoints;
        private readonly SemaphoreSlim _semaphore;

        public Session(Endpoints endpoints,
            AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand> streamingCall,
            Client client)
        {
            _endpoints = endpoints;
            _semaphore = new SemaphoreSlim(1);
            _streamingCall = streamingCall;
            _client = client;
            Loop();
        }

        public async Task WriteAsync(Proto.TelemetryCommand telemetryCommand)
        {
            var writer = _streamingCall.RequestStream;
            await writer.WriteAsync(telemetryCommand);
        }

        // TODO: Test concurrency.
        public async Task SyncSettings(bool awaitResp)
        {
            // Add more buffer time.
            await _semaphore.WaitAsync();
            try
            {
                var settings = _client.GetSettings();
                var telemetryCommand = new Proto.TelemetryCommand
                {
                    Settings = settings.ToProtobuf()
                };
                await WriteAsync(telemetryCommand);
                // await writer.CompleteAsync();
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

        private void Loop()
        {
            Task.Run(async () =>
            {
                await foreach (var response in _streamingCall.ResponseStream.ReadAllAsync())
                {
                    switch (response.CommandCase)
                    {
                        case Proto.TelemetryCommand.CommandOneofCase.Settings:
                            {
                                Logger.Info(
                                    $"Receive setting from remote, endpoints={_endpoints}, clientId={_client.GetClientId()}");
                                _client.OnSettingsCommand(_endpoints, response.Settings);
                                _event.Set();
                                break;
                            }
                        case Proto.TelemetryCommand.CommandOneofCase.RecoverOrphanedTransactionCommand:
                            {
                                Logger.Info(
                                    $"Receive orphaned transaction recovery command from remote, endpoints={_endpoints}, clientId={_client.GetClientId()}");
                                _client.OnRecoverOrphanedTransactionCommand(_endpoints,
                                    response.RecoverOrphanedTransactionCommand);
                                break;
                            }
                        case Proto.TelemetryCommand.CommandOneofCase.VerifyMessageCommand:
                            {
                                Logger.Info(
                                    $"Receive message verification command from remote, endpoints={_endpoints}, clientId={_client.GetClientId()}");
                                _client.OnVerifyMessageCommand(_endpoints, response.VerifyMessageCommand);
                                break;
                            }
                        case Proto.TelemetryCommand.CommandOneofCase.PrintThreadStackTraceCommand:
                            {
                                Logger.Info(
                                    $"Receive thread stack print command from remote, endpoints={_endpoints}, clientId={_client.GetClientId()}");
                                _client.OnPrintThreadStackTraceCommand(_endpoints, response.PrintThreadStackTraceCommand);
                                break;
                            }
                        default:
                            {
                                Logger.Warn(
                                    $"Receive unrecognized command from remote, endpoints={_endpoints}, command={response}, clientId={_client.GetClientId()}");
                                break;
                            }
                    }
                }
            });
        }
    };
}