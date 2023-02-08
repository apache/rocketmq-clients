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
using System.Threading.Channels;
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

        private readonly grpc::AsyncDuplexStreamingCall<Proto::TelemetryCommand, Proto::TelemetryCommand>
            _streamingCall;

        private readonly Client _client;
        private readonly Channel<bool> _channel;
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
            _channel = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest
            });
            Loop();
        }


        public async Task SyncSettings(bool awaitResp)
        {
            await _semaphore.WaitAsync();
            try
            {
                var writer = _streamingCall.RequestStream;
                // await readTask;
                var settings = _client.GetSettings();
                Proto.TelemetryCommand telemetryCommand = new Proto.TelemetryCommand
                {
                    Settings = settings
                };
                await writer.WriteAsync(telemetryCommand);
                // await writer.CompleteAsync();
                if (awaitResp)
                {
                    await _channel.Reader.ReadAsync();
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        // public async void xx()
        // {
        //     while (true)
        //     {
        //         var reader = _streamingCall.ResponseStream;
        //         if (await reader.MoveNext(_client.TelemetryCts().Token))
        //         {
        //             var command = reader.Current;
        //             Console.WriteLine("xxxxxxxx");
        //             Console.WriteLine(command);
        //         }
        //     }
        // }


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
                            await _channel.Writer.WriteAsync(true);
                            _client.OnSettingsCommand(_endpoints, response.Settings);
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