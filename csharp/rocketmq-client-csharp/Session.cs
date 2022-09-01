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

using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using grpc = Grpc.Core;
using NLog;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class Session
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        public Session(string target, 
            grpc::AsyncDuplexStreamingCall<rmq::TelemetryCommand, rmq::TelemetryCommand> stream,
            Client client)
        {
            _target = target;
            _stream = stream;
            _client = client;
            _channel = Channel.CreateUnbounded<bool>();
        }

        public async Task Loop()
        {
            var reader = _stream.ResponseStream;
            var writer = _stream.RequestStream;
            var request = new rmq::TelemetryCommand
            {
                Settings = new rmq::Settings()
            };
            _client.BuildClientSetting(request.Settings);
            await writer.WriteAsync(request);
            Logger.Debug($"Writing Client Settings to {_target} Done: {request.Settings}");
            while (!_client.TelemetryCts().IsCancellationRequested)
            {
                if (await reader.MoveNext(_client.TelemetryCts().Token))
                {
                    var cmd = reader.Current;
                    Logger.Debug($"Received a TelemetryCommand from {_target}: {cmd}");
                    switch (cmd.CommandCase)
                    {
                        case rmq::TelemetryCommand.CommandOneofCase.None:
                            {
                                Logger.Warn($"Telemetry failed: {cmd.Status}");
                                if (0 == Interlocked.CompareExchange(ref _established, 0, 2))
                                {
                                    await _channel.Writer.WriteAsync(false);
                                }
                                break;
                            }
                        case rmq::TelemetryCommand.CommandOneofCase.Settings:
                            {
                                if (0 == Interlocked.CompareExchange(ref _established, 0, 1))
                                {
                                    await _channel.Writer.WriteAsync(true);
                                }

                                Logger.Info($"Received settings from {_target}: {cmd.Settings}");
                                _client.OnSettingsReceived(cmd.Settings);
                                break;
                            }
                        case rmq::TelemetryCommand.CommandOneofCase.PrintThreadStackTraceCommand:
                            {
                                break;
                            }
                        case rmq::TelemetryCommand.CommandOneofCase.RecoverOrphanedTransactionCommand:
                            {
                                break;
                            }
                        case rmq::TelemetryCommand.CommandOneofCase.VerifyMessageCommand:
                            {
                                break;
                            }
                    }
                }
            }
            Logger.Info($"Telemetry stream for {_target} is cancelled");
            await writer.CompleteAsync();
        }

        public async Task AwaitSettingNegotiationCompletion()
        {
            if (0 != Interlocked.Read(ref _established))
            {
                return;
            }

            Logger.Debug("Await setting negotiation");
            await _channel.Reader.ReadAsync();
        }

        private readonly grpc::AsyncDuplexStreamingCall<rmq::TelemetryCommand, rmq::TelemetryCommand> _stream;
        private readonly Client _client;

        private long _established;

        private readonly Channel<bool> _channel;
        private readonly string _target;
    };
}