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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    public class MockServer : Proto.MessagingService.MessagingServiceBase
    {
        private readonly List<string> _attemptIdList;
        private int _serverDeadlineFlag = 1;

        private readonly Proto.Status _mockStatus = new Proto.Status
        {
            Code = Proto.Code.Ok,
            Message = "mock test"
        };

        private readonly string _topic;
        private readonly string _broker;

        public MockServer(string topic, string broker, List<string> attemptIdList)
        {
            _topic = topic;
            _broker = broker;
            _attemptIdList = attemptIdList;
        }

        public int Port { get; set; }

        public override Task<Proto.QueryRouteResponse> QueryRoute(Proto.QueryRouteRequest request,
            ServerCallContext context)
        {
            var response = new Proto.QueryRouteResponse
            {
                Status = _mockStatus,
                MessageQueues =
                {
                    new Proto.MessageQueue
                    {
                        Topic = new Proto.Resource { Name = _topic },
                        Id = 0,
                        Permission = Proto.Permission.ReadWrite,
                        Broker = new Proto.Broker
                        {
                            Name = _broker,
                            Id = 0,
                            Endpoints = new Proto.Endpoints
                            {
                                Addresses =
                                {
                                    new Proto.Address { Host = "127.0.0.1", Port = Port }
                                }
                            }
                        },
                        AcceptMessageTypes = { Proto.MessageType.Normal }
                    }
                }
            };
            return Task.FromResult(response);
        }

        public override Task<Proto.HeartbeatResponse> Heartbeat(Proto.HeartbeatRequest request,
            ServerCallContext context)
        {
            var response = new Proto.HeartbeatResponse { Status = _mockStatus };
            return Task.FromResult(response);
        }

        public override Task<Proto.QueryAssignmentResponse> QueryAssignment(Proto.QueryAssignmentRequest request,
            ServerCallContext context)
        {
            var response = new Proto.QueryAssignmentResponse
            {
                Status = _mockStatus,
                Assignments =
                {
                    new Proto.Assignment
                    {
                        MessageQueue = new Proto.MessageQueue
                        {
                            Topic = new Proto.Resource { Name = _topic },
                            Id = 0,
                            Permission = Proto.Permission.ReadWrite,
                            Broker = new Proto.Broker
                            {
                                Name = _broker,
                                Id = 0,
                                Endpoints = new Proto.Endpoints
                                {
                                    Addresses =
                                    {
                                        new Proto.Address { Host = "127.0.0.1", Port = Port }
                                    }
                                }
                            },
                            AcceptMessageTypes = { Proto.MessageType.Normal }
                        }
                    }
                }
            };
            return Task.FromResult(response);
        }

        public override async Task ReceiveMessage(Proto.ReceiveMessageRequest request,
            IServerStreamWriter<Proto.ReceiveMessageResponse> responseStream, ServerCallContext context)
        {
            if (_attemptIdList.Count >= 3)
            {
                await Task.Delay(100);
            }

            _attemptIdList.Add(request.AttemptId);

            if (CompareAndSetServerDeadlineFlag(true, false))
            {
                // timeout
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
            else
            {
                var response = new Proto.ReceiveMessageResponse { Status = _mockStatus };
                await responseStream.WriteAsync(response);
            }
        }

        public override async Task Telemetry(IAsyncStreamReader<Proto.TelemetryCommand> requestStream,
            IServerStreamWriter<Proto.TelemetryCommand> responseStream, ServerCallContext context)
        {
            await foreach (var command in requestStream.ReadAllAsync())
            {
                var response = command.Clone();
                response.Status = _mockStatus;
                response.Settings = new Proto.Settings
                {
                    BackoffPolicy = new Proto.RetryPolicy
                    {
                        MaxAttempts = 16,
                        ExponentialBackoff = new Proto.ExponentialBackoff
                        {
                            Initial = new Duration { Seconds = 1 },
                            Max = new Duration { Seconds = 10 },
                            Multiplier = 1.5f
                        }
                    }
                };

                await responseStream.WriteAsync(response);
            }
        }

        private bool CompareAndSetServerDeadlineFlag(bool expectedValue, bool newValue)
        {
            var expected = expectedValue ? 1 : 0;
            var newVal = newValue ? 1 : 0;
            return Interlocked.CompareExchange(ref _serverDeadlineFlag, newVal, expected) == expected;
        }
    }
}