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
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class PushConsumerTest
    {
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestSubscribeBeforeStartup()
        {
            var pushConsumer = CreatePushConsumer();
            await pushConsumer.Subscribe("testTopic", new FilterExpression("*"));
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestUnsubscribeBeforeStartup()
        {
            var pushConsumer = CreatePushConsumer();
            pushConsumer.Unsubscribe("testTopic");
        }

        [TestMethod]
        public async Task TestQueryAssignment()
        {
            var (pushConsumer, mockClientManager, queryRouteResponse, metadata) = SetupMockConsumer();

            var queryAssignmentResponse = CreateQueryAssignmentResponse();
            var queryAssignmentInvocation =
                new RpcInvocation<Proto.QueryAssignmentRequest, Proto.QueryAssignmentResponse>(null,
                queryAssignmentResponse, metadata);

            SetupMockClientManager(mockClientManager, queryRouteResponse, queryAssignmentInvocation, metadata);
            await pushConsumer.QueryAssignment("testTopic");
        }

        [TestMethod]
        public async Task TestScanAssignments()
        {
            var (pushConsumer, mockClientManager, queryRouteResponse, metadata) = SetupMockConsumer();

            var queryAssignmentResponse = CreateQueryAssignmentResponse(new Proto.Assignment
            {
                MessageQueue = queryRouteResponse.MessageQueues[0]
            });
            var queryAssignmentInvocation =
                new RpcInvocation<Proto.QueryAssignmentRequest, Proto.QueryAssignmentResponse>(null,
                queryAssignmentResponse, metadata);

            SetupMockClientManager(mockClientManager, queryRouteResponse, queryAssignmentInvocation, metadata);

            pushConsumer.State = State.Running;
            await pushConsumer.Subscribe("testTopic", new FilterExpression("*"));
            pushConsumer.ScanAssignments();
        }

        [TestMethod]
        public async Task TestScanAssignmentsWithoutResults()
        {
            var (pushConsumer, mockClientManager, queryRouteResponse, metadata) = SetupMockConsumer();

            var queryAssignmentResponse = CreateQueryAssignmentResponse();
            var queryAssignmentInvocation =
                new RpcInvocation<Proto.QueryAssignmentRequest, Proto.QueryAssignmentResponse>(null,
                queryAssignmentResponse, metadata);

            SetupMockClientManager(mockClientManager, queryRouteResponse, queryAssignmentInvocation, metadata);

            pushConsumer.State = State.Running;
            await pushConsumer.Subscribe("testTopic", new FilterExpression("*"));
            pushConsumer.ScanAssignments();
        }

        private PushConsumer CreatePushConsumer()
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1")
                .Build();
            return new PushConsumer(clientConfig, "testGroup",
                new ConcurrentDictionary<string, FilterExpression>(), new TestMessageListener(),
                10, 10, 1);
        }

        private class TestMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView)
            {
                return ConsumeResult.SUCCESS;
            }
        }

        private class MockClientStreamWriter<T> : IClientStreamWriter<T>
        {
            public Task WriteAsync(T message)
            {
                return Task.CompletedTask;
            }

            public WriteOptions WriteOptions { get; set; }

            public Task CompleteAsync()
            {
                throw new NotImplementedException();
            }
        }

        private class MockAsyncStreamReader<T> : IAsyncStreamReader<T>
        {
            public Task<bool> MoveNext(CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public T Current => throw new NotImplementedException();
        }

        private (PushConsumer, Mock<IClientManager>, Proto.QueryRouteResponse, Metadata) SetupMockConsumer()
        {
            var pushConsumer = CreatePushConsumer();
            var metadata = pushConsumer.Sign();

            var mq = new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = "broker0",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses = { new Proto.Address { Host = "127.0.0.1", Port = 8080 } }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic",
                },
                AcceptMessageTypes = { Proto.MessageType.Normal }
            };

            var queryRouteResponse = new Proto.QueryRouteResponse
            {
                Status = new Proto.Status { Code = Proto.Code.Ok },
                MessageQueues = { mq }
            };

            var mockClientManager = new Mock<IClientManager>();
            pushConsumer.SetClientManager(mockClientManager.Object);
            return (pushConsumer, mockClientManager, queryRouteResponse, metadata);
        }

        private Proto.QueryAssignmentResponse CreateQueryAssignmentResponse(params Proto.Assignment[] assignments)
        {
            return new Proto.QueryAssignmentResponse
            {
                Status = new Proto.Status { Code = Proto.Code.Ok },
                Assignments = { assignments }
            };
        }

        private void SetupMockClientManager(Mock<IClientManager> mockClientManager,
            Proto.QueryRouteResponse queryRouteResponse,
            RpcInvocation<Proto.QueryAssignmentRequest, Proto.QueryAssignmentResponse> queryAssignmentInvocation,
            Metadata metadata)
        {
            var queryRouteInvocation = new RpcInvocation<Proto.QueryRouteRequest, Proto.QueryRouteResponse>(null,
                queryRouteResponse, metadata);

            mockClientManager.Setup(cm =>
                cm.QueryRoute(It.IsAny<Endpoints>(), It.IsAny<Proto.QueryRouteRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(queryRouteInvocation));

            var mockCall = new AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand>(
                new MockClientStreamWriter<Proto.TelemetryCommand>(),
                new MockAsyncStreamReader<Proto.TelemetryCommand>(),
                null, null, null, null);

            mockClientManager.Setup(cm =>
                cm.QueryAssignment(It.IsAny<Endpoints>(), It.IsAny<Proto.QueryAssignmentRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(queryAssignmentInvocation));

            mockClientManager.Setup(cm => cm.Telemetry(It.IsAny<Endpoints>())).Returns(mockCall);
        }
    }
}