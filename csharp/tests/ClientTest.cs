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
using Apache.Rocketmq.V2;
using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Endpoints = Org.Apache.Rocketmq.Endpoints;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class ClientTest
    {
        [TestMethod]
        public async Task TestOnVerifyMessageCommand()
        {
            var testClient = CreateTestClient();
            var endpoints = new Endpoints("testEndpoints");
            var command = new VerifyMessageCommand { Nonce = "testNonce" };

            var mockCall = new AsyncDuplexStreamingCall<TelemetryCommand, TelemetryCommand>(
                new MockClientStreamWriter<TelemetryCommand>(),
                new MockAsyncStreamReader<TelemetryCommand>(),
                null,
                null,
                null,
                null);
            var mockClientManager = new Mock<IClientManager>();
            mockClientManager.Setup(cm => cm.Telemetry(endpoints)).Returns(mockCall);

            testClient.SetClientManager(mockClientManager.Object);

            testClient.OnVerifyMessageCommand(endpoints, command);

            mockClientManager.Verify(cm => cm.Telemetry(endpoints), Times.Once);
        }

        [TestMethod]
        public async Task TestOnTopicRouteDataFetchedFailure()
        {
            var testClient = CreateTestClient();
            var endpoints = new Endpoints("testEndpoints");
            var mq = new Proto.MessageQueue
            {
                Topic = new Proto::Resource
                {
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic"
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Broker = new Proto::Broker
                {
                    Name = "testBroker",
                    Id = 0,
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses = { new Proto.Address { Host = "127.0.0.1", Port = 8080 } }
                    }
                }
            };
            var topicRouteData = new TopicRouteData(new[] { mq });

            var mockCall = new AsyncDuplexStreamingCall<TelemetryCommand, TelemetryCommand>(
                new MockClientStreamWriter<TelemetryCommand>(),
                new MockAsyncStreamReader<TelemetryCommand>(),
                null,
                null,
                null,
                null);
            var mockClientManager = new Mock<IClientManager>();
            mockClientManager.Setup(cm => cm.Telemetry(endpoints)).Returns(mockCall);

            testClient.SetClientManager(mockClientManager.Object);

            try
            {
                await testClient.OnTopicRouteDataFetched("testTopic", topicRouteData);
                Assert.Fail();
            }
            catch (Exception e)
            {
                mockClientManager.Verify(cm => cm.Telemetry(It.IsAny<Endpoints>()), Times.Once);
            }
        }

        [TestMethod]
        public async Task TestOnPrintThreadStackTraceCommand()
        {
            var testClient = CreateTestClient();
            var endpoints = new Endpoints("testEndpoints");
            var command = new PrintThreadStackTraceCommand { Nonce = "testNonce" };
            var mockCall = new AsyncDuplexStreamingCall<TelemetryCommand, TelemetryCommand>(
                new MockClientStreamWriter<TelemetryCommand>(),
                new MockAsyncStreamReader<TelemetryCommand>(),
                null,
                null,
                null,
                null);

            var mockClientManager = new Mock<IClientManager>();
            mockClientManager.Setup(cm => cm.Telemetry(endpoints)).Returns(mockCall);

            testClient.SetClientManager(mockClientManager.Object);

            // Act
            testClient.OnPrintThreadStackTraceCommand(endpoints, command);

            // Assert
            mockClientManager.Verify(cm => cm.Telemetry(endpoints), Times.Once);
        }

        private Client CreateTestClient()
        {
            return new Producer(new ClientConfig.Builder().SetEndpoints("127.0.0.1:9876").Build(),
                new ConcurrentDictionary<string, bool>(), 1, null);
        }

        private class MockClientStreamWriter<T> : IClientStreamWriter<T>
        {
            public Task WriteAsync(T message)
            {
                // Simulate async operation
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
                throw new System.NotImplementedException();
            }

            public T Current => throw new NotImplementedException();
        }
    }
}