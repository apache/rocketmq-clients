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
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Endpoints = Org.Apache.Rocketmq.Endpoints;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    /// <summary>
    /// Consumer recovery must follow the subscription route: renew active sessions, retire obsolete ones, and create
    /// a fresh session when the same endpoints return.
    /// </summary>
    [TestClass]
    public class PushConsumerRecoveryTest
    {
        private const string Topic = "topic";
        private const string Broker = "broker";
        private const string TestConsumerGroup = "testGroup";

        [TestMethod]
        [Timeout(30000)]
        public async Task TestPushConsumerRenewsTelemetryForEndpointsStillInItsRouteTable()
        {
            using var first = new FakeTelemetryCall(true, SettingsCommand());
            using var renewed = new FakeTelemetryCall(true, SettingsCommand());
            var consumer = CreateConsumer(out var clientManager, first, renewed);
            try
            {
                var route = CreateTopicRouteData();
                var endpoints = route.MessageQueues[0].Broker.Endpoints;
                await consumer.OnTopicRouteDataFetched(Topic, route);
                AssertSettingsWritten(first);
                Assert.IsFalse(consumer.IsEndpointsDeprecated(endpoints));

                consumer.ReconnectTelemetry(endpoints);

                await TestAwaiter.Until(() => renewed.WrittenCommands > 0 && 1 == first.DisposeCalls,
                    "consumer renewal to write settings and dispose the old stream");
                AssertSettingsWritten(renewed);
                Assert.AreEqual(0, renewed.DisposeCalls);
                Assert.IsFalse(consumer.IsEndpointsDeprecated(endpoints));
                clientManager.Verify(m => m.Telemetry(endpoints), Times.Exactly(2));
            }
            finally
            {
                await consumer.DisposeAsync();
            }
        }

        [TestMethod]
        [Timeout(30000)]
        public async Task TestPushConsumerDropsObsoleteSessionAndReaddsSameEndpointsWithANewStream()
        {
            using var first = new FakeTelemetryCall(true, SettingsCommand());
            using var rejoined = new FakeTelemetryCall(true, SettingsCommand());
            var consumer = CreateConsumer(out var clientManager, first, rejoined);
            try
            {
                var route = CreateTopicRouteData();
                var endpoints = route.MessageQueues[0].Broker.Endpoints;
                await consumer.OnTopicRouteDataFetched(Topic, route);
                AssertSettingsWritten(first);

                await consumer.OnTopicRouteDataFetched(Topic, new TopicRouteData(Array.Empty<Proto.MessageQueue>()));

                Assert.IsTrue(consumer.IsEndpointsDeprecated(endpoints));
                Assert.AreEqual(1, first.DisposeCalls,
                    "publishing an empty route must close the obsolete stream without a reconnect callback");
                Assert.AreEqual(0, rejoined.WrittenCommands);
                clientManager.Verify(m => m.Telemetry(endpoints), Times.Once);

                await consumer.OnTopicRouteDataFetched(Topic, route);

                Assert.IsFalse(consumer.IsEndpointsDeprecated(endpoints));
                AssertSettingsWritten(rejoined);
                Assert.AreEqual(0, rejoined.DisposeCalls);
                Assert.AreEqual(1, first.DisposeCalls);
                clientManager.Verify(m => m.Telemetry(endpoints), Times.Exactly(2));
            }
            finally
            {
                await consumer.DisposeAsync();
            }
        }

        [TestMethod]
        [Timeout(30000)]
        public async Task TestPushConsumerShutdownClosesTheSessionsOfItsRouteTable()
        {
            using var stream = new FakeTelemetryCall(true, SettingsCommand());
            var consumer = CreateConsumer(out var clientManager, stream);
            try
            {
                var route = CreateTopicRouteData();
                var endpoints = route.MessageQueues[0].Broker.Endpoints;
                await consumer.OnTopicRouteDataFetched(Topic, route);
                AssertSettingsWritten(stream);

                await consumer.DisposeAsync();

                Assert.AreEqual(State.Terminated, consumer.State);
                Assert.AreEqual(1, stream.DisposeCalls,
                    "public consumer disposal must close its telemetry stream");
                consumer.ReconnectTelemetry(endpoints);
                clientManager.Verify(m => m.Telemetry(endpoints), Times.Once);
            }
            finally
            {
                await consumer.DisposeAsync();
            }
        }

        [TestMethod]
        [Timeout(30000)]
        public async Task TestPushConsumerAutomaticallyRenewsTelemetryAfterEof()
        {
            using var first = new FakeTelemetryCall(true, SettingsCommand());
            using var renewed = new FakeTelemetryCall(true, SettingsCommand());
            var consumer = CreateConsumer(out var clientManager, first, renewed);
            try
            {
                var route = CreateTopicRouteData();
                var endpoints = route.MessageQueues[0].Broker.Endpoints;
                await consumer.OnTopicRouteDataFetched(Topic, route);
                AssertSettingsWritten(first);

                first.Reader.Close();

                await TestAwaiter.Until(() => renewed.WrittenCommands > 0 && 1 == first.DisposeCalls,
                    "EOF to renew an active consumer's stream without an explicit reconnect");
                AssertSettingsWritten(renewed);
                Assert.AreEqual(0, renewed.DisposeCalls);
                Assert.IsFalse(consumer.IsEndpointsDeprecated(endpoints));
                clientManager.Verify(m => m.Telemetry(endpoints), Times.Exactly(2));
            }
            finally
            {
                await consumer.DisposeAsync();
            }
        }

        [TestMethod]
        public async Task TestMalformedVerificationCommandDoesNotEscape()
        {
            using var stream = new FakeTelemetryCall(true, SettingsCommand());
            var consumer = CreateConsumer(out var manager, stream);
            try
            {
                var route = CreateTopicRouteData();
                var endpoints = route.MessageQueues[0].Broker.Endpoints;
                await consumer.OnTopicRouteDataFetched(Topic, route);
                await consumer.OnVerifyMessageCommand(endpoints, new Proto.VerifyMessageCommand { Nonce = "nonce" });
                Assert.AreEqual(1, stream.WrittenCommands);
                Assert.AreEqual(0, stream.DisposeCalls);
                manager.Verify(m => m.Telemetry(endpoints), Times.Once);
            }
            finally
            {
                await consumer.DisposeAsync();
            }
        }

        private static PushConsumer CreateConsumer(out Mock<IClientManager> clientManager,
            params FakeTelemetryCall[] streams)
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1:8080")
                .EnableSsl(false)
                .Build();
            var consumer = new PushConsumer(clientConfig, TestConsumerGroup,
                new ConcurrentDictionary<string, FilterExpression>(), new NoopMessageListener(), 10, 10, 1)
            {
                State = State.Running
            };
            clientManager = new Mock<IClientManager>();
            var telemetry = clientManager.SetupSequence(m => m.Telemetry(It.IsAny<Endpoints>()));
            foreach (var stream in streams)
            {
                telemetry.Returns(stream.Call);
            }

            telemetry.Throws(new InvalidOperationException("Unexpected telemetry stream creation"));
            consumer.SetClientManager(clientManager.Object);
            return consumer;
        }

        private static void AssertSettingsWritten(FakeTelemetryCall stream)
        {
            Assert.IsTrue(stream.WrittenCommands > 0, "the consumer must write settings on the stream");
            Assert.IsNotNull(stream.WrittenCommand(0).Settings);
        }

        private static TopicRouteData CreateTopicRouteData()
        {
            var messageQueue = new Proto.MessageQueue
            {
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "namespace",
                    Name = Topic
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Broker = new Proto.Broker
                {
                    Name = Broker,
                    Id = 0,
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses = { new Proto.Address { Host = "127.0.0.1", Port = 8080 } }
                    }
                }
            };
            return new TopicRouteData(new[] { messageQueue });
        }

        /// <summary>
        /// Settings a push consumer accepts: its subscription settings reject a retry policy which carries no strategy,
        /// and an exponential backoff without its durations would fault the read loop which delivers them.
        /// </summary>
        private static Proto.TelemetryCommand SettingsCommand()
        {
            return new Proto.TelemetryCommand
            {
                Settings = new Proto.Settings
                {
                    Subscription = new Proto.Subscription(),
                    BackoffPolicy = new Proto.RetryPolicy
                    {
                        MaxAttempts = 3,
                        ExponentialBackoff = new Proto.ExponentialBackoff
                        {
                            Initial = new Duration { Seconds = 1 },
                            Max = new Duration { Seconds = 10 },
                            Multiplier = 2
                        }
                    }
                }
            };
        }

        private sealed class NoopMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView)
            {
                return ConsumeResult.SUCCESS;
            }
        }
    }
}
