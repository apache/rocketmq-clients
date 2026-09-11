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
using System.Linq;
using System.Threading.Tasks;
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
        private const string Topic = "testTopic";

        [TestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task TestUnsupportedCommandsReplyOnExistingSession(bool verify)
        {
            var (client, manager, stream, endpoints) = await ArrangeClient();
            try
            {
                await Reply(client, endpoints, verify);
                var response = stream.WrittenCommand(1);
                Assert.AreEqual(Proto.Code.Unsupported, response.Status.Code);
                Assert.AreEqual("nonce", verify ? response.VerifyMessageResult.Nonce : response.ThreadStackTrace.Nonce);
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Once);
            }
            finally
            {
                await client.DisposeAsync();
            }
        }

        [TestMethod]
        [DataRow(false)]
        [DataRow(true)]
        [Timeout(20000)]
        public async Task TestCancelledCommandReplyDoesNotEscape(bool verify)
        {
            var (client, manager, stream, endpoints) = await ArrangeClient();
            Task reply = null;
            try
            {
                stream.Writer.WriteGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                reply = Reply(client, endpoints, verify);
                await TestAwaiter.Until(() => stream.WrittenCommands == 2, "the command reply to block in the writer");
                Assert.IsFalse(reply.IsCompleted);

                await client.DisposeAsync();
                await reply.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.AreEqual(1, stream.DisposeCalls);
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Once);
            }
            finally
            {
                await client.DisposeAsync();
                if (reply != null)
                {
                    await reply;
                }
            }
        }

        [TestMethod]
        public async Task TestLateCommandsDoNotRecreateSessions()
        {
            var (client, manager, stream, endpoints) = await ArrangeClient();
            await client.DisposeAsync();
            await Reply(client, endpoints, false);
            await Reply(client, endpoints, true);
            client.ReconnectTelemetry(endpoints);
            Assert.AreEqual(1, stream.DisposeCalls);
            manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Once);
        }

        [TestMethod]
        public async Task TestOnTopicRouteDataFetchedFailure()
        {
            var client = CreateTestClient();
            var route = CreateRoute();
            var endpoints = route.MessageQueues.First().Broker.Endpoints;
            var manager = new Mock<IClientManager>();
            var failed = new FakeTelemetryCall();
            var gate = new TaskCompletionSource<bool>();
            gate.SetException(new RpcException(new Status(StatusCode.Unavailable, "settings write failed")));
            failed.Writer.WriteGate = gate;
            var renewed = new FakeTelemetryCall(true, SettingsCommand(client));
            manager.SetupSequence(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(failed.Call).Returns(renewed.Call);
            client.SetClientManager(manager.Object);
            try
            {
                await Assert.ThrowsExactlyAsync<RpcException>(() => client.OnTopicRouteDataFetched(Topic, route));
                Assert.IsTrue(client.IsEndpointsDeprecated(endpoints));
                Assert.AreEqual(1, failed.DisposeCalls);

                await client.OnTopicRouteDataFetched(Topic, route);
                Assert.IsFalse(client.IsEndpointsDeprecated(endpoints));
                Assert.IsNotNull(renewed.WrittenCommand(0).Settings);
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Exactly(2));
            }
            finally
            {
                await client.DisposeAsync();
                failed.Call.Dispose();
                renewed.Call.Dispose();
            }
        }

        [TestMethod]
        [DataRow(State.Starting)]
        [DataRow(State.Running)]
        [Timeout(20000)]
        public async Task TestInitializingEndpointCanRenewBeforeRoutePublication(State state)
        {
            var client = CreateTestClient();
            client.State = state;
            var route = CreateRoute();
            var endpoints = route.MessageQueues.First().Broker.Endpoints;
            var first = new FakeTelemetryCall(false);
            var renewed = new FakeTelemetryCall(true, SettingsCommand(client));
            var manager = new Mock<IClientManager>();
            manager.SetupSequence(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(first.Call).Returns(renewed.Call);
            client.SetClientManager(manager.Object);
            try
            {
                await Task.Run(() => client.OnTopicRouteDataFetched(Topic, route)).WaitAsync(TimeSpan.FromSeconds(10));
                await TestAwaiter.Until(() => renewed.WrittenCommands == 1, "settings on the replacement stream");
                Assert.IsFalse(client.IsEndpointsDeprecated(endpoints));
                Assert.AreEqual(1, first.DisposeCalls);
                await client.OnTopicRouteDataFetched(Topic, route);
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Exactly(2));
            }
            finally
            {
                await client.DisposeAsync();
                first.Call.Dispose();
                renewed.Call.Dispose();
            }
        }

        [TestMethod]
        [Timeout(15000)]
        public async Task TestConcurrentRoutesShareInitializationAndKeepEndpointLive()
        {
            var client = CreateTestClient();
            var route = CreateRoute();
            var endpoints = route.MessageQueues.First().Broker.Endpoints;
            var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var stream = new FakeTelemetryCall(true, SettingsCommand(client));
            stream.Writer.WriteGate = gate;
            var manager = new Mock<IClientManager>();
            manager.Setup(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(stream.Call);
            client.SetClientManager(manager.Object);
            var first = client.OnTopicRouteDataFetched(Topic, route);
            var second = client.OnTopicRouteDataFetched("anotherTopic", route);
            try
            {
                await TestAwaiter.Until(() => stream.WrittenCommands == 1, "the shared initialization write");
                Assert.IsFalse(first.IsCompleted);
                Assert.IsFalse(second.IsCompleted);
                await client.OnTopicRouteDataFetched(Topic, new TopicRouteData(Array.Empty<Proto.MessageQueue>()));
                Assert.IsFalse(client.IsEndpointsDeprecated(endpoints));
                Assert.AreEqual(0, stream.DisposeCalls);

                gate.SetResult(true);
                await Task.WhenAll(first, second);
                Assert.AreEqual(1, stream.WrittenCommands);
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Once);
            }
            finally
            {
                gate.TrySetResult(true);
                await Task.WhenAll(first, second);
                await client.DisposeAsync();
            }
        }

        [TestMethod]
        public async Task TestLateRouteResponseCannotCreateSessionAfterShutdown()
        {
            var client = CreateTestClient();
            var manager = new Mock<IClientManager>();
            var responseSource = new TaskCompletionSource<RpcInvocation<Proto.QueryRouteRequest, Proto.QueryRouteResponse>>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            manager.Setup(m => m.QueryRoute(It.IsAny<Endpoints>(), It.IsAny<Proto.QueryRouteRequest>(), It.IsAny<TimeSpan>()))
                .Returns(responseSource.Task);
            client.SetClientManager(manager.Object);
            var pending = client.FetchRoute(Topic);
            await client.DisposeAsync();
            var response = new Proto.QueryRouteResponse { Status = new Proto.Status { Code = Proto.Code.Ok } };
            response.MessageQueues.Add(CreateMessageQueue());
            responseSource.SetResult(new RpcInvocation<Proto.QueryRouteRequest, Proto.QueryRouteResponse>(
                new Proto.QueryRouteRequest(), response, new Metadata()));

            await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => pending);
            Assert.AreEqual(State.Terminated, client.State);
            manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Never);
        }

        [TestMethod]
        public async Task TestShutdownCancelsInitializationAndPreventsRoutePublication()
        {
            var client = CreateTestClient();
            var route = CreateRoute();
            var endpoints = route.MessageQueues.First().Broker.Endpoints;
            var stream = new FakeTelemetryCall();
            var manager = new Mock<IClientManager>();
            manager.Setup(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(stream.Call);
            client.SetClientManager(manager.Object);
            var pending = Task.Run(() => client.OnTopicRouteDataFetched(Topic, route));
            await TestAwaiter.Until(() => stream.WrittenCommands == 1, "initial settings to be waiting for a response");
            await client.DisposeAsync();
            await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => pending);
            Assert.IsTrue(client.IsEndpointsDeprecated(endpoints));
            Assert.AreEqual(1, stream.DisposeCalls);
        }

        [TestMethod]
        public async Task TestOnReconnectEndpointsCommand()
        {
            var client = CreateTestClient();
            var manager = new Mock<IClientManager>();
            client.SetClientManager(manager.Object);
            var endpoints = RecoveryTestSupport.FakeEndpoints;
            try
            {
                client.OnReconnectEndpointsCommand(endpoints, new Proto.ReconnectEndpointsCommand { Nonce = "nonce" });
                manager.Verify(m => m.Reconnect(endpoints), Times.Once);
            }
            finally
            {
                await client.DisposeAsync();
            }
        }

        [TestMethod]
        public async Task TestReaddedSessionIgnoresOldRecoveryEvenWhenPruningIsDelayed()
        {
            var (client, manager, first, endpoints) = await ArrangeClient();
            using var readded = new FakeTelemetryCall(true, SettingsCommand(client));
            using var renewed = new FakeTelemetryCall(true, SettingsCommand(client));
            manager.SetupSequence(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(readded.Call).Returns(renewed.Call);
            var recovery = new ClientManager(client);
            var rpc = new Mock<IRpcClient>();
            var oldHeartbeat = new TaskCompletionSource<Proto.HeartbeatResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
            Task oldMonitor = null;
            try
            {
                var oldSession = client.GetSessionIfPresent(endpoints);
                await recovery.MonitorHeartbeat(endpoints, rpc.Object,
                    RecoveryTestSupport.FailedHeartbeat(StatusCode.DeadlineExceeded));
                oldMonitor = recovery.MonitorHeartbeat(endpoints, rpc.Object, oldHeartbeat.Task);
                await client.OnTopicRouteDataFetched(Topic, new TopicRouteData(Array.Empty<Proto.MessageQueue>()));
                await client.OnTopicRouteDataFetched(Topic, CreateRoute());
                Assert.AreNotSame(oldSession, client.GetSessionIfPresent(endpoints));
                Assert.AreEqual(1, first.DisposeCalls);

                client.ReconnectTelemetry(endpoints, oldSession);
                Assert.AreEqual(0, readded.DisposeCalls);
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Exactly(2));

                oldHeartbeat.SetException(new RpcException(new Status(StatusCode.DeadlineExceeded, "retired heartbeat")));
                await oldMonitor;
                await recovery.MonitorHeartbeat(endpoints, rpc.Object,
                    RecoveryTestSupport.FailedHeartbeat(StatusCode.DeadlineExceeded));
                rpc.Verify(r => r.ResetTransport(), Times.Never);

                await recovery.MonitorHeartbeat(endpoints, rpc.Object,
                    RecoveryTestSupport.FailedHeartbeat(StatusCode.DeadlineExceeded));
                await TestAwaiter.Until(() => renewed.WrittenCommands == 1, "recovery of the new session only");
                rpc.Verify(r => r.ResetTransport(), Times.Once);
            }
            finally
            {
                oldHeartbeat.TrySetResult(new Proto.HeartbeatResponse());
                if (oldMonitor != null)
                {
                    await oldMonitor;
                }

                await recovery.Shutdown();
                await client.DisposeAsync();
            }
        }

        private static Task Reply(Client client, Endpoints endpoints, bool verify)
        {
            return verify
                ? client.OnVerifyMessageCommand(endpoints, new Proto.VerifyMessageCommand { Nonce = "nonce" })
                : client.OnPrintThreadStackTraceCommand(endpoints, new Proto.PrintThreadStackTraceCommand { Nonce = "nonce" });
        }

        private static async Task<(RouteTestClient, Mock<IClientManager>, FakeTelemetryCall, Endpoints)> ArrangeClient()
        {
            var client = CreateTestClient();
            var stream = new FakeTelemetryCall(true, SettingsCommand(client));
            var manager = new Mock<IClientManager>();
            manager.Setup(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(stream.Call);
            client.SetClientManager(manager.Object);
            var route = CreateRoute();
            await client.OnTopicRouteDataFetched(Topic, route);
            return (client, manager, stream, route.MessageQueues.First().Broker.Endpoints);
        }

        private static RouteTestClient CreateTestClient()
        {
            return new RouteTestClient();
        }

        private static Proto.TelemetryCommand SettingsCommand(Client client)
        {
            return new Proto.TelemetryCommand { Settings = client.GetSettings().ToProtobuf() };
        }

        private static TopicRouteData CreateRoute()
        {
            return new TopicRouteData(new[] { CreateMessageQueue() });
        }

        private static Proto.MessageQueue CreateMessageQueue()
        {
            return new Proto.MessageQueue
            {
                Topic = new Proto.Resource { Name = Topic },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Broker = new Proto.Broker
                {
                    Name = "broker", Id = 0,
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses = { new Proto.Address { Host = "127.0.0.1", Port = 8080 } }
                    }
                }
            };
        }

        private sealed class RouteTestClient : Producer
        {
            internal RouteTestClient() : base(RecoveryTestSupport.CreateClientConfig(), new ConcurrentDictionary<string, bool>(), 1, null)
            {
                State = State.Running;
            }

            internal Task<TopicRouteData> FetchRoute(string topic)
            {
                return GetRouteData(topic);
            }
        }
    }
}
