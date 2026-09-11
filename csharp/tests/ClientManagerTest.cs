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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Apache.Rocketmq.V2;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Endpoints = Org.Apache.Rocketmq.Endpoints;
using grpcLib = Grpc.Core;

namespace tests
{
    [TestClass]
    public class ClientManagerTest
    {
        private static readonly Endpoints FakeEndpoints = RecoveryTestSupport.FakeEndpoints;
        private ClientManager _clientManager;
        private Mock<IRpcClient> _rpcClient;

        private readonly ClientConfig _clientConfig = new ClientConfig.Builder()
            .SetEndpoints("127.0.0.1:8080")
            .Build();

        [TestInitialize]
        public void Initialize()
        {
            _clientManager = new ClientManager(CreateTestClient());
            _rpcClient = new Mock<IRpcClient>();
            _rpcClient.Setup(c => c.Shutdown()).Returns(Task.CompletedTask);
            CacheRpcClient(_clientManager, _rpcClient.Object);
        }

        [TestCleanup]
        public async Task Cleanup()
        {
            await _clientManager.Shutdown();
        }

        [TestMethod]
        public async Task TestHeartbeat()
        {
            var request = new HeartbeatRequest();
            var response = new HeartbeatResponse();
            _rpcClient.Setup(c => c.Heartbeat(It.IsAny<grpcLib.Metadata>(), request, It.IsAny<TimeSpan>()))
                .ReturnsAsync(response);

            var invocation = await _clientManager.Heartbeat(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            Assert.AreSame(response, invocation.Response);
            await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
                _clientManager.Heartbeat(null, request, TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        public async Task TestSendMessage()
        {
            var request = new SendMessageRequest();
            var response = new SendMessageResponse();
            _rpcClient.Setup(c => c.SendMessage(It.IsAny<grpcLib.Metadata>(), request, It.IsAny<TimeSpan>()))
                .ReturnsAsync(response);

            var invocation = await _clientManager.SendMessage(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            Assert.AreSame(response, invocation.Response);
            await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
                _clientManager.SendMessage(null, request, TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        public async Task TestQueryAssignment()
        {
            var request = new QueryAssignmentRequest();
            var response = new QueryAssignmentResponse();
            _rpcClient.Setup(c => c.QueryAssignment(It.IsAny<grpcLib.Metadata>(), request, It.IsAny<TimeSpan>()))
                .ReturnsAsync(response);

            var invocation = await _clientManager.QueryAssignment(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            Assert.AreSame(response, invocation.Response);
            await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
                _clientManager.QueryAssignment(null, request, TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        public async Task TestReceiveMessage()
        {
            var request = new ReceiveMessageRequest();
            var response = new List<ReceiveMessageResponse>();
            _rpcClient.Setup(c => c.ReceiveMessage(It.IsAny<grpcLib.Metadata>(), request, It.IsAny<TimeSpan>()))
                .ReturnsAsync(response);

            var invocation = await _clientManager.ReceiveMessage(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            Assert.AreSame(response, invocation.Response);
            await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
                _clientManager.ReceiveMessage(null, request, TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        public async Task TestAckMessage()
        {
            var request = new AckMessageRequest();
            var response = new AckMessageResponse();
            _rpcClient.Setup(c => c.AckMessage(It.IsAny<grpcLib.Metadata>(), request, It.IsAny<TimeSpan>()))
                .ReturnsAsync(response);

            var invocation = await _clientManager.AckMessage(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            Assert.AreSame(response, invocation.Response);
            await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
                _clientManager.AckMessage(null, request, TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        public async Task TestChangeInvisibleDuration()
        {
            var request = new ChangeInvisibleDurationRequest();
            var response = new ChangeInvisibleDurationResponse();
            _rpcClient.Setup(c => c.ChangeInvisibleDuration(It.IsAny<grpcLib.Metadata>(), request, It.IsAny<TimeSpan>()))
                .ReturnsAsync(response);

            var invocation = await _clientManager.ChangeInvisibleDuration(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            Assert.AreSame(response, invocation.Response);
            await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
                _clientManager.ChangeInvisibleDuration(null, request, TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        public async Task TestForwardMessageToDeadLetterQueue()
        {
            var request = new ForwardMessageToDeadLetterQueueRequest();
            var response = new ForwardMessageToDeadLetterQueueResponse();
            _rpcClient.Setup(c => c.ForwardMessageToDeadLetterQueue(It.IsAny<grpcLib.Metadata>(), request,
                    It.IsAny<TimeSpan>()))
                .ReturnsAsync(response);

            var invocation = await _clientManager.ForwardMessageToDeadLetterQueue(FakeEndpoints, request,
                TimeSpan.FromSeconds(1));
            Assert.AreSame(response, invocation.Response);
            await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
                _clientManager.ForwardMessageToDeadLetterQueue(null, request, TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        public async Task TestEndTransaction()
        {
            var request = new EndTransactionRequest();
            var response = new EndTransactionResponse();
            _rpcClient.Setup(c => c.EndTransaction(It.IsAny<grpcLib.Metadata>(), request, It.IsAny<TimeSpan>()))
                .ReturnsAsync(response);

            var invocation = await _clientManager.EndTransaction(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            Assert.AreSame(response, invocation.Response);
            await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
                _clientManager.EndTransaction(null, request, TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        public async Task TestNotifyClientTermination()
        {
            var request = new NotifyClientTerminationRequest();
            var response = new NotifyClientTerminationResponse();
            _rpcClient.Setup(c => c.NotifyClientTermination(It.IsAny<grpcLib.Metadata>(), request, It.IsAny<TimeSpan>()))
                .ReturnsAsync(response);

            var invocation = await _clientManager.NotifyClientTermination(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            Assert.AreSame(response, invocation.Response);
            await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
                _clientManager.NotifyClientTermination(null, request, TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        public async Task TestRecallMessage()
        {
            var request = new RecallMessageRequest();
            var response = new RecallMessageResponse();
            _rpcClient.Setup(c => c.RecallMessage(It.IsAny<grpcLib.Metadata>(), request, It.IsAny<TimeSpan>()))
                .ReturnsAsync(response);

            var invocation = await _clientManager.RecallMessage(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            Assert.AreSame(response, invocation.Response);
            await Assert.ThrowsExactlyAsync<ArgumentNullException>(() =>
                _clientManager.RecallMessage(null, request, TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        public async Task TestHeartbeatDeadlineExceededTriggersRecoveryAfterThreshold()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();

            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
            rpcClient.Verify(c => c.ResetTransport(), Times.Never);

            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
            rpcClient.Verify(c => c.ResetTransport(), Times.Once);
            Assert.AreEqual(1, client.ReconnectTelemetryCalls);
        }

        [TestMethod]
        public async Task TestHeartbeatSuccessResetsFailureAttempts()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();

            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                Task.FromResult(new HeartbeatResponse()));
            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));

            rpcClient.Verify(c => c.ResetTransport(), Times.Never);
        }

        [TestMethod]
        public async Task TestHeartbeatUnavailableTriggersRecoveryWhenChannelIsReady()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            rpcClient.Setup(c => c.State).Returns(grpcLib.ConnectivityState.Ready);

            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.Unavailable));

            rpcClient.Verify(c => c.ResetTransport(), Times.Once);
            Assert.AreEqual(1, client.ReconnectTelemetryCalls);
        }

        [TestMethod]
        public async Task TestHeartbeatUnavailableDoesNotRecoverChannelWhichIsNotReady()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            rpcClient.Setup(c => c.State).Returns(grpcLib.ConnectivityState.TransientFailure);

            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.Unavailable));
            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));

            rpcClient.Verify(c => c.ResetTransport(), Times.Never);
        }

        [TestMethod]
        public async Task TestHeartbeatResourceExhaustedDoesNotTriggerRecovery()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            rpcClient.Setup(c => c.State).Returns(grpcLib.ConnectivityState.Ready);

            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.ResourceExhausted));
            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.ResourceExhausted));

            rpcClient.Verify(c => c.ResetTransport(), Times.Never);
        }

        [TestMethod]
        public async Task TestHeartbeatRecoveryHasCooldown()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();

            for (var i = 0; i < 2 * ClientManager.HeartbeatFailureThreshold; i++)
            {
                await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                    RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
            }

            rpcClient.Verify(c => c.ResetTransport(), Times.Once);
            Assert.AreEqual(1, client.ReconnectTelemetryCalls);
        }

        [TestMethod]
        public async Task TestServerReconnectIgnoresHeartbeatCooldown()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();

            for (var i = 0; i < ClientManager.HeartbeatFailureThreshold; i++)
            {
                await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                    RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
            }

            clientManager.Reconnect(FakeEndpoints, rpcClient.Object);

            rpcClient.Verify(c => c.ResetTransport(), Times.Exactly(2));
            Assert.AreEqual(2, client.ReconnectTelemetryCalls);
        }

        [TestMethod]
        public async Task TestConcurrentReconnectOnlyRecoversOnce()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            const int threadCount = 8;

            var recoveryStarted =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var allowRecoveryToComplete =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            rpcClient.Setup(c => c.ResetTransport()).Callback(() =>
            {
                recoveryStarted.TrySetResult(true);
                allowRecoveryToComplete.Task.Wait(TimeSpan.FromSeconds(5));
            });

            using var barrier = new Barrier(threadCount + 1);
            var reconnects = Enumerable.Range(0, threadCount)
                .Select(_ => Task.Run(() =>
                {
                    barrier.SignalAndWait(TimeSpan.FromSeconds(5));
                    clientManager.Reconnect(FakeEndpoints, rpcClient.Object);
                }))
                .ToList();
            barrier.SignalAndWait(TimeSpan.FromSeconds(5));
            try
            {
                await recoveryStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

                // Every other caller must skip the recovery instead of queueing behind the one in progress.
                await TestAwaiter.Until(() => reconnects.Count(task => task.IsCompleted) == threadCount - 1,
                    "the concurrent reconnects to be skipped");

                allowRecoveryToComplete.SetResult(true);
                await Task.WhenAll(reconnects).WaitAsync(TimeSpan.FromSeconds(5));

                rpcClient.Verify(c => c.ResetTransport(), Times.Once);
                Assert.AreEqual(1, client.ReconnectTelemetryCalls);
            }
            finally
            {
                allowRecoveryToComplete.TrySetResult(true);
                await Task.WhenAll(reconnects).WaitAsync(TimeSpan.FromSeconds(5));
                await clientManager.Shutdown();
            }
        }

        [TestMethod]
        public async Task TestTransportRecoveryIsSkippedWhenClientIsNotRunning()
        {
            var client = CreateRecoveryTestClient();
            client.State = Org.Apache.Rocketmq.State.Stopping;
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            rpcClient.Setup(c => c.State).Returns(grpcLib.ConnectivityState.Ready);

            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
            await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.Unavailable));

            rpcClient.Verify(c => c.ResetTransport(), Times.Never);
            Assert.AreEqual(0, client.ReconnectTelemetryCalls);
        }

        [TestMethod]
        public void TestReconnectWithoutCachedRpcClientIsIgnored()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);

            clientManager.Reconnect(FakeEndpoints);

            Assert.AreEqual(0, client.ReconnectTelemetryCalls);
        }

        [TestMethod]
        public void TestRecoveryReplacesTheTransportBeforeItRebuildsTheTelemetry()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();

            var order = new List<string>();
            rpcClient.Setup(c => c.ResetTransport()).Callback(() =>
            {
                lock (order)
                {
                    order.Add("transport");
                }
            });
            client.ReconnectTelemetryHook = _ =>
            {
                lock (order)
                {
                    order.Add("telemetry");
                }
            };

            clientManager.Reconnect(FakeEndpoints, rpcClient.Object);

            // Disposing the replaced channel is what faults a writer stuck on a half-open connection, releasing the
            // stream lock which the renewal then needs. Renewing first would let it stall behind that writer.
            Assert.AreEqual("transport,telemetry", string.Join(",", order));
        }

        [TestMethod]
        public async Task TestHeartbeatCallbacksDrainInRegistrationOrder()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            var first = new TaskCompletionSource<HeartbeatResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
            var success = new TaskCompletionSource<HeartbeatResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
            var last = new TaskCompletionSource<HeartbeatResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
            var firstMonitor = clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object, first.Task);
            var successMonitor = clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object, success.Task);
            var lastMonitor = clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object, last.Task);
            try
            {
                first.SetException(new grpcLib.RpcException(new grpcLib.Status(grpcLib.StatusCode.DeadlineExceeded, "first")));
                await firstMonitor.WaitAsync(TimeSpan.FromSeconds(5));
                last.SetException(new grpcLib.RpcException(new grpcLib.Status(grpcLib.StatusCode.DeadlineExceeded, "last")));
                // Force the last callback to run before the success callback. Its result must stay behind success.
                await lastMonitor.WaitAsync(TimeSpan.FromSeconds(5));
                rpcClient.Verify(c => c.ResetTransport(), Times.Never);

                success.SetResult(new HeartbeatResponse());
                await successMonitor.WaitAsync(TimeSpan.FromSeconds(5));
                rpcClient.Verify(c => c.ResetTransport(), Times.Never);
                Assert.AreEqual(0, client.ReconnectTelemetryCalls);
            }
            finally
            {
                first.TrySetResult(new HeartbeatResponse());
                success.TrySetResult(new HeartbeatResponse());
                last.TrySetResult(new HeartbeatResponse());
                await Task.WhenAll(firstMonitor, successMonitor, lastMonitor).WaitAsync(TimeSpan.FromSeconds(5));
                await clientManager.Shutdown();
            }
        }

        [TestMethod]
        public async Task TestAlternatingCompletedHeartbeatsOnOnePoolCallerNeverRecover()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            try
            {
                await Task.Run(async () =>
                {
                    var monitors = new List<Task>();
                    for (var i = 0; i < 1000; i++)
                    {
                        monitors.Add(clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                            RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded)));
                        monitors.Add(clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                            Task.FromResult(new HeartbeatResponse())));
                    }

                    await Task.WhenAll(monitors);
                }).WaitAsync(TimeSpan.FromSeconds(5));

                rpcClient.Verify(c => c.ResetTransport(), Times.Never);
                Assert.AreEqual(0, client.ReconnectTelemetryCalls);
            }
            finally
            {
                await clientManager.Shutdown();
            }
        }

        [TestMethod]
        public async Task TestHeartbeatRecoveryResumesAfterCooldownExpires()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            try
            {
                for (var i = 0; i < 2 * ClientManager.HeartbeatFailureThreshold; i++)
                {
                    await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                        RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
                }

                rpcClient.Verify(c => c.ResetTransport(), Times.Once);
                ExpireRecoveryCooldown(clientManager);
                // Failures accumulated during cooldown still count; one additional deadline is enough.
                await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                    RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));

                rpcClient.Verify(c => c.ResetTransport(), Times.Exactly(2));
                Assert.AreEqual(2, client.ReconnectTelemetryCalls);
            }
            finally
            {
                await clientManager.Shutdown();
            }
        }

        [TestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task TestLateHeartbeatsCannotRecreateRetiredState(bool shutdown)
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            rpcClient.Setup(c => c.State).Returns(grpcLib.ConnectivityState.Ready);
            var heartbeats = Enumerable.Range(0, 2).Select(_ =>
                new TaskCompletionSource<HeartbeatResponse>(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
            var monitors = heartbeats.Select(source =>
                clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object, source.Task)).ToArray();
            try
            {
                Assert.AreEqual(1, RecoveryStateCount(clientManager));
                if (shutdown)
                {
                    await clientManager.Shutdown();
                }
                else
                {
                    client.EndpointsDeprecated = true;
                    clientManager.PruneTransportRecoveryStates();
                }

                Assert.AreEqual(0, RecoveryStateCount(clientManager));
                foreach (var heartbeat in heartbeats)
                {
                    heartbeat.SetException(new grpcLib.RpcException(
                        new grpcLib.Status(grpcLib.StatusCode.DeadlineExceeded, "late heartbeat")));
                }

                await Task.WhenAll(monitors).WaitAsync(TimeSpan.FromSeconds(5));
                // Neither a new monitor nor a server command may reopen recovery after shutdown/deprecation.
                await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                    RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.Unavailable));
                clientManager.Reconnect(FakeEndpoints, rpcClient.Object);

                Assert.AreEqual(0, RecoveryStateCount(clientManager));
                rpcClient.Verify(c => c.ResetTransport(), Times.Never);
                Assert.AreEqual(0, client.ReconnectTelemetryCalls);
            }
            finally
            {
                foreach (var heartbeat in heartbeats)
                {
                    heartbeat.TrySetResult(new HeartbeatResponse());
                }

                await Task.WhenAll(monitors).WaitAsync(TimeSpan.FromSeconds(5));
                await clientManager.Shutdown();
            }
        }

        [TestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task TestReaddedEndpointHasFreshCountersAndCooldownAndIgnoresOldCallbacks(bool recoverFirst)
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            var heartbeats = Enumerable.Range(0, 3).Select(_ =>
                new TaskCompletionSource<HeartbeatResponse>(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
            var monitors = new List<Task>();
            try
            {
                var initialFailures = recoverFirst ? ClientManager.HeartbeatFailureThreshold : 1;
                for (var i = 0; i < initialFailures; i++)
                {
                    await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                        RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
                }

                var initialResets = recoverFirst ? 1 : 0;
                var retiredState = RecoveryState(clientManager);
                monitors.AddRange(heartbeats.Select(source =>
                    clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object, source.Task)));
                client.EndpointsDeprecated = true;
                clientManager.PruneTransportRecoveryStates();
                Assert.AreEqual(0, RecoveryStateCount(clientManager));
                client.EndpointsDeprecated = false;

                await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                    RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
                var activeState = RecoveryState(clientManager);
                Assert.AreNotSame(retiredState, activeState);
                rpcClient.Verify(c => c.ResetTransport(), Times.Exactly(initialResets));

                heartbeats[0].SetException(new grpcLib.RpcException(
                    new grpcLib.Status(grpcLib.StatusCode.DeadlineExceeded, "retired first")));
                heartbeats[1].SetException(new grpcLib.RpcException(
                    new grpcLib.Status(grpcLib.StatusCode.DeadlineExceeded, "retired second")));
                heartbeats[2].SetResult(new HeartbeatResponse());
                await Task.WhenAll(monitors).WaitAsync(TimeSpan.FromSeconds(5));
                Assert.AreSame(activeState, RecoveryState(clientManager));
                rpcClient.Verify(c => c.ResetTransport(), Times.Exactly(initialResets));

                // The old success must not erase the new failure; the old cooldown must not suppress recovery.
                await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                    RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
                rpcClient.Verify(c => c.ResetTransport(), Times.Exactly(initialResets + 1));
                Assert.AreEqual(initialResets + 1, client.ReconnectTelemetryCalls);
            }
            finally
            {
                foreach (var heartbeat in heartbeats)
                {
                    heartbeat.TrySetResult(new HeartbeatResponse());
                }

                await Task.WhenAll(monitors).WaitAsync(TimeSpan.FromSeconds(5));
                await clientManager.Shutdown();
            }
        }

        [TestMethod]
        public async Task TestHeartbeatCapturesRecoveryStateBeforeSending()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            rpcClient.Setup(c => c.Shutdown()).Returns(Task.CompletedTask);
            CacheRpcClient(clientManager, rpcClient.Object);
            rpcClient.Setup(c => c.Heartbeat(It.IsAny<grpcLib.Metadata>(), It.IsAny<HeartbeatRequest>(),
                It.IsAny<TimeSpan>())).Returns(() =>
            {
                client.EndpointsDeprecated = true;
                clientManager.PruneTransportRecoveryStates();
                client.EndpointsDeprecated = false;
                return RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded);
            });
            try
            {
                await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                    RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
                await Assert.ThrowsExactlyAsync<grpcLib.RpcException>(() =>
                    clientManager.Heartbeat(FakeEndpoints, new HeartbeatRequest(), TimeSpan.FromSeconds(1)));

                Assert.AreEqual(0, RecoveryStateCount(clientManager),
                    "A heartbeat sent before retirement must not register against the re-added endpoint");
                rpcClient.Verify(c => c.ResetTransport(), Times.Never);
            }
            finally
            {
                await clientManager.Shutdown();
            }
        }

        [TestMethod]
        public async Task TestHeartbeatCallerDoesNotWaitForTransportRecovery()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            rpcClient.Setup(c => c.Shutdown()).Returns(Task.CompletedTask);
            CacheRpcClient(clientManager, rpcClient.Object);
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            rpcClient.Setup(c => c.ResetTransport()).Callback(() =>
            {
                started.TrySetResult(true);
                release.Task.GetAwaiter().GetResult();
            });
            rpcClient.Setup(c => c.Heartbeat(It.IsAny<grpcLib.Metadata>(), It.IsAny<HeartbeatRequest>(),
                It.IsAny<TimeSpan>())).Returns(() =>
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
            try
            {
                for (var i = 0; i < ClientManager.HeartbeatFailureThreshold; i++)
                {
                    await Assert.ThrowsExactlyAsync<grpcLib.RpcException>(() =>
                        clientManager.Heartbeat(FakeEndpoints, new HeartbeatRequest(), TimeSpan.FromSeconds(1))
                            .WaitAsync(TimeSpan.FromSeconds(5)));
                }

                await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.AreEqual(0, client.ReconnectTelemetryCalls);
                release.SetResult(true);
                await TestAwaiter.Until(() => !RecoveryInProgress(clientManager), "the background heartbeat recovery");
                Assert.AreEqual(1, client.ReconnectTelemetryCalls);
            }
            finally
            {
                release.TrySetResult(true);
                await TestAwaiter.Until(() => !RecoveryInProgress(clientManager), "the background recovery to exit");
                await clientManager.Shutdown();
            }
        }

        [TestMethod]
        public async Task TestRetiredBusyResetCannotOverlapReaddedEndpointOrRenewOldTelemetry()
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            rpcClient.Setup(c => c.State).Returns(grpcLib.ConnectivityState.Ready);
            rpcClient.Setup(c => c.Shutdown()).Returns(Task.CompletedTask);
            CacheRpcClient(clientManager, rpcClient.Object);
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var resetCalls = 0;
            rpcClient.Setup(c => c.ResetTransport()).Callback(() =>
            {
                if (Interlocked.Increment(ref resetCalls) == 1)
                {
                    started.TrySetResult(true);
                    release.Task.GetAwaiter().GetResult();
                }
            });
            var oldRecovery = clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.Unavailable));
            try
            {
                await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
                client.EndpointsDeprecated = true;
                clientManager.PruneTransportRecoveryStates();
                Assert.AreEqual(0, RecoveryStateCount(clientManager));
                client.EndpointsDeprecated = false;

                for (var i = 0; i < ClientManager.HeartbeatFailureThreshold; i++)
                {
                    await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                        RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
                }

                clientManager.Reconnect(FakeEndpoints);
                Assert.AreEqual(1, Volatile.Read(ref resetCalls), "Retirement must not release the old reset's guard");
                Assert.AreEqual(0, client.ReconnectTelemetryCalls);

                release.SetResult(true);
                await oldRecovery.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.AreEqual(0, client.ReconnectTelemetryCalls, "The retired reset must not renew the new session");

                await clientManager.MonitorHeartbeat(FakeEndpoints, rpcClient.Object,
                    RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
                Assert.AreEqual(2, Volatile.Read(ref resetCalls));
                Assert.AreEqual(1, client.ReconnectTelemetryCalls);
            }
            finally
            {
                release.TrySetResult(true);
                await oldRecovery.WaitAsync(TimeSpan.FromSeconds(5));
                await clientManager.Shutdown();
            }
        }

        [TestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task TestTransportRecoveryDoesNotRenewTelemetryAfterStop(bool shutdown)
        {
            var client = CreateRecoveryTestClient();
            var clientManager = new ClientManager(client);
            var rpcClient = new Mock<IRpcClient>();
            rpcClient.Setup(c => c.ResetTransport()).Callback(() =>
            {
                if (shutdown)
                {
                    clientManager.Shutdown().GetAwaiter().GetResult();
                }
                else
                {
                    client.State = Org.Apache.Rocketmq.State.Stopping;
                }
            });
            try
            {
                clientManager.Reconnect(FakeEndpoints, rpcClient.Object);
                rpcClient.Verify(c => c.ResetTransport(), Times.Once);
                Assert.AreEqual(0, client.ReconnectTelemetryCalls);
            }
            finally
            {
                await clientManager.Shutdown();
            }
        }

        [TestMethod]
        public async Task TestShutdownInvokesAndAwaitsRpcShutdownOutsideClientLock()
        {
            var clientLock = PrivateField<ReaderWriterLockSlim>(_clientManager, "_clientLock");
            var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var heldDuringShutdown = false;
            _rpcClient.Setup(c => c.Shutdown()).Returns(() =>
            {
                heldDuringShutdown = clientLock.IsReadLockHeld;
                return completion.Task;
            });
            var shutdown = _clientManager.Shutdown();
            try
            {
                Assert.IsFalse(shutdown.IsCompleted);
                Assert.IsFalse(heldDuringShutdown, "RPC shutdown must be invoked outside the cache lock");
                Assert.AreEqual(0, clientLock.CurrentReadCount, "The cache lock must not be held across await");
            }
            finally
            {
                completion.TrySetResult(true);
                await shutdown.WaitAsync(TimeSpan.FromSeconds(5));
            }
        }

        private static T PrivateField<T>(object instance, string name)
        {
            var field = instance.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(field, $"Missing field {name}");
            return (T)field.GetValue(instance);
        }

        private static void CacheRpcClient(ClientManager clientManager, IRpcClient rpcClient)
        {
            var clientLock = PrivateField<ReaderWriterLockSlim>(clientManager, "_clientLock");
            clientLock.EnterWriteLock();
            try
            {
                PrivateField<Dictionary<Endpoints, IRpcClient>>(clientManager, "_rpcClients")[FakeEndpoints] = rpcClient;
            }
            finally
            {
                clientLock.ExitWriteLock();
            }
        }

        private static int RecoveryStateCount(ClientManager clientManager)
        {
            lock (PrivateField<object>(clientManager, "_transportRecoveryLock"))
            {
                return PrivateField<IDictionary>(clientManager, "_transportRecoveryStates").Count;
            }
        }

        private static object RecoveryState(ClientManager clientManager)
        {
            lock (PrivateField<object>(clientManager, "_transportRecoveryLock"))
            {
                return PrivateField<IDictionary>(clientManager, "_transportRecoveryStates")[FakeEndpoints];
            }
        }

        private static bool RecoveryInProgress(ClientManager clientManager)
        {
            lock (PrivateField<object>(clientManager, "_transportRecoveryLock"))
            {
                return PrivateField<HashSet<Endpoints>>(clientManager, "_recoveringEndpoints").Contains(FakeEndpoints);
            }
        }

        private static void ExpireRecoveryCooldown(ClientManager clientManager)
        {
            lock (PrivateField<object>(clientManager, "_transportRecoveryLock"))
            {
                var state = RecoveryState(clientManager);
                Assert.IsNotNull(state);
                var timestamp = state.GetType().GetField("LastRecoveryTimestamp",
                    BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.IsNotNull(timestamp);
                var elapsed = ClientManager.TransportRecoveryCooldown + TimeSpan.FromSeconds(1);
                timestamp.SetValue(state, Stopwatch.GetTimestamp() - (long)(elapsed.TotalSeconds * Stopwatch.Frequency));
            }
        }

        private Client CreateTestClient()
        {
            return new Producer(_clientConfig, new ConcurrentDictionary<string, bool>(), 1, null);
        }

        private RecoveryTestClient CreateRecoveryTestClient()
        {
            return new RecoveryTestClient(_clientConfig);
        }
    }
}
