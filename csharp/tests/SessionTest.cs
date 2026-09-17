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
using System.Reflection;
using System.Threading;
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
    public class SessionTests
    {
        [TestMethod]
        public async Task TestSyncSettings()
        {
            await using var client = new Producer(RecoveryTestSupport.CreateClientConfig(), new ConcurrentDictionary<string, bool>(), 1, null);
            using var stream = new FakeTelemetryCall(true, new Proto.TelemetryCommand { Settings = client.GetSettings().ToProtobuf() });
            var session = new Session(RecoveryTestSupport.FakeEndpoints, stream.Call, client);
            try
            {
                await session.SyncSettings(true);
                Assert.AreEqual(1, stream.WrittenCommands);
                Assert.IsNotNull(stream.WrittenCommand(0).Settings);
            }
            finally
            {
                await session.CloseAsync();
            }
        }

        [TestMethod]
        public async Task TestReconnectRenewsTelemetryAndSyncsSettings()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            using var first = new FakeTelemetryCall();
            using var renewed = new FakeTelemetryCall();
            var manager = new Mock<IClientManager>();
            manager.Setup(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(renewed.Call);
            client.SetClientManager(manager.Object);
            var session = new Session(RecoveryTestSupport.FakeEndpoints, first.Call, client);
            try
            {
                session.Reconnect();
                await TestAwaiter.Until(() => renewed.WrittenCommands == 1, "settings on renewed telemetry");
                Assert.IsNotNull(renewed.WrittenCommand(0).Settings);
                Assert.AreEqual(1, first.DisposeCalls);
                Assert.AreEqual(0, renewed.DisposeCalls);
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Once);
            }
            finally
            {
                await session.CloseAsync();
            }

            Assert.AreEqual(1, renewed.DisposeCalls);
        }

        [TestMethod]
        [Timeout(20000)]
        public async Task TestConcurrentReconnectOnlyRenewsTelemetryOnce()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            using var first = new FakeTelemetryCall();
            using var renewed = new FakeTelemetryCall();
            const int threadCount = 8;
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var manager = new Mock<IClientManager>();
            manager.Setup(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(() =>
            {
                started.TrySetResult(true);
                release.Task.GetAwaiter().GetResult();
                return renewed.Call;
            });
            client.SetClientManager(manager.Object);
            var session = new Session(RecoveryTestSupport.FakeEndpoints, first.Call, client);
            using var barrier = new Barrier(threadCount + 1);
            var reconnects = Enumerable.Range(0, threadCount).Select(_ => Task.Run(() =>
            {
                Assert.IsTrue(barrier.SignalAndWait(TimeSpan.FromSeconds(10)));
                session.Reconnect();
            })).ToArray();
            try
            {
                Assert.IsTrue(barrier.SignalAndWait(TimeSpan.FromSeconds(10)));
                await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
                await TestAwaiter.Until(() => reconnects.Count(task => task.IsCompleted) == threadCount - 1,
                    "all losing reconnect attempts to exit while the winner is blocked");
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Once);
                release.SetResult(true);
                await Task.WhenAll(reconnects);
                await TestAwaiter.Until(() => renewed.WrittenCommands == 1, "the winning renewal to synchronize settings");
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Once);
            }
            finally
            {
                release.TrySetResult(true);
                await Task.WhenAll(reconnects);
                await session.CloseAsync();
            }
        }

        [TestMethod]
        public async Task TestReconnectIsForgivenWhileClientIsNotRunningAndRetriedAfterwards()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            using var first = new FakeTelemetryCall();
            using var renewed = new FakeTelemetryCall();
            var manager = new Mock<IClientManager>();
            manager.Setup(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(renewed.Call);
            client.SetClientManager(manager.Object);
            var session = new Session(RecoveryTestSupport.FakeEndpoints, first.Call, client);
            try
            {
                client.State = State.Stopping;
                session.Reconnect();
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Never);
                client.State = State.Running;
                session.Reconnect();
                await TestAwaiter.Until(() => renewed.WrittenCommands == 1, "renewal after the client resumes");
                Assert.AreEqual(1, first.DisposeCalls);
            }
            finally
            {
                await session.CloseAsync();
            }
        }

        [TestMethod]
        public async Task TestReconnectRemovesSessionWhenEndpointsAreDeprecated()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            client.EndpointsDeprecated = true;
            using var stream = new FakeTelemetryCall();
            var manager = new Mock<IClientManager>();
            client.SetClientManager(manager.Object);
            var session = new Session(RecoveryTestSupport.FakeEndpoints, stream.Call, client);
            try
            {
                session.Reconnect();
                await TestAwaiter.Until(() => client.RemoveSessionCalls == 1 && stream.DisposeCalls == 1,
                    "the deprecated session to be removed and closed");
                client.EndpointsDeprecated = false;
                session.Reconnect();
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Never);
            }
            finally
            {
                await session.CloseAsync();
            }
        }

        [TestMethod]
        public async Task TestReconnectEndpointsCommandIsDispatched()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            using var stream = new FakeTelemetryCall(true, new Proto.TelemetryCommand
            {
                ReconnectEndpointsCommand = new Proto.ReconnectEndpointsCommand { Nonce = "nonce" }
            });
            var manager = new Mock<IClientManager>();
            client.SetClientManager(manager.Object);
            var session = new Session(RecoveryTestSupport.FakeEndpoints, stream.Call, client);
            try
            {
                var received = await client.FirstReconnectCommand.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.AreEqual("nonce", received.Nonce);
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Never);
            }
            finally
            {
                await session.CloseAsync();
            }
        }

        [TestMethod]
        public async Task TestWriteAsyncIsDroppedOnceTheSessionIsClosed()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            using var stream = new FakeTelemetryCall();
            var session = new Session(RecoveryTestSupport.FakeEndpoints, stream.Call, client);
            await session.CloseAsync();
            await session.WriteAsync(new Proto.TelemetryCommand());
            Assert.AreEqual(0, stream.WrittenCommands);
            Assert.AreEqual(1, stream.DisposeCalls);
        }

        [TestMethod]
        [Timeout(15000)]
        public async Task TestCloseIsNotHeldUpByAStuckWrite()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            using var stream = new FakeTelemetryCall();
            stream.Writer.WriteGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var session = new Session(RecoveryTestSupport.FakeEndpoints, stream.Call, client);
            var write = session.WriteAsync(new Proto.TelemetryCommand());
            await TestAwaiter.Until(() => stream.WrittenCommands == 1, "the blocked write");
            await session.CloseAsync().WaitAsync(Session.CloseTimeout.Add(TimeSpan.FromSeconds(5)));
            var error = await Assert.ThrowsExactlyAsync<RpcException>(() => write);
            Assert.AreEqual(StatusCode.Cancelled, error.StatusCode);
            Assert.AreEqual(1, stream.DisposeCalls);
        }

        [TestMethod]
        public async Task TestRenewalDiscardsTheStreamOfASessionClosedWhileItWasCreated()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            using var first = new FakeTelemetryCall();
            using var renewed = new FakeTelemetryCall();
            Session session = null;
            Task close = null;
            var manager = new Mock<IClientManager>();
            manager.Setup(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(() =>
            {
                close = session.CloseAsync();
                return renewed.Call;
            });
            client.SetClientManager(manager.Object);
            session = new Session(RecoveryTestSupport.FakeEndpoints, first.Call, client);
            try
            {
                session.Reconnect();
                await (close ?? Task.CompletedTask);
                await TestAwaiter.Until(() => renewed.DisposeCalls == 1, "the unused replacement to be disposed");
                Assert.AreEqual(0, renewed.WrittenCommands);
            }
            finally
            {
                await session.CloseAsync();
            }
        }

        [TestMethod]
        public async Task TestReadLoopTerminationRenewsActiveSession()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            using var first = new FakeTelemetryCall();
            using var renewed = new FakeTelemetryCall();
            var manager = new Mock<IClientManager>();
            manager.Setup(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(renewed.Call);
            client.SetClientManager(manager.Object);
            var session = new Session(RecoveryTestSupport.FakeEndpoints, first.Call, client);
            try
            {
                first.Reader.Close();
                await TestAwaiter.Until(() => renewed.WrittenCommands == 1, "EOF to renew telemetry and synchronize settings");
                Assert.AreEqual(1, first.DisposeCalls);
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Once);
            }
            finally
            {
                await session.CloseAsync();
            }
        }

        [TestMethod]
        [Timeout(20000)]
        public async Task TestReplacementEofIsNotLostWhileSettingsSynchronizationIsPending()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            using var first = new FakeTelemetryCall();
            using var ended = new FakeTelemetryCall(false);
            ended.Writer.WriteGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var healthy = new FakeTelemetryCall(true, new Proto.TelemetryCommand { Settings = client.GetSettings().ToProtobuf() });
            var manager = new Mock<IClientManager>();
            manager.SetupSequence(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(ended.Call).Returns(healthy.Call);
            client.SetClientManager(manager.Object);
            var session = new Session(RecoveryTestSupport.FakeEndpoints, first.Call, client);
            var initialSync = Task.Run(() => session.SyncSettings(true));
            try
            {
                await TestAwaiter.Until(() => first.WrittenCommands == 1, "initial settings to await their response");
                session.Reconnect();
                await TestAwaiter.Until(() => healthy.WrittenCommands == 1,
                    "an immediately terminated replacement to renew again", TimeSpan.FromSeconds(10));
                await initialSync;
                Assert.AreEqual(1, ended.DisposeCalls);
                Assert.IsNotNull(healthy.WrittenCommand(0).Settings);
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Exactly(2));
            }
            finally
            {
                await session.CloseAsync();
                await initialSync;
            }
        }

        [TestMethod]
        [Timeout(15000)]
        public async Task TestCommandFailureCancelsOldCallBeforeWaitingForWriter()
        {
            await using var client = new ThrowingSettingsClient();
            using var first = new FakeTelemetryCall(true, new Proto.TelemetryCommand { Settings = client.GetSettings().ToProtobuf() });
            first.Writer.WriteGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var renewed = new FakeTelemetryCall();
            var manager = new Mock<IClientManager>();
            manager.Setup(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(renewed.Call);
            client.SetClientManager(manager.Object);
            var session = new Session(RecoveryTestSupport.FakeEndpoints, first.Call, client);
            var write = session.WriteAsync(new Proto.TelemetryCommand());
            try
            {
                await TestAwaiter.Until(() => first.WrittenCommands == 1, "the old writer to block");
                client.FailSettings.TrySetResult(true);
                await TestAwaiter.Until(() => renewed.WrittenCommands == 1, "command failure to renew despite the blocked writer");
                var error = await Assert.ThrowsExactlyAsync<RpcException>(() => write);
                Assert.AreEqual(StatusCode.Cancelled, error.StatusCode);
                Assert.AreEqual(1, first.DisposeCalls);
            }
            finally
            {
                client.FailSettings.TrySetResult(true);
                await session.CloseAsync();
                await Assert.ThrowsExactlyAsync<RpcException>(() => write);
            }
        }

        [TestMethod]
        [Timeout(15000)]
        public async Task TestCurrentStreamEofRetriesAfterLosingRenewalGuard()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            using var first = new FakeTelemetryCall();
            using var renewed = new FakeTelemetryCall();
            var manager = new Mock<IClientManager>();
            manager.Setup(m => m.Telemetry(It.IsAny<Endpoints>())).Returns(renewed.Call);
            client.SetClientManager(manager.Object);
            var session = new Session(RecoveryTestSupport.FakeEndpoints, first.Call, client);
            var guard = typeof(Session).GetField("_reconnecting", BindingFlags.Instance | BindingFlags.NonPublic);
            var onTermination = typeof(Session).GetMethod("RenewAfterTerminationAsync",
                BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(guard);
            Assert.IsNotNull(onTermination);
            using var context = new PausedRenewalContext();
            Task renewal = null;
            try
            {
                guard.SetValue(session, 1);
                var previousContext = SynchronizationContext.Current;
                try
                {
                    SynchronizationContext.SetSynchronizationContext(context);
                    renewal = (Task)onTermination.Invoke(session, new object[] { first.Call });
                }
                finally
                {
                    SynchronizationContext.SetSynchronizationContext(previousContext);
                }

                // Execute the EOF continuation while a stale callback still holds the renewal guard.
                await context.RunNextAsync();
                Assert.IsFalse(renewal.IsCompleted, "a current stream's EOF must not be lost when admission is busy");
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Never);

                guard.SetValue(session, 0);
                await context.RunNextAsync();
                await renewal.WaitAsync(TimeSpan.FromSeconds(5));
                await TestAwaiter.Until(() => renewed.WrittenCommands == 1, "the retained EOF to renew and sync settings");
                Assert.AreEqual(1, first.DisposeCalls);
                manager.Verify(m => m.Telemetry(It.IsAny<Endpoints>()), Times.Once);
            }
            finally
            {
                guard.SetValue(session, 0);
                await session.CloseAsync();
                if (renewal != null && !renewal.IsCompleted)
                {
                    await context.RunNextAsync();
                    await renewal.WaitAsync(TimeSpan.FromSeconds(5));
                }
            }
        }

        private sealed class PausedRenewalContext : SynchronizationContext, IDisposable
        {
            private readonly ConcurrentQueue<Action> _continuations = new ConcurrentQueue<Action>();
            private readonly SemaphoreSlim _posted = new SemaphoreSlim(0);

            public override void Post(SendOrPostCallback callback, object state)
            {
                _continuations.Enqueue(() => callback(state));
                _posted.Release();
            }

            internal async Task RunNextAsync()
            {
                Assert.IsTrue(await _posted.WaitAsync(TimeSpan.FromSeconds(5)), "the renewal backoff must complete");
                Assert.IsTrue(_continuations.TryDequeue(out var continuation));
                var previousContext = Current;
                try
                {
                    SetSynchronizationContext(this);
                    continuation();
                }
                finally
                {
                    SetSynchronizationContext(previousContext);
                }
            }

            public void Dispose()
            {
                _posted.Dispose();
            }
        }

        private sealed class ThrowingSettingsClient : RecoveryTestClient
        {
            internal readonly TaskCompletionSource<bool> FailSettings = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            internal ThrowingSettingsClient() : base(RecoveryTestSupport.CreateClientConfig())
            {
            }

            internal override void OnSettingsCommand(Endpoints endpoints, Proto.Settings settings)
            {
                FailSettings.Task.GetAwaiter().GetResult();
                throw new InvalidOperationException("invalid settings");
            }
        }
    }
}
