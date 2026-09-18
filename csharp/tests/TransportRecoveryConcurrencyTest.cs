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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Endpoints = Org.Apache.Rocketmq.Endpoints;
using Proto = Apache.Rocketmq.V2;
using grpcLib = Grpc.Core;

namespace tests
{
    /// <summary>
    /// Exercises recovery under repeated contention and telemetry creation failures without changing process-wide
    /// thread pool settings.
    /// </summary>
    [TestClass]
    public class TransportRecoveryConcurrencyTest
    {
        private const int Rounds = 50;
        private const int Threads = 16;

        [TestMethod]
        [Timeout(120000)]
        public async Task TestRepeatedConcurrentReconnectNeverResetsTheTransportTwiceAtTheSameTime()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            var clientManager = new ClientManager(client);
            client.SetClientManager(clientManager);
            var rpcClient = new Mock<IRpcClient>();
            var overlap = new OverlapRecorder();
            var resetCalls = 0;
            rpcClient.Setup(c => c.ResetTransport()).Callback(() =>
            {
                overlap.Enter();
                // Widened on purpose: without a window to overlap in, a broken guard would still look correct.
                Thread.SpinWait(4000);
                overlap.Leave();
                Interlocked.Increment(ref resetCalls);
            });

            for (var round = 0; round < Rounds; round++)
            {
                using var barrier = new Barrier(Threads);
                await Task.WhenAll(Enumerable.Range(0, Threads).Select(_ => Task.Run(() =>
                {
                    barrier.SignalAndWait();
                    clientManager.Reconnect(RecoveryTestSupport.FakeEndpoints, rpcClient.Object);
                })));
            }

            // The guard makes the recoveries mutually exclusive rather than singular: a server command which arrives
            // once the previous recovery completed is entitled to reset again, and only a reset which overlaps
            // another one is a defect.
            Assert.AreEqual(1, overlap.Widest,
                "transport resets ran at the same time, the widest overlap reached " + overlap.Widest);
            Assert.IsTrue(Volatile.Read(ref resetCalls) >= Rounds,
                "every round must have been able to recover again, only " + resetCalls + " resets happened");
            Assert.AreEqual(client.ReconnectTelemetryCalls, Volatile.Read(ref resetCalls),
                "every transport reset must be followed by exactly one telemetry rebuild");
        }

        [TestMethod]
        [Timeout(120000)]
        public async Task TestConcurrentHeartbeatFailuresAndServerCommandsRecoverWithoutDeadlock()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            var clientManager = new ClientManager(client);
            client.SetClientManager(clientManager);
            var rpcClient = new Mock<IRpcClient>();
            rpcClient.Setup(c => c.State).Returns(grpcLib.ConnectivityState.Ready);
            var resetCalls = 0;
            rpcClient.Setup(c => c.ResetTransport()).Callback(() => Interlocked.Increment(ref resetCalls));

            // Every source at once: heartbeat timeouts, heartbeat refusals, the server command and plain successes.
            var workload = Enumerable.Range(0, 64).Select(index => Task.Run(async () =>
            {
                switch (index % 4)
                {
                    case 0:
                        await clientManager.MonitorHeartbeat(RecoveryTestSupport.FakeEndpoints, rpcClient.Object,
                            RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.DeadlineExceeded));
                        break;
                    case 1:
                        await clientManager.MonitorHeartbeat(RecoveryTestSupport.FakeEndpoints, rpcClient.Object,
                            RecoveryTestSupport.FailedHeartbeat(grpcLib.StatusCode.Unavailable));
                        break;
                    case 2:
                        clientManager.Reconnect(RecoveryTestSupport.FakeEndpoints, rpcClient.Object);
                        break;
                    default:
                        await clientManager.MonitorHeartbeat(RecoveryTestSupport.FakeEndpoints, rpcClient.Object,
                            Task.FromResult(new Proto.HeartbeatResponse()));
                        break;
                }
            })).ToArray();

            await Task.WhenAll(workload);

            Assert.IsTrue(Volatile.Read(ref resetCalls) > 0,
                "the contending triggers must recover the transport");
            Assert.AreEqual(Volatile.Read(ref resetCalls), client.ReconnectTelemetryCalls,
                "every transport reset must be followed by exactly one telemetry rebuild");
        }

        [TestMethod]
        [Timeout(30000)]
        public async Task TestTelemetryRenewalRetriesCreationFailuresAndWritesSettings()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            var endpoints = new Endpoints(client.GetClientConfig().Endpoints);
            using var first = new FakeTelemetryCall();
            using var renewed = new FakeTelemetryCall();
            var attempts = 0;
            var clientManager = new Mock<IClientManager>();
            clientManager.Setup(m => m.Telemetry(endpoints)).Returns(() =>
            {
                if (Interlocked.Increment(ref attempts) <= 2)
                {
                    throw new grpcLib.RpcException(
                        new grpcLib.Status(grpcLib.StatusCode.Unavailable, "no stream"));
                }

                return renewed.Call;
            });
            client.SetClientManager(clientManager.Object);

            var session = new Session(endpoints, first.Call, client);
            try
            {
                session.Reconnect();
                await TestAwaiter.Until(() => renewed.WrittenCommands > 0 && 1 == first.DisposeCalls,
                    "settings to be written after two telemetry creation failures", TimeSpan.FromSeconds(10));

                Assert.AreEqual(3, Volatile.Read(ref attempts));
                Assert.IsNotNull(renewed.WrittenCommand(0).Settings);
                Assert.AreEqual(0, renewed.DisposeCalls, "the replacement stream must remain usable");
            }
            finally
            {
                await session.CloseAsync();
            }
        }

        /// <summary>
        /// Widest number of recoveries which were inside the guarded section at the same moment.
        /// </summary>
        private sealed class OverlapRecorder
        {
            private int _inProgress;
            private int _widest;

            internal int Widest => Volatile.Read(ref _widest);

            internal void Enter()
            {
                var depth = Interlocked.Increment(ref _inProgress);
                int widest;
                do
                {
                    widest = Volatile.Read(ref _widest);
                }
                while (depth > widest && depth != Interlocked.CompareExchange(ref _widest, depth, widest));
            }

            internal void Leave()
            {
                Interlocked.Decrement(ref _inProgress);
            }
        }
    }
}
