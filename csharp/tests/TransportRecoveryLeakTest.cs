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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Endpoints = Org.Apache.Rocketmq.Endpoints;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    /// <summary>
    /// Transport recovery replaces the channel and its handler on every cycle, and the replaced ones own real sockets.
    /// A recovery which ran forever must not accumulate them, so the descriptors held by the process are counted across
    /// a long run of real recoveries against a live server.
    /// </summary>
    [TestClass]
    public class TransportRecoveryLeakTest : GrpcServerIntegrationTest
    {
        private const int RecoveryCycles = 200;

        /// <summary>
        /// Disposing a handler closes its sockets asynchronously, so a handful of descriptors may still be in flight
        /// when the count is taken. Anything beyond that means the replaced transports are being retained.
        /// </summary>
        private const int DescriptorTolerance = 12;

        private Server _server;

        [TestInitialize]
        public void SetUp()
        {
            _server = SetUpServer(new MockServer("topic", "broker", new List<string>()));
        }

        [TestCleanup]
        public async Task TearDown()
        {
            await _server.ShutdownAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }

        [TestMethod]
        [Timeout(180000)]
        public async Task TestRepeatedTransportRecoveryDoesNotLeakDescriptors()
        {
            var rpcClient = new RpcClient(new Endpoints($"127.0.0.1:{Port}"), false);
            var metadata = new Metadata();
            var request = new Proto.HeartbeatRequest();
            var timeout = TimeSpan.FromSeconds(5);

            // Called for real so that the first channel owns a connection before the baseline is taken, otherwise the
            // comparison would measure a client which never connected at all.
            await rpcClient.Heartbeat(metadata, request, timeout);
            Assert.AreEqual(ConnectivityState.Ready, rpcClient.State,
                "the channel of a client which just completed a call must be ready");

            var descriptorsBefore = CountOpenDescriptors();
            var establishedBefore = CountEstablishedConnectionsToTheServer();

            for (var i = 0; i < RecoveryCycles; i++)
            {
                rpcClient.ResetTransport();
                // Called again so that every cycle really does open a connection on the replacement channel.
                await rpcClient.Heartbeat(metadata, request, timeout);
            }

            RecoveryTestSupport.ForceFinalization();

            var descriptorsAfter = CountOpenDescriptors();
            var establishedAfter = CountEstablishedConnectionsToTheServer();
            var descriptorGrowth = descriptorsAfter - descriptorsBefore;
            var establishedGrowth = establishedAfter - establishedBefore;

            Assert.IsTrue(descriptorGrowth <= DescriptorTolerance,
                $"{RecoveryCycles} transport recoveries leaked descriptors: {descriptorsBefore} -> " +
                $"{descriptorsAfter} (growth {descriptorGrowth}, tolerance {DescriptorTolerance})");
            Assert.IsTrue(establishedGrowth <= DescriptorTolerance,
                $"{RecoveryCycles} transport recoveries left connections behind: {establishedBefore} -> " +
                $"{establishedAfter} (growth {establishedGrowth})");
            Assert.AreEqual(ConnectivityState.Ready, rpcClient.State,
                "the channel of the last replacement must still be usable");
        }

        /// <summary>
        /// The invariant the whole recovery rests on: replacing the transport must make the streams of the replaced
        /// channel unusable at once, because that is what releases a writer which is stuck on a half-open connection
        /// and holding the lock the telemetry renewal needs.
        /// </summary>
        [TestMethod]
        [Timeout(60000)]
        public async Task TestResetTransportMakesTheStreamsOfTheReplacedChannelUnusable()
        {
            var rpcClient = new RpcClient(new Endpoints($"127.0.0.1:{Port}"), false);
            var call = rpcClient.Telemetry(new Metadata());

            // Completed for real so that the channel owns a connection, which is the state a half-open one is in.
            await rpcClient.Heartbeat(new Metadata(), new Proto.HeartbeatRequest(), TimeSpan.FromSeconds(5));
            Assert.AreEqual(ConnectivityState.Ready, rpcClient.State,
                "the channel of a client which just completed a call must be ready");

            await call.RequestStream.WriteAsync(new Proto.TelemetryCommand());

            rpcClient.ResetTransport();
            Assert.AreEqual(ConnectivityState.Idle, rpcClient.State,
                "a replacement channel which has not been used yet must start out idle");

            // Bounded: a write which hangs here instead of failing is exactly the defect this guards against.
            Exception failure = null;
            var succeeded = false;
            try
            {
                await call.RequestStream.WriteAsync(new Proto.TelemetryCommand())
                    .WaitAsync(TimeSpan.FromSeconds(10));
                succeeded = true;
            }
            catch (Exception e)
            {
                failure = e;
            }

            Assert.IsFalse(succeeded, "a write on the stream of a replaced channel must not succeed");
            Assert.IsNotNull(failure, "a write on the stream of a replaced channel must not hang");
            Assert.IsNotInstanceOfType(failure, typeof(TimeoutException),
                "a write on the stream of a replaced channel hung for the whole bound instead of failing");
            Assert.IsInstanceOfType(failure, typeof(ObjectDisposedException),
                "the replaced channel must be disposed, got " + failure.GetType().Name + ": " + failure.Message);
        }

        /// <summary>
        /// Descriptors held by this process. <see cref="Process.HandleCount"/> reports zero on Unix, where the
        /// descriptor table of the calling process is exposed under /dev/fd instead.
        /// </summary>
        private static int CountOpenDescriptors()
        {
            return OperatingSystem.IsWindows()
                ? Process.GetCurrentProcess().HandleCount
                : Directory.GetFiles("/dev/fd").Length;
        }

        /// <summary>
        /// Connections this client holds towards the mock server. Filtering on the server port keeps the connections of
        /// every other process on the machine, and the server side of these very connections, out of the count.
        /// </summary>
        private int CountEstablishedConnectionsToTheServer()
        {
            return IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpConnections()
                .Count(connection => TcpState.Established == connection.State
                                     && Port == connection.RemoteEndPoint.Port
                                     && IPAddress.Loopback.Equals(connection.RemoteEndPoint.Address));
        }
    }
}
