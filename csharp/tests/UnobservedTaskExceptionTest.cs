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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
    /// Checks recovery's background exception boundaries in a dedicated testhost. Other tests must neither contribute
    /// faults to this check nor have their faults cleared by it; caller-owned monitor and write tasks are awaited.
    /// </summary>
    [TestClass]
    public class UnobservedTaskExceptionTest
    {
        private const string ChildEnvironmentVariable = "ROCKETMQ_RECOVERY_UNOBSERVED_TEST_CHILD";
        private const string ChildSuccessMarker = "RocketMQ isolated recovery exception check passed";

        [TestMethod]
        [Timeout(90000)]
        public async Task TestTransportRecoveryLeavesNoUnobservedTaskException()
        {
            var testName = typeof(UnobservedTaskExceptionTest).FullName + "." +
                           nameof(TestTransportRecoveryLeavesNoUnobservedTaskException);
            if (Environment.GetEnvironmentVariable(ChildEnvironmentVariable) != testName)
            {
                await RunInIsolatedTestHost(testName);
                return;
            }

            var unobserved = new ConcurrentQueue<string>();
            var canary = new InvalidOperationException("isolated recovery exception-handler canary");
            var canaryObserved = 0;
            EventHandler<UnobservedTaskExceptionEventArgs> handler = (_, args) =>
            {
                var exceptions = args.Exception.Flatten().InnerExceptions;
                // Only this exact, deliberately unobserved task is excluded. Never clear real recovery faults.
                if (1 == exceptions.Count && ReferenceEquals(canary, exceptions[0]))
                {
                    Interlocked.Increment(ref canaryObserved);
                }
                else
                {
                    unobserved.Enqueue(args.Exception.ToString());
                }

                args.SetObserved();
            };

            TaskScheduler.UnobservedTaskException += handler;
            try
            {
                CreateUnobservedCanary(canary);
                await HeartbeatMonitoringWithFailedRecovery();
                await TelemetryRenewalRetries();
                await RenewalAfterReadLoopEof();
                await CloseWhileAWriteIsStuck();

                RecoveryTestSupport.ForceFinalization();
                var recorded = unobserved.ToArray();
                Assert.AreEqual(0, recorded.Length,
                    $"Unobserved recovery task exceptions:{Environment.NewLine}" +
                    string.Join(Environment.NewLine + "---" + Environment.NewLine, recorded));
                Assert.AreEqual(1, Volatile.Read(ref canaryObserved),
                    "the isolated handler must observe its known canary during finalization");
                Console.WriteLine(ChildSuccessMarker);
            }
            finally
            {
                TaskScheduler.UnobservedTaskException -= handler;
            }
        }

        private static async Task RunInIsolatedTestHost(string testName)
        {
            var assemblyPath = typeof(UnobservedTaskExceptionTest).Assembly.Location;
            var startInfo = new ProcessStartInfo(FindDotnetHost())
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                WorkingDirectory = Path.GetDirectoryName(assemblyPath)
            };
            startInfo.ArgumentList.Add("vstest");
            startInfo.ArgumentList.Add(assemblyPath);
            startInfo.ArgumentList.Add("/TestCaseFilter:FullyQualifiedName=" + testName);
            startInfo.ArgumentList.Add("/Logger:console;verbosity=detailed");
            startInfo.Environment[ChildEnvironmentVariable] = testName;

            using var process = new Process { StartInfo = startInfo };
            Assert.IsTrue(process.Start(), "the isolated testhost must start");
            var stdout = process.StandardOutput.ReadToEndAsync();
            var stderr = process.StandardError.ReadToEndAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            var timedOut = false;
            try
            {
                await process.WaitForExitAsync(timeout.Token);
            }
            catch (OperationCanceledException) when (timeout.IsCancellationRequested)
            {
                timedOut = true;
            }
            finally
            {
                if (!process.HasExited)
                {
                    process.Kill(entireProcessTree: true);
                    await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
                }
            }

            var output = await stdout.WaitAsync(TimeSpan.FromSeconds(5));
            var errors = await stderr.WaitAsync(TimeSpan.FromSeconds(5));
            var diagnostics = output + Environment.NewLine + errors;
            Assert.IsFalse(timedOut, "isolated recovery test timed out:" + Environment.NewLine + diagnostics);
            Assert.AreEqual(0, process.ExitCode, "isolated recovery test failed:" + Environment.NewLine + diagnostics);
            StringAssert.Contains(output, ChildSuccessMarker,
                "the exact filtered test must run its child scenarios, not merely discover zero tests");
        }

        private static string FindDotnetHost()
        {
            var executable = OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet";
            var runtimeHost = Path.GetFullPath(Path.Combine(RuntimeEnvironment.GetRuntimeDirectory(),
                "..", "..", "..", executable));
            var root = Environment.GetEnvironmentVariable("DOTNET_ROOT");
            var candidates = new[]
            {
                Environment.GetEnvironmentVariable("DOTNET_HOST_PATH"),
                runtimeHost,
                string.IsNullOrEmpty(root) ? null : Path.Combine(root, executable)
            };
            foreach (var candidate in candidates)
            {
                if (!string.IsNullOrEmpty(candidate) && Path.IsPathFullyQualified(candidate) && File.Exists(candidate))
                {
                    return candidate;
                }
            }

            throw new FileNotFoundException("Cannot locate the current .NET host via DOTNET_HOST_PATH, the runtime " +
                                            "directory or DOTNET_ROOT; refusing to use an unrelated SDK from PATH.");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void CreateUnobservedCanary(Exception canary)
        {
            _ = Task.FromException(canary);
        }

        private static async Task HeartbeatMonitoringWithFailedRecovery()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig())
            {
                FailReconnectTelemetry = true
            };
            var clientManager = new ClientManager(client);
            client.SetClientManager(clientManager);
            var rpcClient = new Mock<IRpcClient>();
            rpcClient.Setup(c => c.State).Returns(grpcLib.ConnectivityState.Ready);
            var resetCalls = 0;
            rpcClient.Setup(c => c.ResetTransport())
                .Callback(() => Interlocked.Increment(ref resetCalls))
                .Throws(new grpcLib.RpcException(new grpcLib.Status(grpcLib.StatusCode.Internal, "reset failed")));

            // A distinct endpoint keeps the explicit server recovery out of the heartbeat endpoint's cooldown.
            // Both reset and telemetry rebuilding throw, and neither may escape the recovery boundary.
            clientManager.Reconnect(new Endpoints("127.0.0.1:8081"), rpcClient.Object);
            Assert.AreEqual(1, Volatile.Read(ref resetCalls));

            var statuses = new[]
            {
                grpcLib.StatusCode.DeadlineExceeded, grpcLib.StatusCode.Unavailable,
                grpcLib.StatusCode.ResourceExhausted, grpcLib.StatusCode.Internal, grpcLib.StatusCode.Cancelled
            };
            var monitors = new List<Task>();
            foreach (var status in statuses)
            {
                for (var i = 0; i < 4; i++)
                {
                    monitors.Add(clientManager.MonitorHeartbeat(RecoveryTestSupport.FakeEndpoints, rpcClient.Object,
                        RecoveryTestSupport.FailedHeartbeat(status)));
                    monitors.Add(clientManager.MonitorHeartbeat(RecoveryTestSupport.FakeEndpoints, rpcClient.Object,
                        Task.FromResult(new Proto.HeartbeatResponse())));
                }
            }

            // A pending monitor has not yet observed its raw heartbeat failure.
            await Task.WhenAll(monitors);
            Assert.IsTrue(Volatile.Read(ref resetCalls) >= 2,
                "a heartbeat-driven recovery must also attempt the failing reset");
        }

        private static async Task TelemetryRenewalRetries()
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
                    throw new grpcLib.RpcException(new grpcLib.Status(grpcLib.StatusCode.Unavailable, "no stream"));
                }

                return renewed.Call;
            });
            client.SetClientManager(clientManager.Object);
            var session = new Session(endpoints, first.Call, client);
            try
            {
                session.Reconnect();
                await TestAwaiter.Until(() => renewed.WrittenCommands > 0 && 1 == first.DisposeCalls,
                    "the retry to write settings on the replacement stream", TimeSpan.FromSeconds(10));
                Assert.AreEqual(3, Volatile.Read(ref attempts));
                Assert.IsNotNull(renewed.WrittenCommand(0).Settings);
                Assert.AreEqual(0, renewed.DisposeCalls);
            }
            finally
            {
                await session.CloseAsync();
            }
        }

        private static async Task RenewalAfterReadLoopEof()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            var endpoints = new Endpoints(client.GetClientConfig().Endpoints);
            using var first = new FakeTelemetryCall();
            using var renewed = new FakeTelemetryCall();
            var clientManager = new Mock<IClientManager>();
            clientManager.Setup(m => m.Telemetry(endpoints)).Returns(renewed.Call);
            client.SetClientManager(clientManager.Object);
            var session = new Session(endpoints, first.Call, client);
            try
            {
                first.Reader.Close();
                await TestAwaiter.Until(() => renewed.WrittenCommands > 0 && 1 == first.DisposeCalls,
                    "EOF to renew telemetry and write settings on a healthy stream");
                Assert.IsNotNull(renewed.WrittenCommand(0).Settings);
                Assert.AreEqual(0, renewed.DisposeCalls);
                clientManager.Verify(m => m.Telemetry(endpoints), Times.Once);
            }
            finally
            {
                await session.CloseAsync();
            }
        }

        private static async Task CloseWhileAWriteIsStuck()
        {
            await using var client = new RecoveryTestClient(RecoveryTestSupport.CreateClientConfig());
            var endpoints = new Endpoints(client.GetClientConfig().Endpoints);
            using var stream = new FakeTelemetryCall();
            client.SetClientManager(new Mock<IClientManager>().Object);
            var session = new Session(endpoints, stream.Call, client);
            var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            stream.Writer.WriteGate = gate;
            var write = session.WriteAsync(new Proto.TelemetryCommand());
            try
            {
                await TestAwaiter.Until(() => 1 == stream.WrittenCommands, "the write to reach the stream");
                Assert.IsFalse(write.IsCompleted, "the write must still be blocked when close begins");
                await session.CloseAsync().WaitAsync(Session.CloseTimeout.Add(TimeSpan.FromSeconds(5)));
                Assert.AreEqual(1, stream.DisposeCalls);
                Assert.IsFalse(gate.Task.IsCompleted, "call disposal must not complete or fault the caller's gate");
            }
            finally
            {
                await session.CloseAsync();
                // WriteAsync belongs to its caller, who must observe the real cancellation fault even during cleanup.
                var exception = await Assert.ThrowsExactlyAsync<grpcLib.RpcException>(
                    () => write.WaitAsync(TimeSpan.FromSeconds(5)));
                Assert.AreEqual(grpcLib.StatusCode.Cancelled, exception.StatusCode);
                Assert.IsTrue(write.IsFaulted, "disposal must fault the pending write, not report success");
            }
        }
    }
}
