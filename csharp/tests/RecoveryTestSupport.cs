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
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Endpoints = Org.Apache.Rocketmq.Endpoints;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    /// <summary>
    /// Client pinned to the running state whose session lifecycle hooks are observable, so that transport recovery can
    /// be driven without a live broker.
    /// </summary>
    internal class RecoveryTestClient : Producer
    {
        private readonly TaskCompletionSource<Proto.ReconnectEndpointsCommand> _firstReconnectCommand =
            new TaskCompletionSource<Proto.ReconnectEndpointsCommand>(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _reconnectTelemetryCalls;
        private int _removeSessionCalls;

        internal RecoveryTestClient(ClientConfig clientConfig)
            : base(clientConfig, new ConcurrentDictionary<string, bool>(), 1, null)
        {
            State = Org.Apache.Rocketmq.State.Running;
        }

        /// <summary>
        /// Whether the endpoints must be reported as dropped from the topic route table.
        /// </summary>
        internal volatile bool EndpointsDeprecated;

        /// <summary>
        /// Whether the telemetry renewal must fail, so that the recovery paths can be driven into their failure
        /// branches.
        /// </summary>
        internal volatile bool FailReconnectTelemetry;

        internal int ReconnectTelemetryCalls => Volatile.Read(ref _reconnectTelemetryCalls);

        internal int RemoveSessionCalls => Volatile.Read(ref _removeSessionCalls);

        internal Task<Proto.ReconnectEndpointsCommand> FirstReconnectCommand => _firstReconnectCommand.Task;

        /// <summary>
        /// Runs after the renewal counter is incremented, so that a test can pin the order in which recovery replaces
        /// the transport and rebuilds the telemetry.
        /// </summary>
        internal Action<Endpoints> ReconnectTelemetryHook;

        internal override void ReconnectTelemetry(Endpoints endpoints, Session expectedSession = null)
        {
            if (FailReconnectTelemetry)
            {
                throw new RpcException(new Status(StatusCode.Unavailable, "no telemetry"));
            }

            Interlocked.Increment(ref _reconnectTelemetryCalls);
            ReconnectTelemetryHook?.Invoke(endpoints);
        }

        internal override bool IsEndpointsDeprecated(Endpoints endpoints)
        {
            return EndpointsDeprecated;
        }

        internal override async Task<bool> RemoveSession(Endpoints endpoints, Session session)
        {
            Interlocked.Increment(ref _removeSessionCalls);
            await session.CloseAsync();
            return true;
        }

        internal override void OnReconnectEndpointsCommand(Endpoints endpoints, Proto.ReconnectEndpointsCommand command)
        {
            _firstReconnectCommand.TrySetResult(command);
        }
    }

    /// <summary>
    /// In-memory telemetry stream whose writes and disposal are observable, standing in for a gRPC call.
    /// </summary>
    internal sealed class FakeTelemetryCall : IDisposable
    {
        internal readonly RecordingStreamWriter Writer;
        internal readonly FakeStreamReader Reader;
        internal readonly AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand> Call;

        private int _disposeCalls;

        internal FakeTelemetryCall(bool keepStreamOpen = true, params Proto.TelemetryCommand[] responses)
        {
            Writer = new RecordingStreamWriter();
            Reader = new FakeStreamReader(keepStreamOpen, responses);
            Call = new AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand>(
                Writer, Reader, null, null, null, () =>
                {
                    if (0 != Interlocked.Exchange(ref _disposeCalls, 1))
                    {
                        return;
                    }

                    // A real call cancels pending reads and writes, not the caller's test gate.
                    Reader.Close();
                    Writer.Close();
                });
        }

        public void Dispose()
        {
            Call.Dispose();
        }

        internal int DisposeCalls => Volatile.Read(ref _disposeCalls);

        internal int WrittenCommands
        {
            get
            {
                lock (Writer.Commands)
                {
                    return Writer.Commands.Count;
                }
            }
        }

        internal Proto.TelemetryCommand WrittenCommand(int index)
        {
            lock (Writer.Commands)
            {
                return Writer.Commands[index];
            }
        }
    }

    internal sealed class RecordingStreamWriter : IClientStreamWriter<Proto.TelemetryCommand>
    {
        internal readonly List<Proto.TelemetryCommand> Commands = new List<Proto.TelemetryCommand>();

        private readonly CancellationTokenSource _closed = new CancellationTokenSource();
        private bool _disposed;

        /// <summary>
        /// When set, writes are recorded and then wait for this gate or call disposal. Disposal leaves the gate itself
        /// untouched, so an unused gate never acquires an unobserved exception.
        /// </summary>
        internal TaskCompletionSource<bool> WriteGate { get; set; }

        public WriteOptions WriteOptions { get; set; }

        public async Task WriteAsync(Proto.TelemetryCommand message)
        {
            Task gate;
            CancellationToken cancellationToken;
            lock (Commands)
            {
                if (_disposed)
                {
                    throw new RpcException(new Status(StatusCode.Cancelled, "telemetry call disposed"));
                }

                cancellationToken = _closed.Token;
                gate = WriteGate?.Task;
                Commands.Add(message);
            }

            try
            {
                if (null != gate)
                {
                    await gate.WaitAsync(cancellationToken);
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw new RpcException(new Status(StatusCode.Cancelled, "telemetry call disposed"));
            }
        }

        internal void Close()
        {
            lock (Commands)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                _closed.Cancel();
                _closed.Dispose();
            }
        }

        public Task CompleteAsync()
        {
            return Task.CompletedTask;
        }
    }

    internal sealed class FakeStreamReader : IAsyncStreamReader<Proto.TelemetryCommand>
    {
        private readonly Queue<Proto.TelemetryCommand> _responses;
        private readonly bool _keepOpen;
        private readonly TaskCompletionSource<bool> _closed =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal FakeStreamReader(bool keepOpen, params Proto.TelemetryCommand[] responses)
        {
            _keepOpen = keepOpen;
            _responses = new Queue<Proto.TelemetryCommand>(responses);
        }

        public Proto.TelemetryCommand Current { get; private set; }

        internal void Close()
        {
            lock (_responses)
            {
                _responses.Clear();
                _closed.TrySetResult(true);
            }
        }

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lock (_responses)
            {
                if (_closed.Task.IsCompleted)
                {
                    return false;
                }

                if (_responses.Count > 0)
                {
                    Current = _responses.Dequeue();
                    return true;
                }
            }

            if (!_keepOpen)
            {
                return false;
            }

            await _closed.Task.WaitAsync(cancellationToken);
            return false;
        }
    }

    internal static class TestAwaiter
    {
        internal static async Task Until(Func<bool> condition, string description, TimeSpan? timeout = null)
        {
            var deadline = DateTime.UtcNow.Add(timeout ?? TimeSpan.FromSeconds(5));
            while (!condition())
            {
                Assert.IsTrue(DateTime.UtcNow < deadline, $"Timed out waiting for {description}");
                await Task.Delay(20);
            }
        }
    }

    internal static class RecoveryTestSupport
    {
        internal static readonly Endpoints FakeEndpoints = new Endpoints("127.0.0.1:8080");

        internal static ClientConfig CreateClientConfig()
        {
            return new ClientConfig.Builder().SetEndpoints("127.0.0.1:9876").Build();
        }

        internal static Task<Proto.HeartbeatResponse> FailedHeartbeat(StatusCode statusCode)
        {
            return Task.FromException<Proto.HeartbeatResponse>(new RpcException(new Status(statusCode, null)));
        }

        /// <summary>
        /// An unobserved task exception and a leaked descriptor both surface only once the offending object is
        /// finalized, which a full collection forces.
        /// </summary>
        internal static void ForceFinalization()
        {
            for (var i = 0; i < 3; i++)
            {
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
                GC.WaitForPendingFinalizers();
            }

            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
        }
    }
}
