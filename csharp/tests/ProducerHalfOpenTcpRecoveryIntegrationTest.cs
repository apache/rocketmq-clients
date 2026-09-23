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
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    /// <summary>
    /// A peer which disappears without sending either FIN or RST leaves the connection half-open: the transport still
    /// considers it usable, so every request written onto it blocks until its own deadline expires. Recovery has to be
    /// driven by the heartbeat of the client, which replaces the transport once it timed out twice in a row.
    /// </summary>
    [TestClass]
    public class ProducerHalfOpenTcpRecoveryIntegrationTest : GrpcServerIntegrationTest
    {
        private const string Loopback = "127.0.0.1";
        private const string Topic = "topic";
        private const string Broker = "broker";

        private static readonly TimeSpan MaxRecoveryTime = TimeSpan.FromSeconds(40);

        private Server _server;
        private MockServer _mockServer;

        [TestInitialize]
        public void SetUp()
        {
            _mockServer = new MockServer(Topic, Broker, new List<string>());
            _server = SetUpServer(_mockServer);
            _mockServer.Port = Port;
        }

        [TestCleanup]
        public async Task TearDown()
        {
            await _server.ShutdownAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }

        [TestMethod]
        [Timeout(60000)]
        public async Task TestProducerRecoversFromHalfOpenTcpAfterHeartbeatTimeouts()
        {
            var proxy = new BlackholeTcpProxy(Port);
            Producer producer = null;
            // Everything below can fail while the proxy is already listening, so the try has to cover it or the
            // listener and its accept loop would outlive this test and disturb the ones running after it.
            try
            {
                // The route data must advertise the proxy, otherwise the producer would talk to the server directly.
                _mockServer.Port = proxy.Port;

                var credentialsProvider = new StaticSessionCredentialsProvider("accessKey", "secretKey");
                var clientConfig = new ClientConfig.Builder()
                    .SetEndpoints(Loopback + ":" + proxy.Port)
                    .SetCredentialsProvider(credentialsProvider)
                    .EnableSsl(false)
                    .SetRequestTimeout(TimeSpan.FromSeconds(3))
                    .Build();

                producer = await new Producer.Builder()
                    .SetClientConfig(clientConfig)
                    .SetTopics(Topic)
                    .SetMaxAttempts(1)
                    .Build();

                var message = new Message.Builder()
                    .SetTopic(Topic)
                    .SetBody(Encoding.UTF8.GetBytes("tcp-blackhole"))
                    .Build();

                await producer.Send(message);
                proxy.AssertHealthy();
                var initialConnectionCount = proxy.AcceptedConnectionCount;
                Assert.IsTrue(initialConnectionCount > 0);

                proxy.BlackholeExistingConnections();
                var blackholeStart = Stopwatch.GetTimestamp();

                await AssertSendFailure(producer, message);
                // No connection was torn down, the blackholed one is half-open rather than closed.
                Assert.AreEqual(0, proxy.ClosedConnectionCount);

                while (Stopwatch.GetElapsedTime(blackholeStart) < MaxRecoveryTime)
                {
                    proxy.AssertHealthy();
                    try
                    {
                        await producer.Send(message);
                    }
                    catch (Exception)
                    {
                        // Keep sending until the heartbeat recovery replaces the blackholed connection.
                        continue;
                    }

                    Assert.IsTrue(proxy.AcceptedConnectionCount > initialConnectionCount,
                        "The producer recovered without establishing a new connection");
                    return;
                }

                proxy.AssertHealthy();
                Assert.Fail($"Producer did not recover from the half-open TCP connection within {MaxRecoveryTime}");
            }
            finally
            {
                if (null != producer)
                {
                    await producer.DisposeAsync();
                }

                proxy.Dispose();
            }
        }

        private static async Task AssertSendFailure(Producer producer, Message message)
        {
            Exception failure = null;
            try
            {
                await producer.Send(message);
            }
            catch (Exception e)
            {
                failure = e;
            }

            Assert.IsNotNull(failure, "Message should time out on the blackholed TCP connection");
        }

        /// <summary>
        /// Forwards traffic to the backend until the connections established so far are blackholed. A blackholed
        /// connection keeps being drained but nothing is forwarded anymore, so neither peer ever observes a FIN or a
        /// RST and both sides keep the connection in their pool.
        /// </summary>
        private sealed class BlackholeTcpProxy : IDisposable
        {
            private readonly int _backendPort;
            private readonly TcpListener _listener;
            private readonly CancellationTokenSource _cts = new CancellationTokenSource();
            private readonly ConcurrentBag<SocketPair> _connections = new ConcurrentBag<SocketPair>();

            private int _acceptedConnectionCount;
            private int _closedConnectionCount;
            private int _blackholeThroughConnectionId;
            private Exception _acceptFailure;

            internal BlackholeTcpProxy(int backendPort)
            {
                _backendPort = backendPort;
                _listener = new TcpListener(IPAddress.Loopback, 0);
                _listener.Start();
                Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
                _ = AcceptConnections();
            }

            internal int Port { get; }

            internal int AcceptedConnectionCount => Volatile.Read(ref _acceptedConnectionCount);

            internal int ClosedConnectionCount => Volatile.Read(ref _closedConnectionCount);

            internal void BlackholeExistingConnections()
            {
                Interlocked.Exchange(ref _blackholeThroughConnectionId,
                    Volatile.Read(ref _acceptedConnectionCount));
            }

            /// <summary>
            /// Surfaces a failure of the accept loop, which would otherwise make the producer hang instead of failing.
            /// </summary>
            internal void AssertHealthy()
            {
                var failure = Volatile.Read(ref _acceptFailure);
                if (null == failure)
                {
                    return;
                }

                throw new AssertFailedException("TCP proxy stopped accepting connections", failure);
            }

            private async Task AcceptConnections()
            {
                while (!_cts.IsCancellationRequested)
                {
                    TcpClient downstream = null;
                    try
                    {
                        downstream = await _listener.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false);
                        downstream.NoDelay = true;

                        var upstream = new TcpClient();
                        upstream.NoDelay = true;
                        await upstream.ConnectAsync(IPAddress.Loopback, _backendPort, _cts.Token).ConfigureAwait(false);

                        var connection = new SocketPair(this,
                            Interlocked.Increment(ref _acceptedConnectionCount), downstream, upstream);
                        _connections.Add(connection);
                        _ = Forward(connection, downstream, upstream);
                        _ = Forward(connection, upstream, downstream);
                    }
                    catch (Exception e)
                    {
                        if (!_cts.IsCancellationRequested)
                        {
                            Interlocked.CompareExchange(ref _acceptFailure, e, null);
                        }

                        downstream?.Dispose();
                        return;
                    }
                }
            }

            private async Task Forward(SocketPair connection, TcpClient source, TcpClient destination)
            {
                var buffer = new byte[8192];
                try
                {
                    var input = source.GetStream();
                    var output = destination.GetStream();
                    while (!_cts.IsCancellationRequested)
                    {
                        var length = await input.ReadAsync(buffer.AsMemory(), _cts.Token).ConfigureAwait(false);
                        if (0 == length)
                        {
                            break;
                        }

                        if (connection.Id <= Volatile.Read(ref _blackholeThroughConnectionId))
                        {
                            continue;
                        }

                        await output.WriteAsync(buffer.AsMemory(0, length), _cts.Token).ConfigureAwait(false);
                        await output.FlushAsync(_cts.Token).ConfigureAwait(false);
                    }
                }
                catch (Exception)
                {
                    // Socket closure is expected during transport recovery and test cleanup.
                }
                finally
                {
                    connection.Close();
                }
            }

            public void Dispose()
            {
                if (_cts.IsCancellationRequested)
                {
                    return;
                }

                _cts.Cancel();
                try
                {
                    _listener.Stop();
                }
                catch (Exception)
                {
                    // Ignore exception on purpose.
                }

                foreach (var connection in _connections)
                {
                    connection.Close();
                }
            }

            private sealed class SocketPair
            {
                private readonly BlackholeTcpProxy _proxy;
                private readonly TcpClient _downstream;
                private readonly TcpClient _upstream;
                private int _closed;

                internal SocketPair(BlackholeTcpProxy proxy, int id, TcpClient downstream, TcpClient upstream)
                {
                    _proxy = proxy;
                    Id = id;
                    _downstream = downstream;
                    _upstream = upstream;
                }

                internal int Id { get; }

                internal void Close()
                {
                    if (0 != Interlocked.CompareExchange(ref _closed, 1, 0))
                    {
                        return;
                    }

                    Dispose(_downstream);
                    Dispose(_upstream);
                    Interlocked.Increment(ref _proxy._closedConnectionCount);
                }

                private static void Dispose(TcpClient socket)
                {
                    try
                    {
                        socket.Dispose();
                    }
                    catch (Exception)
                    {
                        // Ignore exception on purpose.
                    }
                }
            }
        }
    }
}
