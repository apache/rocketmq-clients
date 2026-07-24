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

package org.apache.rocketmq.test.client;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SendResultEntry;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.java.message.MessageIdCodec;
import org.apache.rocketmq.test.server.BaseMockServerImpl;
import org.apache.rocketmq.test.server.GrpcServerIntegrationTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ProducerHalfOpenTcpRecoveryIntegrationTest extends GrpcServerIntegrationTest {
    private static final String LOOPBACK = "127.0.0.1";
    private static final String TOPIC = "topic";
    private static final Duration MAX_RECOVERY_TIME = Duration.ofSeconds(40);

    private BaseMockServerImpl serverImpl;

    @Before
    public void setUp() throws Exception {
        serverImpl = new ProducerMockServer(TOPIC);
        setUpServer(serverImpl, port);
    }

    @Test(timeout = 60000)
    public void testProducerRecoversFromHalfOpenTcpAfterHeartbeatTimeouts() throws Exception {
        final BlackholeTcpProxy proxy = new BlackholeTcpProxy(port);
        serverImpl.setPort(proxy.getPort());
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        final SessionCredentialsProvider credentials =
            new StaticSessionCredentialsProvider("accessKey", "secretKey");
        final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(LOOPBACK + ":" + proxy.getPort())
            .setCredentialProvider(credentials)
            .setRequestTimeout(Duration.ofSeconds(3))
            .build();
        final Producer producer = provider.newProducerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setTopics(TOPIC)
            .setMaxAttempts(1)
            .build();
        final Message message = provider.newMessageBuilder()
            .setTopic(TOPIC)
            .setBody("tcp-blackhole".getBytes(StandardCharsets.UTF_8))
            .build();

        try {
            producer.send(message);
            final int initialConnectionCount = proxy.getAcceptedConnectionCount();
            Assert.assertTrue(initialConnectionCount > 0);
            proxy.blackholeExistingConnections();
            final long blackholeStartNanoTime = System.nanoTime();

            assertSendFailure(producer, message);
            Assert.assertEquals(0, proxy.getClosedConnectionCount());

            while (Duration.ofNanos(System.nanoTime() - blackholeStartNanoTime).compareTo(MAX_RECOVERY_TIME) < 0) {
                try {
                    producer.send(message);
                } catch (Exception ignore) {
                    // Keep sending until heartbeat recovery replaces the blackholed TCP connection.
                    continue;
                }
                Assert.assertTrue(proxy.getAcceptedConnectionCount() > initialConnectionCount);
                return;
            }
            Assert.fail("Producer did not recover from the half-open TCP connection within " + MAX_RECOVERY_TIME);
        } finally {
            producer.close();
            proxy.close();
        }
    }

    private static void assertSendFailure(Producer producer, Message message) {
        Exception failure = null;
        try {
            producer.send(message);
        } catch (Exception t) {
            failure = t;
        }
        Assert.assertNotNull("Message should time out on the blackholed TCP connection", failure);
    }

    private static final class ProducerMockServer extends BaseMockServerImpl {
        private ProducerMockServer(String topic) {
            super(topic);
        }

        @Override
        public void sendMessage(SendMessageRequest request, StreamObserver<SendMessageResponse> responseObserver) {
            final apache.rocketmq.v2.Status status =
                apache.rocketmq.v2.Status.newBuilder().setCode(Code.OK).build();
            final SendResultEntry entry = SendResultEntry.newBuilder()
                .setStatus(status)
                .setMessageId(MessageIdCodec.getInstance().nextMessageId().toString())
                .setOffset(1)
                .build();
            responseObserver.onNext(SendMessageResponse.newBuilder()
                .setStatus(status)
                .addEntries(entry)
                .build());
            responseObserver.onCompleted();
        }
    }

    private static final class BlackholeTcpProxy implements AutoCloseable {
        private final int backendPort;
        private final ServerSocket serverSocket;
        private final ExecutorService executor;
        private final AtomicBoolean running;
        private final AtomicInteger acceptedConnectionCount;
        private final AtomicInteger closedConnectionCount;
        private final AtomicInteger blackholeThroughConnectionId;
        private final List<SocketPair> connections;

        private BlackholeTcpProxy(int backendPort) throws IOException {
            this.backendPort = backendPort;
            this.serverSocket = new ServerSocket();
            this.serverSocket.bind(new InetSocketAddress(InetAddress.getByName(LOOPBACK), 0));
            this.executor = Executors.newCachedThreadPool(runnable -> {
                final Thread thread = new Thread(runnable, "BlackholeTcpProxy");
                thread.setDaemon(true);
                return thread;
            });
            this.running = new AtomicBoolean(true);
            this.acceptedConnectionCount = new AtomicInteger();
            this.closedConnectionCount = new AtomicInteger();
            this.blackholeThroughConnectionId = new AtomicInteger();
            this.connections = new CopyOnWriteArrayList<>();
            this.executor.execute(this::acceptConnections);
        }

        private int getPort() {
            return serverSocket.getLocalPort();
        }

        private int getAcceptedConnectionCount() {
            return acceptedConnectionCount.get();
        }

        private int getClosedConnectionCount() {
            return closedConnectionCount.get();
        }

        private void blackholeExistingConnections() {
            blackholeThroughConnectionId.set(acceptedConnectionCount.get());
        }

        private void acceptConnections() {
            while (running.get()) {
                try {
                    final Socket downstream = serverSocket.accept();
                    downstream.setTcpNoDelay(true);
                    final Socket upstream = new Socket();
                    upstream.setTcpNoDelay(true);
                    upstream.connect(new InetSocketAddress(LOOPBACK, backendPort));
                    final int connectionId = acceptedConnectionCount.incrementAndGet();
                    final SocketPair connection = new SocketPair(connectionId, downstream, upstream);
                    connections.add(connection);
                    executor.execute(() -> forward(connection, downstream, upstream));
                    executor.execute(() -> forward(connection, upstream, downstream));
                } catch (IOException e) {
                    if (running.get()) {
                        throw new IllegalStateException("Failed to accept proxied connection", e);
                    }
                }
            }
        }

        private void forward(SocketPair connection, Socket source, Socket destination) {
            final byte[] buffer = new byte[8192];
            try {
                final InputStream input = source.getInputStream();
                final OutputStream output = destination.getOutputStream();
                while (running.get()) {
                    final int length = input.read(buffer);
                    if (length < 0) {
                        break;
                    }
                    if (connection.id <= blackholeThroughConnectionId.get()) {
                        continue;
                    }
                    output.write(buffer, 0, length);
                    output.flush();
                }
            } catch (IOException ignore) {
                // Socket closure is expected during transport recovery and test cleanup.
            } finally {
                connection.close();
            }
        }

        @Override
        public void close() {
            if (!running.compareAndSet(true, false)) {
                return;
            }
            try {
                serverSocket.close();
            } catch (IOException ignore) {
                // Ignore exception on purpose.
            }
            connections.forEach(SocketPair::close);
            executor.shutdownNow();
        }

        private final class SocketPair {
            private final int id;
            private final Socket downstream;
            private final Socket upstream;
            private final AtomicBoolean closed;

            private SocketPair(int id, Socket downstream, Socket upstream) {
                this.id = id;
                this.downstream = downstream;
                this.upstream = upstream;
                this.closed = new AtomicBoolean();
            }

            private void close() {
                if (!closed.compareAndSet(false, true)) {
                    return;
                }
                closeSocket(downstream);
                closeSocket(upstream);
                closedConnectionCount.incrementAndGet();
            }

            private void closeSocket(Socket socket) {
                try {
                    socket.close();
                } catch (IOException ignore) {
                    // Ignore exception on purpose.
                }
            }
        }
    }
}
