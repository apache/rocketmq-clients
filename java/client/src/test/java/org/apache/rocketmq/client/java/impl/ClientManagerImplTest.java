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

package org.apache.rocketmq.client.java.impl;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.RecallMessageRequest;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SyncLiteSubscriptionRequest;
import com.google.common.util.concurrent.Futures;
import io.grpc.ConnectivityState;
import io.grpc.Metadata;
import io.grpc.Status;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.RpcClient;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class ClientManagerImplTest extends TestBase {
    private static ClientManagerImpl CLIENT_MANAGER;

    @BeforeClass
    public static void setUp() throws Exception {
        Client client = Mockito.mock(Client.class);
        final Metadata metadata = new Metadata();
        Mockito.doReturn(metadata).when(client).sign();
        final ClientId clientId = new ClientId();
        Mockito.doReturn(clientId).when(client).getClientId();
        CLIENT_MANAGER = new ClientManagerImpl(client);
        Mockito.when(client.getClientId()).thenReturn(FAKE_CLIENT_ID);
        CLIENT_MANAGER.startAsync().awaitRunning();
    }

    @AfterClass
    public static void tearDown() {
        CLIENT_MANAGER.stopAsync().awaitTerminated();
    }

    @Test
    public void testQueryRoute() {
        QueryRouteRequest request = QueryRouteRequest.newBuilder().build();
        CLIENT_MANAGER.queryRoute(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.queryRoute(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testHeartbeat() {
        HeartbeatRequest request = HeartbeatRequest.newBuilder().build();
        CLIENT_MANAGER.heartbeat(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.heartbeat(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testSendMessage() {
        SendMessageRequest request = SendMessageRequest.newBuilder().build();
        CLIENT_MANAGER.sendMessage(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.sendMessage(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testQueryAssignment() {
        QueryAssignmentRequest request = QueryAssignmentRequest.newBuilder().build();
        CLIENT_MANAGER.queryAssignment(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.queryAssignment(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testReceiveMessage() {
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder().build();
        CLIENT_MANAGER.receiveMessage(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.receiveMessage(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testAckMessage() {
        AckMessageRequest request = AckMessageRequest.newBuilder().build();
        CLIENT_MANAGER.ackMessage(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.ackMessage(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testChangeInvisibleDuration() {
        ChangeInvisibleDurationRequest request = ChangeInvisibleDurationRequest.newBuilder().build();
        CLIENT_MANAGER.changeInvisibleDuration(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.changeInvisibleDuration(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testForwardMessageToDeadLetterQueue() {
        ForwardMessageToDeadLetterQueueRequest request = ForwardMessageToDeadLetterQueueRequest.newBuilder().build();
        CLIENT_MANAGER.forwardMessageToDeadLetterQueue(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.forwardMessageToDeadLetterQueue(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testEndTransaction() {
        EndTransactionRequest request = EndTransactionRequest.newBuilder().build();
        CLIENT_MANAGER.endTransaction(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.endTransaction(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testNotifyClientTermination() {
        NotifyClientTerminationRequest request = NotifyClientTerminationRequest.newBuilder().build();
        CLIENT_MANAGER.notifyClientTermination(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.notifyClientTermination(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testRecallMessage() {
        RecallMessageRequest request = RecallMessageRequest.newBuilder().build();
        CLIENT_MANAGER.recallMessage(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.recallMessage(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testSyncLiteSubscription() {
        SyncLiteSubscriptionRequest request = SyncLiteSubscriptionRequest.newBuilder().build();
        CLIENT_MANAGER.syncLiteSubscription(fakeEndpoints(), request, Duration.ofSeconds(1));
        CLIENT_MANAGER.syncLiteSubscription(null, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testHeartbeatDeadlineExceededTriggersRecoveryAfterThreshold() {
        final ClientManagerImpl clientManager = createClientManager();
        final RpcClient rpcClient = Mockito.mock(RpcClient.class);

        clientManager.monitorHeartbeat(fakeEndpoints(), rpcClient,
            Futures.immediateFailedFuture(Status.DEADLINE_EXCEEDED.asRuntimeException()));
        Mockito.verify(rpcClient, Mockito.never()).enterIdle();

        clientManager.monitorHeartbeat(fakeEndpoints(), rpcClient,
            Futures.immediateFailedFuture(Status.DEADLINE_EXCEEDED.asRuntimeException()));
        Mockito.verify(rpcClient, Mockito.times(1)).enterIdle();
    }

    @Test
    public void testHeartbeatSuccessResetsFailureAttempts() {
        final ClientManagerImpl clientManager = createClientManager();
        final RpcClient rpcClient = Mockito.mock(RpcClient.class);

        clientManager.monitorHeartbeat(fakeEndpoints(), rpcClient,
            Futures.immediateFailedFuture(Status.DEADLINE_EXCEEDED.asRuntimeException()));
        clientManager.monitorHeartbeat(fakeEndpoints(), rpcClient,
            Futures.immediateFuture(HeartbeatResponse.getDefaultInstance()));
        clientManager.monitorHeartbeat(fakeEndpoints(), rpcClient,
            Futures.immediateFailedFuture(Status.DEADLINE_EXCEEDED.asRuntimeException()));

        Mockito.verify(rpcClient, Mockito.never()).enterIdle();
    }

    @Test
    public void testHeartbeatUnavailableTriggersRecoveryImmediately() {
        final ClientManagerImpl clientManager = createClientManager();
        final RpcClient rpcClient = Mockito.mock(RpcClient.class);
        Mockito.when(rpcClient.getState(false)).thenReturn(ConnectivityState.READY);

        clientManager.monitorHeartbeat(fakeEndpoints(), rpcClient,
            Futures.immediateFailedFuture(Status.UNAVAILABLE.asRuntimeException()));

        Mockito.verify(rpcClient, Mockito.times(1)).enterIdle();
    }

    @Test
    public void testHeartbeatUnavailableDoesNotRecoverNonReadyChannel() {
        final ClientManagerImpl clientManager = createClientManager();
        final RpcClient rpcClient = Mockito.mock(RpcClient.class);
        Mockito.when(rpcClient.getState(false)).thenReturn(ConnectivityState.TRANSIENT_FAILURE);

        clientManager.monitorHeartbeat(fakeEndpoints(), rpcClient,
            Futures.immediateFailedFuture(Status.DEADLINE_EXCEEDED.asRuntimeException()));
        clientManager.monitorHeartbeat(fakeEndpoints(), rpcClient,
            Futures.immediateFailedFuture(Status.UNAVAILABLE.asRuntimeException()));
        clientManager.monitorHeartbeat(fakeEndpoints(), rpcClient,
            Futures.immediateFailedFuture(Status.DEADLINE_EXCEEDED.asRuntimeException()));

        Mockito.verify(rpcClient, Mockito.never()).enterIdle();
    }

    @Test
    public void testHeartbeatResourceExhaustedDoesNotTriggerRecovery() {
        final ClientManagerImpl clientManager = createClientManager();
        final RpcClient rpcClient = Mockito.mock(RpcClient.class);

        clientManager.monitorHeartbeat(fakeEndpoints(), rpcClient,
            Futures.immediateFailedFuture(Status.RESOURCE_EXHAUSTED.asRuntimeException()));

        Mockito.verify(rpcClient, Mockito.never()).enterIdle();
    }

    @Test
    public void testHeartbeatRecoveryHasCooldown() {
        final ClientManagerImpl clientManager = createClientManager();
        final RpcClient rpcClient = Mockito.mock(RpcClient.class);

        for (int i = 0; i < 2 * ClientManagerImpl.HEART_BEAT_FAILURE_THRESHOLD; i++) {
            clientManager.monitorHeartbeat(fakeEndpoints(), rpcClient,
                Futures.immediateFailedFuture(Status.DEADLINE_EXCEEDED.asRuntimeException()));
        }

        Mockito.verify(rpcClient, Mockito.times(1)).enterIdle();
    }

    @Test
    public void testConcurrentReconnectOnlyRecoversOnce() throws InterruptedException {
        final Client client = Mockito.mock(Client.class);
        Mockito.when(client.getClientId()).thenReturn(FAKE_CLIENT_ID);
        final ClientManagerImpl clientManager = new ClientManagerImpl(client);
        final RpcClient rpcClient = Mockito.mock(RpcClient.class);
        final Endpoints endpoints = fakeEndpoints();
        final int threadCount = 8;
        final CountDownLatch ready = new CountDownLatch(threadCount);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            for (int i = 0; i < threadCount; i++) {
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        clientManager.reconnect(endpoints, rpcClient);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            Assert.assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            Assert.assertTrue(done.await(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }

        Mockito.verify(rpcClient, Mockito.times(1)).enterIdle();
        Mockito.verify(client, Mockito.times(1)).reconnectTelemetry(Mockito.eq(endpoints));
    }

    private ClientManagerImpl createClientManager() {
        final Client client = Mockito.mock(Client.class);
        Mockito.when(client.getClientId()).thenReturn(FAKE_CLIENT_ID);
        return new ClientManagerImpl(client);
    }

}
