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
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.SendMessageRequest;
import io.grpc.Metadata;
import java.time.Duration;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.AfterClass;
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
}