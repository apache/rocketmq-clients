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
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClientManagerImplTest extends TestBase {
    private static final ClientManagerImpl CLIENT_MANAGER = new ClientManagerImpl(null);

    @BeforeClass
    public static void setUp() {
        CLIENT_MANAGER.startAsync().awaitRunning();
    }

    @AfterClass
    public static void tearDown() {
        CLIENT_MANAGER.stopAsync().awaitTerminated();
    }

    @Test
    public void testQueryRoute() {
        Metadata metadata = new Metadata();
        QueryRouteRequest request = QueryRouteRequest.newBuilder().build();
        CLIENT_MANAGER.queryRoute(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        CLIENT_MANAGER.queryRoute(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testHeartbeat() {
        Metadata metadata = new Metadata();
        HeartbeatRequest request = HeartbeatRequest.newBuilder().build();
        CLIENT_MANAGER.heartbeat(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        CLIENT_MANAGER.heartbeat(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testSendMessage() {
        Metadata metadata = new Metadata();
        SendMessageRequest request = SendMessageRequest.newBuilder().build();
        CLIENT_MANAGER.sendMessage(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        CLIENT_MANAGER.sendMessage(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testQueryAssignment() {
        Metadata metadata = new Metadata();
        QueryAssignmentRequest request = QueryAssignmentRequest.newBuilder().build();
        CLIENT_MANAGER.queryAssignment(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        CLIENT_MANAGER.queryAssignment(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testReceiveMessage() {
        Metadata metadata = new Metadata();
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder().build();
        CLIENT_MANAGER.receiveMessage(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        CLIENT_MANAGER.receiveMessage(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testAckMessage() {
        Metadata metadata = new Metadata();
        AckMessageRequest request = AckMessageRequest.newBuilder().build();
        CLIENT_MANAGER.ackMessage(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        CLIENT_MANAGER.ackMessage(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testChangeInvisibleDuration() {
        Metadata metadata = new Metadata();
        ChangeInvisibleDurationRequest request = ChangeInvisibleDurationRequest.newBuilder().build();
        CLIENT_MANAGER.changeInvisibleDuration(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        CLIENT_MANAGER.changeInvisibleDuration(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testForwardMessageToDeadLetterQueue() {
        Metadata metadata = new Metadata();
        ForwardMessageToDeadLetterQueueRequest request = ForwardMessageToDeadLetterQueueRequest.newBuilder().build();
        CLIENT_MANAGER.forwardMessageToDeadLetterQueue(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        CLIENT_MANAGER.forwardMessageToDeadLetterQueue(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testEndTransaction() {
        Metadata metadata = new Metadata();
        EndTransactionRequest request = EndTransactionRequest.newBuilder().build();
        CLIENT_MANAGER.endTransaction(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        CLIENT_MANAGER.endTransaction(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testNotifyClientTermination() {
        Metadata metadata = new Metadata();
        NotifyClientTerminationRequest request = NotifyClientTerminationRequest.newBuilder().build();
        CLIENT_MANAGER.notifyClientTermination(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        CLIENT_MANAGER.notifyClientTermination(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }
}