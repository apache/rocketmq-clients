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

package org.apache.rocketmq.client.impl;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.PollCommandRequest;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReportMessageConsumptionResultRequest;
import apache.rocketmq.v1.ReportThreadStackTraceRequest;
import apache.rocketmq.v1.SendMessageRequest;
import io.grpc.Metadata;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.tools.TestBase;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClientManagerImplTest extends TestBase {
    private ClientManagerImpl clientManager;

    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);
        this.clientManager = new ClientManagerImpl("");
    }

    @Test(description = "Expect no throwable")
    public void testQueryRoute() {
        Metadata metadata = new Metadata();
        QueryRouteRequest request = QueryRouteRequest.newBuilder().build();
        clientManager.queryRoute(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.queryRoute(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testHeartbeat() {
        Metadata metadata = new Metadata();
        HeartbeatRequest request = HeartbeatRequest.newBuilder().build();
        clientManager.heartbeat(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.heartbeat(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testSendMessage() {
        Metadata metadata = new Metadata();
        SendMessageRequest request = SendMessageRequest.newBuilder().build();
        clientManager.sendMessage(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.sendMessage(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testQueryAssignment() {
        Metadata metadata = new Metadata();
        QueryAssignmentRequest request = QueryAssignmentRequest.newBuilder().build();
        clientManager.queryAssignment(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.queryAssignment(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testReceiveMessage() {
        Metadata metadata = new Metadata();
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder().build();
        clientManager.receiveMessage(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.receiveMessage(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testAckMessage() {
        Metadata metadata = new Metadata();
        AckMessageRequest request = AckMessageRequest.newBuilder().build();
        clientManager.ackMessage(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.ackMessage(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testNackMessage() {
        Metadata metadata = new Metadata();
        NackMessageRequest request = NackMessageRequest.newBuilder().build();
        clientManager.nackMessage(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.nackMessage(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testForwardMessageToDeadLetterQueue() {
        Metadata metadata = new Metadata();
        ForwardMessageToDeadLetterQueueRequest request = ForwardMessageToDeadLetterQueueRequest.newBuilder().build();
        clientManager.forwardMessageToDeadLetterQueue(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.forwardMessageToDeadLetterQueue(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testEndTransaction() {
        Metadata metadata = new Metadata();
        EndTransactionRequest request = EndTransactionRequest.newBuilder().build();
        clientManager.endTransaction(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.endTransaction(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testQueryOffset() {
        Metadata metadata = new Metadata();
        QueryOffsetRequest request = QueryOffsetRequest.newBuilder().build();
        clientManager.queryOffset(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.queryOffset(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testPullMessage() {
        Metadata metadata = new Metadata();
        PullMessageRequest request = PullMessageRequest.newBuilder().build();
        clientManager.pullMessage(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.pullMessage(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testPollCommand() {
        Metadata metadata = new Metadata();
        PollCommandRequest request = PollCommandRequest.newBuilder().build();
        clientManager.pollCommand(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.pollCommand(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testReportThreadStackTrace() {
        Metadata metadata = new Metadata();
        final ReportThreadStackTraceRequest request = ReportThreadStackTraceRequest.newBuilder().build();
        clientManager.reportThreadStackTrace(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.reportThreadStackTrace(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testReportMessageConsumption() {
        Metadata metadata = new Metadata();
        final ReportMessageConsumptionResultRequest request =
                ReportMessageConsumptionResultRequest.newBuilder().build();
        clientManager.reportMessageConsumption(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.reportMessageConsumption(null, metadata, request, 1, TimeUnit.SECONDS);
    }

    @Test(description = "Expect no throwable")
    public void testNotifyClientTermination() {
        Metadata metadata = new Metadata();
        NotifyClientTerminationRequest request = NotifyClientTerminationRequest.newBuilder().build();
        clientManager.notifyClientTermination(fakeEndpoints0(), metadata, request, 1, TimeUnit.SECONDS);
        clientManager.notifyClientTermination(null, metadata, request, 1, TimeUnit.SECONDS);
    }
}