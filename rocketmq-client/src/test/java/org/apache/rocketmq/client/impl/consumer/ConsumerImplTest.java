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

package org.apache.rocketmq.client.impl.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import apache.rocketmq.v1.GenericPollingRequest;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.ResponseCommon;
import com.google.rpc.Code;
import com.google.rpc.Status;
import java.io.UnsupportedEncodingException;
import org.apache.rocketmq.client.consumer.PullMessageResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.ReceiveMessageResult;
import org.apache.rocketmq.client.consumer.ReceiveStatus;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.client.tools.TestBase;
import org.testng.annotations.Test;

public class ConsumerImplTest extends TestBase {
    private final ConsumerImpl consumer = new ConsumerImpl(FAKE_GROUP_0) {
        @Override
        public void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData) {
        }

        @Override
        public HeartbeatRequest wrapHeartbeatRequest() {
            return null;
        }

        @Override
        public GenericPollingRequest wrapGenericPollingRequest() {
            return null;
        }

        @Override
        public void doStats() {
        }
    };

    public ConsumerImplTest() throws ClientException {
    }

    @Test
    public void testProcessPullMessageResponseWithOk() throws UnsupportedEncodingException {
        final Status status = Status.newBuilder().setCode(Code.OK_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        PullMessageResponse response =
                PullMessageResponse.newBuilder().setCommon(common).addMessages(fakePbMessage0()).build();
        PullMessageResult result = consumer.processPullMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getPullStatus(), PullStatus.OK);
        assertFalse(result.getMessagesFound().isEmpty());
        assertEquals(result.getMessagesFound().size(), 1);
    }

    @Test
    public void testProcessPullMessageResponseWithOkEmptyResponse() {
        final Status status = Status.newBuilder().setCode(Code.OK_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        PullMessageResponse response = PullMessageResponse.newBuilder().setCommon(common).build();
        PullMessageResult result = consumer.processPullMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getPullStatus(), PullStatus.OK);
        assertTrue(result.getMessagesFound().isEmpty());
    }

    @Test
    public void testProcessPullMessageResponseWithResourceExhausted() {
        final Status status = Status.newBuilder().setCode(Code.RESOURCE_EXHAUSTED_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        PullMessageResponse response = PullMessageResponse.newBuilder().setCommon(common).build();
        PullMessageResult result = consumer.processPullMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getPullStatus(), PullStatus.RESOURCE_EXHAUSTED);
        assertTrue(result.getMessagesFound().isEmpty());
    }

    @Test
    public void testProcessPullMessageResponseWithDeadlineExceeded() {
        final Status status = Status.newBuilder().setCode(Code.DEADLINE_EXCEEDED_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        PullMessageResponse response = PullMessageResponse.newBuilder().setCommon(common).build();
        PullMessageResult result = consumer.processPullMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getPullStatus(), PullStatus.DEADLINE_EXCEEDED);
        assertTrue(result.getMessagesFound().isEmpty());
    }

    @Test
    public void testProcessPullMessageResponseWithNotFound() {
        final Status status = Status.newBuilder().setCode(Code.NOT_FOUND_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        PullMessageResponse response = PullMessageResponse.newBuilder().setCommon(common).build();
        PullMessageResult result = consumer.processPullMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getPullStatus(), PullStatus.NOT_FOUND);
        assertTrue(result.getMessagesFound().isEmpty());
    }

    @Test
    public void testProcessPullMessageResponseWithOutOfRange() {
        final Status status = Status.newBuilder().setCode(Code.OUT_OF_RANGE_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        PullMessageResponse response = PullMessageResponse.newBuilder().setCommon(common).build();
        PullMessageResult result = consumer.processPullMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getPullStatus(), PullStatus.OUT_OF_RANGE);
        assertTrue(result.getMessagesFound().isEmpty());
    }

    @Test
    public void testProcessPullMessageResponseWithInternalError() {
        final Status status = Status.newBuilder().setCode(Code.INTERNAL_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        PullMessageResponse response = PullMessageResponse.newBuilder().setCommon(common).build();
        PullMessageResult result = consumer.processPullMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getPullStatus(), PullStatus.INTERNAL);
        assertTrue(result.getMessagesFound().isEmpty());
    }

    @Test
    public void testProcessPullMessageResponseWithNullStatus() {
        final Status status = Status.newBuilder().setCode(999).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        PullMessageResponse response = PullMessageResponse.newBuilder().setCommon(common).build();
        PullMessageResult result = consumer.processPullMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getPullStatus(), PullStatus.INTERNAL);
        assertTrue(result.getMessagesFound().isEmpty());
    }

    @Test
    public void testProcessReceiveMessageResponseWithOk() throws UnsupportedEncodingException {
        final Status status = Status.newBuilder().setCode(Code.OK_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        ReceiveMessageResponse response =
                ReceiveMessageResponse.newBuilder().setCommon(common).addMessages(fakePbMessage0()).build();
        ReceiveMessageResult result = consumer.processReceiveMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getReceiveStatus(), ReceiveStatus.OK);
        assertFalse(result.getMessagesFound().isEmpty());
        assertEquals(result.getMessagesFound().size(), 1);
    }

    @Test
    public void testProcessReceiveMessageResponseWithOkEmptyResponse() {
        final Status status = Status.newBuilder().setCode(Code.OK_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        ReceiveMessageResponse response =
                ReceiveMessageResponse.newBuilder().setCommon(common).build();
        ReceiveMessageResult result = consumer.processReceiveMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getReceiveStatus(), ReceiveStatus.OK);
        assertTrue(result.getMessagesFound().isEmpty());
    }

    @Test
    public void testProcessReceiveMessageResponseWithResourceExhausted() {
        final Status status = Status.newBuilder().setCode(Code.RESOURCE_EXHAUSTED_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        ReceiveMessageResponse response = ReceiveMessageResponse.newBuilder().setCommon(common).build();
        ReceiveMessageResult result = consumer.processReceiveMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getReceiveStatus(), ReceiveStatus.RESOURCE_EXHAUSTED);
        assertTrue(result.getMessagesFound().isEmpty());
    }

    @Test
    public void testProcessReceiveMessageResponseWithDeadlineExceeded() {
        final Status status = Status.newBuilder().setCode(Code.DEADLINE_EXCEEDED_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        ReceiveMessageResponse response = ReceiveMessageResponse.newBuilder().setCommon(common).build();
        ReceiveMessageResult result = consumer.processReceiveMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getReceiveStatus(), ReceiveStatus.DEADLINE_EXCEEDED);
        assertTrue(result.getMessagesFound().isEmpty());
    }

    @Test
    public void testProcessReceiveMessageResponseWithInternalError() {
        final Status status = Status.newBuilder().setCode(Code.INTERNAL_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        ReceiveMessageResponse response = ReceiveMessageResponse.newBuilder().setCommon(common).build();
        ReceiveMessageResult result = consumer.processReceiveMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getReceiveStatus(), ReceiveStatus.INTERNAL);
        assertTrue(result.getMessagesFound().isEmpty());
    }

    @Test
    public void testProcessReceiveMessageResponseWithNullStatus() {
        final Status status = Status.newBuilder().setCode(999).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        ReceiveMessageResponse response = ReceiveMessageResponse.newBuilder().setCommon(common).build();
        ReceiveMessageResult result = consumer.processReceiveMessageResponse(fakeEndpoints0(), response);
        assertEquals(result.getReceiveStatus(), ReceiveStatus.INTERNAL);
        assertTrue(result.getMessagesFound().isEmpty());
    }
}