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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.ConsumerGroup;
import apache.rocketmq.v1.FilterExpression;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.MultiplexingRequest;
import apache.rocketmq.v1.MultiplexingResponse;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.ResponseCommon;
import apache.rocketmq.v1.SubscriptionEntry;
import apache.rocketmq.v1.VerifyMessageConsumptionResponse;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Metadata;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.ConsumeContext;
import org.apache.rocketmq.client.consumer.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.filter.ExpressionType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.impl.ClientManager;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.tools.TestBase;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PushConsumerImplTest extends TestBase {
    @Mock
    private ClientManager clientManager;

    @Mock
    private ConsumeService consumeService;

    @InjectMocks
    private PushConsumerImpl consumerImpl;

    @BeforeMethod
    public void beforeMethod() throws ClientException {
        this.consumerImpl = new PushConsumerImpl(FAKE_GROUP_0);
        consumerImpl.setNamesrvAddr(FAKE_NAME_SERVER_ADDR_0);
        consumerImpl.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeStatus consume(List<MessageExt> messages, ConsumeContext context) {
                return null;
            }
        });
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void setOffsetStoreWithNull() {
        try {
            consumerImpl.setOffsetStore(null);
            fail();
        } catch (NullPointerException ignore) {
            // Ignore on purpose.
        }
    }

    @Test
    public void testScanAssignments() throws UnsupportedEncodingException {
        consumerImpl.subscribe(FAKE_TOPIC_0, "*", ExpressionType.TAG);
        when(clientManager.queryRoute(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                      ArgumentMatchers.<QueryRouteRequest>any(), anyLong(),
                                      ArgumentMatchers.<TimeUnit>any())).thenReturn(okQueryRouteResponseFuture());
        when(clientManager.queryAssignment(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                           ArgumentMatchers.<QueryAssignmentRequest>any(), anyLong(),
                                           ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(okQueryAssignmentResponseFuture());

        final long delayMillis = 50;
        when(clientManager.multiplexingCall(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                            ArgumentMatchers.<MultiplexingRequest>any(), anyLong(),
                                            ArgumentMatchers.<TimeUnit>any()))
                .thenAnswer(new Answer<ListenableFuture<MultiplexingResponse>>() {
                    @Override
                    public ListenableFuture<MultiplexingResponse> answer(InvocationOnMock invocation) {
                        return multiplexingResponseWithGenericPollingFuture(delayMillis);
                    }
                });

        final Status status = Status.newBuilder().setCode(Code.OK_VALUE).build();
        final ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
        final ReceiveMessageResponse response =
                ReceiveMessageResponse.newBuilder().setCommon(common).addMessages(fakePbMessage0()).build();
        final SettableFuture<ReceiveMessageResponse> future0 = SettableFuture.create();
        when(clientManager.receiveMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                          ArgumentMatchers.<ReceiveMessageRequest>any(), anyLong(),
                                          ArgumentMatchers.<TimeUnit>any()))
                .thenAnswer(new Answer<ListenableFuture<ReceiveMessageResponse>>() {
                    @Override
                    public ListenableFuture<ReceiveMessageResponse> answer(InvocationOnMock invocation) {
                        SCHEDULER.schedule(new Runnable() {
                            @Override
                            public void run() {
                                future0.set(response);
                            }
                        }, delayMillis, TimeUnit.MILLISECONDS);
                        return future0;
                    }
                });

        consumerImpl.scanAssignments();
        verify(clientManager, times(1)).queryRoute(ArgumentMatchers.<Endpoints>any(),
                                                   ArgumentMatchers.<Metadata>any(),
                                                   ArgumentMatchers.<QueryRouteRequest>any(), anyLong(),
                                                   ArgumentMatchers.<TimeUnit>any());
        verify(clientManager, times(1)).queryAssignment(ArgumentMatchers.<Endpoints>any(),
                                                        ArgumentMatchers.<Metadata>any(),
                                                        ArgumentMatchers.<QueryAssignmentRequest>any(),
                                                        anyLong(), ArgumentMatchers.<TimeUnit>any());
        verify(clientManager, times(1)).receiveMessage(ArgumentMatchers.<Endpoints>any(),
                                                       ArgumentMatchers.<Metadata>any(),
                                                       ArgumentMatchers.<ReceiveMessageRequest>any(),
                                                       anyLong(), ArgumentMatchers.<TimeUnit>any());
    }

    @Test
    public void testAckMessage() throws ExecutionException, InterruptedException {
        when(clientManager.ackMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                      ArgumentMatchers.<AckMessageRequest>any(), anyLong(),
                                      ArgumentMatchers.<TimeUnit>any())).thenReturn(okAckMessageResponseFuture());
        ListenableFuture<AckMessageResponse> future = consumerImpl.ackMessage(fakeMessageExt());
        final AckMessageResponse ackMessageResponse = future.get();
        assertEquals(ackMessageResponse.getCommon().getStatus().getCode(), Code.OK_VALUE);

        final Throwable throwable = new RuntimeException();
        when(clientManager.ackMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                      ArgumentMatchers.<AckMessageRequest>any(), anyLong(),
                                      ArgumentMatchers.<TimeUnit>any())).thenThrow(throwable);
        future = consumerImpl.ackMessage(fakeMessageExt());
        try {
            future.get();
            fail();
        } catch (Throwable t) {
            assertEquals(t.getCause(), throwable);
        }
    }

    @Test
    public void testForwardMessageToDeadLetterQueue() throws ExecutionException, InterruptedException {
        when(clientManager.forwardMessageToDeadLetterQueue(ArgumentMatchers.<Endpoints>any(),
                                                           ArgumentMatchers.<Metadata>any(),
                                                           ArgumentMatchers
                                                                   .<ForwardMessageToDeadLetterQueueRequest>any(),
                                                           anyLong(),
                                                           ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(okForwardMessageToDeadLetterQueueResponseListenableFuture());
        ListenableFuture<ForwardMessageToDeadLetterQueueResponse> future = consumerImpl
                .forwardMessageToDeadLetterQueue(fakeMessageExt());
        final ForwardMessageToDeadLetterQueueResponse response = future.get();
        assertEquals(response.getCommon().getStatus().getCode(), Code.OK_VALUE);

        final Throwable throwable = new RuntimeException();
        when(clientManager.forwardMessageToDeadLetterQueue(ArgumentMatchers.<Endpoints>any(),
                                                           ArgumentMatchers.<Metadata>any(),
                                                           ArgumentMatchers
                                                                   .<ForwardMessageToDeadLetterQueueRequest>any(),
                                                           anyLong(),
                                                           ArgumentMatchers.<TimeUnit>any())).thenThrow(throwable);
        future = consumerImpl.forwardMessageToDeadLetterQueue(fakeMessageExt());
        try {
            future.get();
            fail();
        } catch (Throwable t) {
            assertEquals(t.getCause(), throwable);
        }
    }

    @Test
    public void testNackMessage() throws ExecutionException, InterruptedException {
        when(clientManager.nackMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                       ArgumentMatchers.<NackMessageRequest>any(), anyLong(),
                                       ArgumentMatchers.<TimeUnit>any())).thenReturn(okNackMessageResponseFuture());
        ListenableFuture<NackMessageResponse> future = consumerImpl.nackMessage(fakeMessageExt());
        final NackMessageResponse nackMessageResponse = future.get();
        assertEquals(nackMessageResponse.getCommon().getStatus().getCode(), Code.OK_VALUE);

        final Throwable throwable = new RuntimeException();
        when(clientManager.nackMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                       ArgumentMatchers.<NackMessageRequest>any(), anyLong(),
                                       ArgumentMatchers.<TimeUnit>any())).thenThrow(throwable);
        future = consumerImpl.nackMessage(fakeMessageExt());
        try {
            future.get();
            fail();
        } catch (Throwable t) {
            assertEquals(t.getCause(), throwable);
        }
    }

    @Test
    public void testVerifyConsumption() throws UnsupportedEncodingException, ExecutionException, InterruptedException {
        when(clientManager.nackMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                       ArgumentMatchers.<NackMessageRequest>any(), anyLong(),
                                       ArgumentMatchers.<TimeUnit>any())).thenReturn(okNackMessageResponseFuture());
        SettableFuture<ConsumeStatus> okFuture = SettableFuture.create();
        okFuture.set(ConsumeStatus.OK);
        when(consumeService.consume(ArgumentMatchers.<MessageExt>any())).thenReturn(okFuture);
        ListenableFuture<VerifyMessageConsumptionResponse> future =
                consumerImpl.verifyConsumption(fakeVerifyMessageConsumptionRequest());
        assertEquals(future.get().getCommon().getStatus().getCode(), Code.OK_VALUE);

        SettableFuture<ConsumeStatus> errorFuture = SettableFuture.create();
        errorFuture.set(ConsumeStatus.ERROR);
        when(consumeService.consume(ArgumentMatchers.<MessageExt>any())).thenReturn(errorFuture);
        future = consumerImpl.verifyConsumption(fakeVerifyMessageConsumptionRequest());
        assertEquals(future.get().getCommon().getStatus().getCode(), Code.ABORTED_VALUE);
    }

    @Test
    public void testPrepareHeartbeatData() {
        consumerImpl.subscribe(FAKE_TOPIC_0, "*", ExpressionType.TAG);
        HeartbeatEntry heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(heartbeatEntry.getClientId(), consumerImpl.getClientId());
        final ConsumerGroup consumerGroup = heartbeatEntry.getConsumerGroup();
        final Resource group = consumerGroup.getGroup();
        assertEquals(group.getName(), consumerImpl.getGroup());
        assertEquals(group.getArn(), consumerImpl.getArn());
        final List<SubscriptionEntry> subscriptionsList = consumerGroup.getSubscriptionsList();
        assertEquals(1, subscriptionsList.size());
        final SubscriptionEntry subscriptionEntry = subscriptionsList.get(0);
        final Resource topicResource = subscriptionEntry.getTopic();
        assertEquals(FAKE_TOPIC_0, topicResource.getName());
        final FilterExpression expression = subscriptionEntry.getExpression();
        assertEquals(expression.getType(), FilterType.TAG);

        consumerImpl.setMessageModel(MessageModel.BROADCASTING);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumeModel.BROADCASTING, heartbeatEntry.getConsumerGroup().getConsumeModel());

        consumerImpl.setMessageModel(MessageModel.CLUSTERING);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumeModel.CLUSTERING, heartbeatEntry.getConsumerGroup().getConsumeModel());

        consumerImpl.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumePolicy.PLAYBACK, heartbeatEntry.getConsumerGroup().getConsumePolicy());

        consumerImpl.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_MAX_OFFSET);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumePolicy.DISCARD, heartbeatEntry.getConsumerGroup().getConsumePolicy());

        consumerImpl.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumePolicy.TARGET_TIMESTAMP, heartbeatEntry.getConsumerGroup().getConsumePolicy());

        consumerImpl.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertEquals(ConsumePolicy.RESUME, heartbeatEntry.getConsumerGroup().getConsumePolicy());

        consumerImpl.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeStatus consume(List<MessageExt> messages, ConsumeContext context) {
                return null;
            }
        });
        heartbeatEntry = consumerImpl.prepareHeartbeatData();
        assertTrue(heartbeatEntry.getNeedRebalance());
    }
}
