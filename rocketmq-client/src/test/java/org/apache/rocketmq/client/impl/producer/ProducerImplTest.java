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

package org.apache.rocketmq.client.impl.producer;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.MessageType;
import apache.rocketmq.v1.MultiplexingRequest;
import apache.rocketmq.v1.MultiplexingResponse;
import apache.rocketmq.v1.Permission;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.ResolveOrphanedTransactionRequest;
import apache.rocketmq.v1.SendMessageRequest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Metadata;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.ClientManager;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionChecker;
import org.apache.rocketmq.client.producer.TransactionResolution;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.tools.TestBase;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ProducerImplTest extends TestBase {
    @Mock
    private ClientManager clientManager;

    @InjectMocks
    private final ProducerImpl producerImpl = new ProducerImpl(dummyGroup0);

    @BeforeTest
    public void beforeTest() throws ClientException {
        producerImpl.setNamesrvAddr(dummyNameServerAddr0);
        producerImpl.setMessageTracingEnabled(false);
        producerImpl.start();
    }

    @AfterTest
    public void afterTest() throws InterruptedException {
        producerImpl.shutdown();
    }

    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);
        when(clientManager.getScheduler()).thenReturn(scheduler());
    }

    @AfterMethod
    public void afterMethod() {
    }

    @Test
    public void testSend() throws ServerException, ClientException, InterruptedException, TimeoutException {
        final Message message = dummyMessage();
        producerImpl.onTopicRouteDataUpdate0(message.getTopic(), dummyTopicRouteData(Permission.READ_WRITE));
        when(clientManager.sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                       ArgumentMatchers.<SendMessageRequest>any(), anyLong(),
                                       ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(successSendMessageResponse());
        final SendResult sendResult = producerImpl.send(message);
        ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(clientManager, times(1)).sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                                    requestCaptor.capture(), anyLong(),
                                                    ArgumentMatchers.<TimeUnit>any());
        final SendMessageRequest request = requestCaptor.getValue();
        assertEquals(request.getMessage().getSystemAttribute().getMessageType(), MessageType.NORMAL);
        assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
    }

    @Test
    public void testSendWithLargeMessage() throws ServerException, ClientException, InterruptedException,
                                                  TimeoutException {
        final int messageBodySize = ProducerImpl.MESSAGE_COMPRESSION_THRESHOLD + 1;
        final Message message = dummyMessage(messageBodySize);
        producerImpl.onTopicRouteDataUpdate0(message.getTopic(), dummyTopicRouteData(Permission.READ_WRITE));
        when(clientManager.sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                       ArgumentMatchers.<SendMessageRequest>any(), anyLong(),
                                       ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(successSendMessageResponse());
        final SendResult sendResult = producerImpl.send(message);
        ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(clientManager, times(1)).sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                                    requestCaptor.capture(), anyLong(),
                                                    ArgumentMatchers.<TimeUnit>any());
        final SendMessageRequest request = requestCaptor.getValue();
        assertEquals(request.getMessage().getSystemAttribute().getMessageType(), MessageType.NORMAL);
        assertTrue(messageBodySize > request.getMessage().getBody().size());
        assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
    }

    @Test
    public void testSendWithNoPermission() throws ServerException, InterruptedException, TimeoutException {
        final Message message = dummyMessage();
        producerImpl.onTopicRouteDataUpdate0(message.getTopic(), dummyTopicRouteData(Permission.NONE));
        when(clientManager.sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                       ArgumentMatchers.<SendMessageRequest>any(), anyLong(),
                                       ArgumentMatchers.<TimeUnit>any())).thenReturn(successSendMessageResponse());
        try {
            producerImpl.send(message);
            fail();
        } catch (ClientException e) {
            assertEquals(e.getErrorCode(), ErrorCode.NO_PERMISSION);
            verify(clientManager, never())
                    .sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                 ArgumentMatchers.<SendMessageRequest>any(), anyLong(),
                                 ArgumentMatchers.<TimeUnit>any());
        }
    }

    @Test
    public void testSendWithFifoMessage() throws ServerException, ClientException, InterruptedException,
                                                 TimeoutException {
        final Message message = dummyFifoMessage();
        producerImpl.onTopicRouteDataUpdate0(message.getTopic(), dummyTopicRouteData(Permission.READ_WRITE));
        when(clientManager.sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                       ArgumentMatchers.<SendMessageRequest>any(), anyLong(),
                                       ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(successSendMessageResponse());
        final SendResult sendResult = producerImpl.send(message);
        ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(clientManager, times(1)).sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                                    requestCaptor.capture(), anyLong(),
                                                    ArgumentMatchers.<TimeUnit>any());
        final SendMessageRequest request = requestCaptor.getValue();
        assertEquals(request.getMessage().getSystemAttribute().getMessageType(), MessageType.FIFO);
        assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
    }

    @Test
    public void testSendWithDelayMessage() throws ServerException, ClientException, InterruptedException,
                                                  TimeoutException {
        final Message message = dummyDelayMessage();
        producerImpl.onTopicRouteDataUpdate0(message.getTopic(), dummyTopicRouteData(Permission.READ_WRITE));
        when(clientManager.sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                       ArgumentMatchers.<SendMessageRequest>any(), anyLong(),
                                       ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(successSendMessageResponse());
        final SendResult sendResult = producerImpl.send(message);
        ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(clientManager, times(1)).sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                                    requestCaptor.capture(), anyLong(),
                                                    ArgumentMatchers.<TimeUnit>any());
        final SendMessageRequest request = requestCaptor.getValue();
        assertEquals(request.getMessage().getSystemAttribute().getMessageType(), MessageType.DELAY);
        assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
    }

    @Test
    public void testSendWithTransactionMessage() throws ServerException, ClientException, InterruptedException,
                                                        TimeoutException {
        final Message message = dummyTransactionMessage();
        producerImpl.onTopicRouteDataUpdate0(message.getTopic(), dummyTopicRouteData(Permission.READ_WRITE));
        when(clientManager.sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                       ArgumentMatchers.<SendMessageRequest>any(), anyLong(),
                                       ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(successSendMessageResponse());
        final SendResult sendResult = producerImpl.send(message);
        ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(clientManager, times(1)).sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                                    requestCaptor.capture(), anyLong(),
                                                    ArgumentMatchers.<TimeUnit>any());
        final SendMessageRequest request = requestCaptor.getValue();
        assertEquals(request.getMessage().getSystemAttribute().getMessageType(), MessageType.TRANSACTION);
        assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
    }

    @Test
    public void testSendWithCallback() throws ClientException, InterruptedException, ExecutionException {
        final Message message = dummyMessage();
        producerImpl.onTopicRouteDataUpdate0(message.getTopic(), dummyTopicRouteData(Permission.READ_WRITE));
        when(clientManager.sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                       ArgumentMatchers.<SendMessageRequest>any(), anyLong(),
                                       ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(successSendMessageResponse());
        // no custom callback executor.
        final SettableFuture<SendResult> future0 = SettableFuture.create();
        producerImpl.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                future0.set(sendResult);
            }

            @Override
            public void onException(Throwable e) {
                future0.setException(e);
            }
        });
        final SendResult sendResult = future0.get();
        assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
        // custom callback executor.
        producerImpl.setCallbackExecutor(sendCallbackExecutor());
        final SettableFuture<SendResult> future1 = SettableFuture.create();
        producerImpl.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                future1.set(sendResult);
            }

            @Override
            public void onException(Throwable e) {
                future1.setException(e);
            }
        });
        assertEquals(future1.get().getSendStatus(), SendStatus.SEND_OK);
    }

    @Test
    public void testCommit() throws ServerException, ClientException, InterruptedException, TimeoutException {
        when(clientManager.endTransaction(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                          ArgumentMatchers.<EndTransactionRequest>any(), anyLong(),
                                          ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(successEndTransactionResponse());
        final Endpoints endpoints = new Endpoints(dummyProtoEndpoints0());
        producerImpl.commit(endpoints, dummyMessageExt(), dummyTransactionId);
        verify(clientManager, times(1)).endTransaction(ArgumentMatchers.<Endpoints>any(),
                                                       ArgumentMatchers.<Metadata>any(),
                                                       ArgumentMatchers.<EndTransactionRequest>any(),
                                                       anyLong(),
                                                       ArgumentMatchers.<TimeUnit>any());
    }

    @Test
    public void testRollback() throws ServerException, ClientException, InterruptedException, TimeoutException {
        when(clientManager.endTransaction(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                          ArgumentMatchers.<EndTransactionRequest>any(), anyLong(),
                                          ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(successEndTransactionResponse());
        final Endpoints endpoints = new Endpoints(dummyProtoEndpoints0());
        producerImpl.rollback(endpoints, dummyMessageExt(), dummyTransactionId);
        verify(clientManager, times(1)).endTransaction(ArgumentMatchers.<Endpoints>any(),
                                                       ArgumentMatchers.<Metadata>any(),
                                                       ArgumentMatchers.<EndTransactionRequest>any(),
                                                       anyLong(),
                                                       ArgumentMatchers.<TimeUnit>any());
    }

    @Test
    public void testResolveOrphanedTransaction() throws UnsupportedEncodingException, InterruptedException {
        ResolveOrphanedTransactionRequest request =
                ResolveOrphanedTransactionRequest.newBuilder().setTransactionId(dummyTransactionId)
                                                 .setOrphanedTransactionalMessage(dummyTransactionMessage0()).build();
        final Endpoints endpoints = new Endpoints(dummyProtoEndpoints0());
        {
            producerImpl.resolveOrphanedTransaction(endpoints, request);
            verify(clientManager, never()).endTransaction(ArgumentMatchers.<Endpoints>any(),
                                                          ArgumentMatchers.<Metadata>any(),
                                                          ArgumentMatchers.<EndTransactionRequest>any(),
                                                          anyLong(),
                                                          ArgumentMatchers.<TimeUnit>any());
        }
        // checker returns null;
        {
            producerImpl.setTransactionChecker(new TransactionChecker() {
                @Override
                public TransactionResolution check(MessageExt msg) {
                    return null;
                }
            });
            producerImpl.resolveOrphanedTransaction(endpoints, request);
            verify(clientManager, never()).endTransaction(ArgumentMatchers.<Endpoints>any(),
                                                          ArgumentMatchers.<Metadata>any(),
                                                          ArgumentMatchers.<EndTransactionRequest>any(),
                                                          anyLong(),
                                                          ArgumentMatchers.<TimeUnit>any());
        }
        // checker returns unknown.
        {
            producerImpl.setTransactionChecker(new TransactionChecker() {
                @Override
                public TransactionResolution check(MessageExt msg) {
                    return TransactionResolution.UNKNOWN;
                }
            });
            producerImpl.resolveOrphanedTransaction(endpoints, request);
            verify(clientManager, never()).endTransaction(ArgumentMatchers.<Endpoints>any(),
                                                          ArgumentMatchers.<Metadata>any(),
                                                          ArgumentMatchers.<EndTransactionRequest>any(),
                                                          anyLong(),
                                                          ArgumentMatchers.<TimeUnit>any());
        }
        // checker returns commit.
        {
            when(clientManager.endTransaction(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                              ArgumentMatchers.<EndTransactionRequest>any(), anyLong(),
                                              ArgumentMatchers.<TimeUnit>any()))
                    .thenReturn(successEndTransactionResponse());
            producerImpl.setTransactionChecker(new TransactionChecker() {
                @Override
                public TransactionResolution check(MessageExt msg) {
                    return TransactionResolution.COMMIT;
                }
            });
            producerImpl.resolveOrphanedTransaction(endpoints, request);
            Thread.sleep(50);
            verify(clientManager, times(1)).endTransaction(ArgumentMatchers.<Endpoints>any(),
                                                           ArgumentMatchers.<Metadata>any(),
                                                           ArgumentMatchers.<EndTransactionRequest>any(),
                                                           anyLong(),
                                                           ArgumentMatchers.<TimeUnit>any());
        }
    }

    @Test
    public void testMultiplexingRequest() throws InterruptedException {
        final Message message = dummyMessage();
        when(clientManager.queryRoute(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                      ArgumentMatchers.<QueryRouteRequest>any(), anyLong(),
                                      ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(successQueryRouteResponse());
        final long delayTimeMillis = 2000;
        int multiplexingTimes = 2;
        when(clientManager.multiplexingCall(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                            ArgumentMatchers.<MultiplexingRequest>any(), anyLong(),
                                            ArgumentMatchers.<TimeUnit>any()))
                .thenAnswer(new Answer<ListenableFuture<MultiplexingResponse>>() {
                    @Override
                    public ListenableFuture<MultiplexingResponse> answer(InvocationOnMock invocation) {
                        return multiplexingResponseWithGenericPolling(delayTimeMillis, TimeUnit.MILLISECONDS);
                    }
                });
        try {
            producerImpl.send(message);
        } catch (Throwable ignore) {
            // ignore on purpose.
        }
        Thread.sleep(multiplexingTimes * delayTimeMillis - delayTimeMillis / 2);
        verify(clientManager, times(multiplexingTimes)).multiplexingCall(ArgumentMatchers.<Endpoints>any(),
                                                                         ArgumentMatchers.<Metadata>any(),
                                                                         ArgumentMatchers.<MultiplexingRequest>any(),
                                                                         anyLong(),
                                                                         ArgumentMatchers.<TimeUnit>any());
    }

    @Test
    public void testUpdateTracer() throws ServerException, ClientException, InterruptedException, TimeoutException {
        assertNull(producerImpl.getTracer());
        producerImpl.setMessageTracingEnabled(true);
        final Message message = dummyMessage();
        // fetch route from remote instead of cache.
        message.setTopic(dummyTopic1);
        when(clientManager.queryRoute(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                      ArgumentMatchers.<QueryRouteRequest>any(), anyLong(),
                                      ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(successQueryRouteResponse());
        when(clientManager.sendMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                       ArgumentMatchers.<SendMessageRequest>any(), anyLong(),
                                       ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(successSendMessageResponse());
        producerImpl.send(message);
        assertNotNull(producerImpl.getTracer());
    }
}