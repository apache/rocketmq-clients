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

package org.apache.rocketmq.client.java.impl.consumer;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Metadata;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.RequestIdGenerator;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.apache.rocketmq.client.java.rpc.Signature;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProcessQueueImplTest extends TestBase {
    @Mock
    private PushConsumerImpl pushConsumer;
    @Mock
    private ConsumeService consumeService;
    @Mock
    private PushSubscriptionSettings pushSubscriptionSettings;
    @Mock
    private RetryPolicy retryPolicy;

    private AtomicLong consumptionOkQuantity;

    private final FilterExpression filterExpression = FilterExpression.SUB_ALL;

    private ProcessQueueImpl processQueue;

    @Before
    public void setup() throws IllegalAccessException, NoSuchFieldException {
        this.processQueue = new ProcessQueueImpl(pushConsumer, fakeMessageQueueImpl0(), filterExpression);
        when(pushConsumer.isRunning()).thenReturn(true);

        this.consumptionOkQuantity = new AtomicLong(0);
        Field field0 = PushConsumerImpl.class.getDeclaredField("consumptionOkQuantity");
        field0.setAccessible(true);
        field0.set(pushConsumer, consumptionOkQuantity);

        AtomicLong consumptionErrorQuantity = new AtomicLong(0);
        Field field1 = PushConsumerImpl.class.getDeclaredField("consumptionErrorQuantity");
        field1.setAccessible(true);
        field1.set(pushConsumer, consumptionErrorQuantity);

        when(pushConsumer.getPushConsumerSettings()).thenReturn(pushSubscriptionSettings);
        when(pushConsumer.getScheduler()).thenReturn(SCHEDULER);

        AtomicLong receivedMessagesQuantity = new AtomicLong(0);
        when(pushConsumer.getReceivedMessagesQuantity()).thenReturn(receivedMessagesQuantity);
        when(pushConsumer.getConsumeService()).thenReturn(consumeService);
    }

    @Test
    public void testExpired() {
        when(pushSubscriptionSettings.getLongPollingTimeout()).thenReturn(Duration.ofSeconds(3));
        when(pushConsumer.getClientConfiguration()).thenReturn(ClientConfiguration.newBuilder()
            .setEndpoints(FAKE_ENDPOINTS).build());
        assertFalse(processQueue.expired());
    }

    @Test
    public void testIsCacheFull() {
        when(pushConsumer.cacheMessageCountThresholdPerQueue()).thenReturn(8);
        when(pushConsumer.cacheMessageBytesThresholdPerQueue()).thenReturn(1024);
        assertFalse(processQueue.isCacheFull());
    }

    @Test
    public void testReceiveMessageImmediately() {
        final int cachedMessagesCountThresholdPerQueue = 8;
        when(pushConsumer.cacheMessageCountThresholdPerQueue()).thenReturn(cachedMessagesCountThresholdPerQueue);
        final int cachedMessageBytesThresholdPerQueue = 1024;
        when(pushConsumer.cacheMessageBytesThresholdPerQueue()).thenReturn(cachedMessageBytesThresholdPerQueue);
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl();
        messageViewList.add(messageView);
        final Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(Signature.REQUEST_ID_KEY, Metadata.ASCII_STRING_MARSHALLER),
            RequestIdGenerator.getInstance().next());
        ReceiveMessageResult receiveMessageResult = new ReceiveMessageResult(fakeEndpoints(), messageViewList);
        SettableFuture<ReceiveMessageResult> future0 = SettableFuture.create();
        future0.set(receiveMessageResult);
        when(pushConsumer.receiveMessage(any(ReceiveMessageRequest.class), any(MessageQueueImpl.class),
            any(Duration.class))).thenReturn(future0);
        when(pushSubscriptionSettings.getReceiveBatchSize()).thenReturn(32);
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder().build();
        when(pushConsumer.wrapReceiveMessageRequest(anyInt(), any(MessageQueueImpl.class),
            any(FilterExpression.class))).thenReturn(request);
        processQueue.fetchMessageImmediately();
        await().atMost(Duration.ofSeconds(3))
            .untilAsserted(() -> verify(pushConsumer, times(cachedMessagesCountThresholdPerQueue))
                .receiveMessage(any(ReceiveMessageRequest.class), any(MessageQueueImpl.class), any(Duration.class)));
    }

    @Test
    public void testEraseMessageWithConsumeOk() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl(2, false);
        messageViewList.add(messageView);
        processQueue.cacheMessages(messageViewList);
        RpcFuture<AckMessageRequest, AckMessageResponse> future0 = okAckMessageResponseFuture();
        when(pushConsumer.ackMessage(any(MessageViewImpl.class))).thenReturn(future0);
        processQueue.eraseMessage(messageView, ConsumeResult.SUCCESS);
        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> verify(pushConsumer, times(1))
            .ackMessage(eq(messageView)));
    }

    @Test
    public void testEraseMessageWithAckFailure() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl(2, false);
        messageViewList.add(messageView);
        processQueue.cacheMessages(messageViewList);
        RpcFuture<AckMessageRequest, AckMessageResponse> future0 = new RpcFuture<>(new Exception());
        when(pushConsumer.ackMessage(any(MessageViewImpl.class))).thenReturn(future0);
        processQueue.eraseMessage(messageView, ConsumeResult.SUCCESS);
        int ackTimes = 3;
        final Duration tolerance = Duration.ofMillis(500);
        await().atMost(ProcessQueueImpl.ACK_MESSAGE_FAILURE_BACKOFF_DELAY.multipliedBy(ackTimes)
            .plus(tolerance)).untilAsserted(() -> verify(pushConsumer, times(ackTimes))
            .ackMessage(eq(messageView)));
    }

    @Test
    public void testEraseMessageWithChangingInvisibleDurationFailure() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl(2, false);
        messageViewList.add(messageView);
        processQueue.cacheMessages(messageViewList);
        RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> future0 =
            new RpcFuture<>(new Exception());
        when(pushConsumer.changeInvisibleDuration(any(MessageViewImpl.class), any(Duration.class))).thenReturn(future0);
        when(pushConsumer.getRetryPolicy()).thenReturn(retryPolicy);
        when(retryPolicy.getNextAttemptDelay(anyInt())).thenReturn(Duration.ofSeconds(1));
        processQueue.eraseMessage(messageView, ConsumeResult.FAILURE);
        int ackTimes = 3;
        final Duration tolerance = Duration.ofMillis(500);
        await().atMost(ProcessQueueImpl.CHANGE_INVISIBLE_DURATION_FAILURE_BACKOFF_DELAY.multipliedBy(ackTimes)
            .plus(tolerance)).untilAsserted(() -> verify(pushConsumer, times(ackTimes))
            .changeInvisibleDuration(any(MessageViewImpl.class), any(Duration.class)));
    }

    @Test
    public void testEraseFifoMessageWithConsumeOk() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl(2, false);
        messageViewList.add(messageView);
        processQueue.cacheMessages(messageViewList);
        RpcFuture<AckMessageRequest, AckMessageResponse> future0 = okAckMessageResponseFuture();
        when(pushConsumer.ackMessage(any(MessageViewImpl.class))).thenReturn(future0);
        when(pushConsumer.getRetryPolicy()).thenReturn(retryPolicy);
        when(retryPolicy.getMaxAttempts()).thenReturn(1);
        when(pushConsumer.getConsumptionExecutor()).thenReturn(SINGLE_THREAD_POOL_EXECUTOR);
        final ListenableFuture<Void> future = processQueue.eraseFifoMessage(messageView, ConsumeResult.SUCCESS);
        future.addListener(() -> verify(pushConsumer, times(1))
            .ackMessage(any(MessageViewImpl.class)), MoreExecutors.directExecutor());
    }

    @Test
    public void testEraseFifoMessageWithConsumeError() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl(2, false);
        messageViewList.add(messageView);
        processQueue.cacheMessages(messageViewList);
        RpcFuture<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse> future0 =
            okForwardMessageToDeadLetterQueueResponseFuture();
        when(pushConsumer.forwardMessageToDeadLetterQueue(any(MessageViewImpl.class))).thenReturn(future0);
        when(pushConsumer.getRetryPolicy()).thenReturn(retryPolicy);
        when(retryPolicy.getMaxAttempts()).thenReturn(1);
        when(pushConsumer.getConsumptionExecutor()).thenReturn(SINGLE_THREAD_POOL_EXECUTOR);
        final ListenableFuture<Void> future = processQueue.eraseFifoMessage(messageView, ConsumeResult.FAILURE);
        future.addListener(() -> verify(pushConsumer, times(1))
            .forwardMessageToDeadLetterQueue(any(MessageViewImpl.class)), MoreExecutors.directExecutor());
    }

    @Test
    public void testEraseFifoMessageWithConsumeErrorInFirstAttempt() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl(2, false);
        messageViewList.add(messageView);
        processQueue.cacheMessages(messageViewList);
        RpcFuture<AckMessageRequest, AckMessageResponse> future0 = okAckMessageResponseFuture();
        when(pushConsumer.ackMessage(any(MessageViewImpl.class))).thenReturn(future0);
        when(pushConsumer.getRetryPolicy()).thenReturn(retryPolicy);
        when(retryPolicy.getMaxAttempts()).thenReturn(2);
        when(pushConsumer.getConsumptionExecutor()).thenReturn(SINGLE_THREAD_POOL_EXECUTOR);
        SettableFuture<ConsumeResult> consumeFuture = SettableFuture.create();
        consumeFuture.set(ConsumeResult.SUCCESS);
        when(consumeService.consume(any(MessageViewImpl.class), any(Duration.class))).thenReturn(consumeFuture);
        when(pushConsumer.getConsumeService()).thenReturn(consumeService);
        final ListenableFuture<Void> future = processQueue.eraseFifoMessage(messageView, ConsumeResult.FAILURE);
        future.addListener(() -> verify(pushConsumer, times(1))
            .ackMessage(any(MessageViewImpl.class)), MoreExecutors.directExecutor());
    }

    @Test
    public void testEraseFifoMessageWithForwardingMessageToDeadLetterQueueFailure() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl(2, false);
        messageViewList.add(messageView);
        processQueue.cacheMessages(messageViewList);
        RpcFuture<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse> future0 =
            forwardMessageToDeadLetterQueueResponseFuture(Code.PROXY_TIMEOUT);
        when(pushConsumer.forwardMessageToDeadLetterQueue(any(MessageViewImpl.class))).thenReturn(future0);
        when(pushConsumer.getRetryPolicy()).thenReturn(retryPolicy);
        when(retryPolicy.getMaxAttempts()).thenReturn(1);
        when(pushConsumer.getConsumptionExecutor()).thenReturn(SINGLE_THREAD_POOL_EXECUTOR);
        processQueue.eraseFifoMessage(messageView, ConsumeResult.FAILURE);
        int forwardingToDeadLetterQueueTimes = 3;
        final Duration tolerance = Duration.ofMillis(500);
        await().atMost(ProcessQueueImpl.FORWARD_FIFO_MESSAGE_TO_DLQ_FAILURE_BACKOFF_DELAY
                .multipliedBy(forwardingToDeadLetterQueueTimes).plus(tolerance))
            .untilAsserted(() -> verify(pushConsumer, times(forwardingToDeadLetterQueueTimes))
                .forwardMessageToDeadLetterQueue(any(MessageViewImpl.class)));
    }

    @Test
    public void testEraseFifoMessageWithForwardingMessageToDeadLetterQueueException() {
        List<MessageViewImpl> messageViewList = new ArrayList<>();
        final MessageViewImpl messageView = fakeMessageViewImpl(2, false);
        messageViewList.add(messageView);
        processQueue.cacheMessages(messageViewList);
        RpcFuture<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse> future0 =
            new RpcFuture<>(new Exception());
        when(pushConsumer.forwardMessageToDeadLetterQueue(any(MessageViewImpl.class))).thenReturn(future0);
        when(pushConsumer.getRetryPolicy()).thenReturn(retryPolicy);
        when(retryPolicy.getMaxAttempts()).thenReturn(1);
        when(pushConsumer.getConsumptionExecutor()).thenReturn(SINGLE_THREAD_POOL_EXECUTOR);
        processQueue.eraseFifoMessage(messageView, ConsumeResult.FAILURE);
        int forwardingToDeadLetterQueueTimes = 3;
        final Duration tolerance = Duration.ofMillis(500);
        await().atMost(ProcessQueueImpl.FORWARD_FIFO_MESSAGE_TO_DLQ_FAILURE_BACKOFF_DELAY
                .multipliedBy(forwardingToDeadLetterQueueTimes).plus(tolerance))
            .untilAsserted(() -> verify(pushConsumer, times(forwardingToDeadLetterQueueTimes))
                .forwardMessageToDeadLetterQueue(any(MessageViewImpl.class)));
    }
}