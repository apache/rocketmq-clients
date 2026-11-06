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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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
import apache.rocketmq.v2.Status;
import com.google.common.util.concurrent.SettableFuture;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.apis.consumer.AsyncMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.java.hook.InflightRequestCountInterceptor;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContext;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AsyncReceiveLifecycleTest extends TestBase {
    private PushConsumerImpl consumer;
    private ProcessQueueImpl processQueue;
    private ExecutorService consumptionExecutor;
    private ExecutorService receiveExecutor;
    private ExecutorService drainExecutor;
    private ScheduledExecutorService scheduler;
    private SettableFuture<ReceiveMessageResult> reception;
    private final ClientId clientId = new ClientId();

    @Before
    public void setUp() throws Exception {
        consumptionExecutor = Executors.newSingleThreadExecutor();
        receiveExecutor = Executors.newSingleThreadExecutor();
        drainExecutor = Executors.newSingleThreadExecutor();
        scheduler = Executors.newSingleThreadScheduledExecutor();
        reception = SettableFuture.create();
        consumer = mock(PushConsumerImpl.class);
        PushSubscriptionSettings settings = mock(PushSubscriptionSettings.class);
        RetryPolicy retryPolicy = mock(RetryPolicy.class);
        when(consumer.isRunning()).thenReturn(true);
        when(consumer.hasAsyncMessageListener()).thenReturn(true);
        when(consumer.beginReceiveOperation()).thenReturn(true);
        when(consumer.getClientId()).thenReturn(clientId);
        when(consumer.getConsumerGroup()).thenReturn(FAKE_CONSUMER_GROUP_0);
        when(consumer.getSettings()).thenReturn(settings);
        when(consumer.getScheduler()).thenReturn(scheduler);
        when(consumer.getConsumptionExecutor()).thenReturn(consumptionExecutor);
        when(consumer.getRetryPolicy()).thenReturn(retryPolicy);
        when(consumer.getReceptionTimes()).thenReturn(new AtomicLong());
        when(consumer.getReceivedMessagesQuantity()).thenReturn(new AtomicLong());
        when(consumer.cacheMessageCountThresholdPerQueue()).thenReturn(16);
        when(consumer.cacheMessageBytesThresholdPerQueue()).thenReturn(1024);
        when(settings.getLongPollingTimeout()).thenReturn(Duration.ofSeconds(1));
        when(settings.getReceiveBatchSize()).thenReturn(16);
        when(retryPolicy.getNextAttemptDelay(anyInt())).thenReturn(Duration.ofSeconds(1));
        setCounter("consumptionOkQuantity");
        setCounter("consumptionErrorQuantity");
        when(consumer.wrapReceiveMessageRequest(anyInt(), any(MessageQueueImpl.class),
            any(FilterExpression.class), any(Duration.class), anyString()))
            .thenReturn(ReceiveMessageRequest.newBuilder().build());
        when(consumer.receiveMessage(any(ReceiveMessageRequest.class), any(MessageQueueImpl.class),
            any(Duration.class))).thenReturn(reception);
        processQueue = spy(new ProcessQueueImpl(consumer, fakeMessageQueueImpl0(), FilterExpression.SUB_ALL));
        // Isolate one receive operation while keeping its real callback and cache handling.
        doNothing().when(processQueue).receiveMessage();
    }

    @After
    public void tearDown() throws InterruptedException {
        for (ExecutorService executor : Arrays.asList(consumptionExecutor, receiveExecutor, drainExecutor, scheduler)) {
            executor.shutdownNow();
            assertTrue("Test executor did not terminate", executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testDrainRetainsReceiveResultAcrossPostReceiveHookAndAcknowledgment() throws Exception {
        CompletableFuture<ConsumeResult> processing = new CompletableFuture<>();
        CountDownLatch listenerInvoked = new CountDownLatch(1);
        ConsumeService service = installService(false, message -> {
            listenerInvoked.countDown();
            return processing;
        });
        CountDownLatch hookEntered = new CountDownLatch(1);
        CountDownLatch releaseHook = new CountDownLatch(1);
        CountDownLatch ackSubmitted = new CountDownLatch(1);
        SettableFuture<AckMessageResponse> acknowledgment = SettableFuture.create();
        RpcFuture<AckMessageRequest, AckMessageResponse> ackRpc = new RpcFuture<>(fakeRpcContext(),
            AckMessageRequest.newBuilder().build(), acknowledgment);
        when(consumer.ackMessage(any(MessageViewImpl.class))).thenAnswer(invocation -> {
            ackSubmitted.countDown();
            return ackRpc;
        });
        InflightRequestCountInterceptor inflight = new InflightRequestCountInterceptor();
        doAnswer(invocation -> {
            inflight.doBefore(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(consumer).doBefore(any(MessageInterceptorContext.class), any());
        doAnswer(invocation -> {
            MessageInterceptorContext context = invocation.getArgument(0);
            inflight.doAfter(context, invocation.getArgument(1));
            if (context.getMessageHookPoints() == MessageHookPoints.RECEIVE) {
                hookEntered.countDown();
                assertTrue(releaseHook.await(5, TimeUnit.SECONDS));
            }
            return null;
        }).when(consumer).doAfter(any(MessageInterceptorContext.class), any());
        processQueue.fetchMessageImmediately();
        assertEquals(1, inflight.getInflightReceiveRequestCount());
        MessageViewImpl message = fakeMessageViewImpl();
        Future<?> receiving = receiveExecutor.submit(() -> reception.set(
            new ReceiveMessageResult(fakeEndpoints(), Collections.singletonList(message))));
        try {
            assertTrue(hookEntered.await(5, TimeUnit.SECONDS));
            assertEquals(0, inflight.getInflightReceiveRequestCount());
            Future<?> draining = startDrain(service);
            assertWaiting(draining);

            releaseHook.countDown();
            receiving.get(5, TimeUnit.SECONDS);
            assertTrue(listenerInvoked.await(5, TimeUnit.SECONDS));
            assertWaiting(draining);
            processing.complete(ConsumeResult.SUCCESS);
            assertTrue(ackSubmitted.await(5, TimeUnit.SECONDS));
            assertWaiting(draining);

            acknowledgment.set(AckMessageResponse.newBuilder().setStatus(okStatus()).build());
            draining.get(5, TimeUnit.SECONDS);
            assertEquals(0, processQueue.cachedMessagesCount());
            verify(consumer, times(1)).finishReceiveOperation();
        } finally {
            releaseHook.countDown();
            processing.complete(ConsumeResult.FAILURE);
            acknowledgment.set(AckMessageResponse.newBuilder().setStatus(okStatus()).build());
        }
    }

    @Test
    public void testDrainWaitsForFilteredMessageAcknowledgment() throws Exception {
        AtomicInteger listenerInvocations = new AtomicInteger();
        ConsumeService service = installService(false, message -> {
            listenerInvocations.incrementAndGet();
            return CompletableFuture.completedFuture(ConsumeResult.SUCCESS);
        });
        when(consumer.isEnableMessageInterceptorFiltering()).thenReturn(true);
        doAnswer(invocation -> {
            MessageInterceptorContext context = invocation.getArgument(0);
            if (context.getMessageHookPoints() == MessageHookPoints.RECEIVE) {
                List<GeneralMessage> messages = invocation.getArgument(1);
                messages.clear();
            }
            return null;
        }).when(consumer).doAfter(any(MessageInterceptorContext.class), any());
        SettableFuture<AckMessageResponse> acknowledgment = SettableFuture.create();
        when(consumer.ackMessage(any(MessageViewImpl.class))).thenReturn(new RpcFuture<>(fakeRpcContext(),
            AckMessageRequest.newBuilder().build(), acknowledgment));
        MessageViewImpl message = fakeMessageViewImpl();
        processQueue.fetchMessageImmediately();
        reception.set(new ReceiveMessageResult(fakeEndpoints(), Collections.singletonList(message)));
        verify(consumer, times(1)).ackMessage(message);
        assertEquals(0, listenerInvocations.get());
        assertEquals(0, processQueue.cachedMessagesCount());

        Future<?> draining = startDrain(service);
        assertWaiting(draining);
        acknowledgment.set(AckMessageResponse.newBuilder().setStatus(okStatus()).build());
        draining.get(5, TimeUnit.SECONDS);
        verify(consumer, times(1)).finishReceiveOperation();
    }

    @Test
    public void testDrainWaitsForCorruptedStandardMessageRetry() throws Exception {
        AtomicInteger listenerInvocations = new AtomicInteger();
        ConsumeService service = installService(false, message -> {
            listenerInvocations.incrementAndGet();
            return CompletableFuture.completedFuture(ConsumeResult.SUCCESS);
        });
        SettableFuture<ChangeInvisibleDurationResponse> retry = SettableFuture.create();
        when(consumer.changeInvisibleDuration(any(MessageViewImpl.class), any(Duration.class)))
            .thenReturn(new RpcFuture<>(fakeRpcContext(), ChangeInvisibleDurationRequest.newBuilder().build(), retry));
        MessageViewImpl message = fakeMessageViewImpl(true);
        processQueue.cacheMessages(Collections.singletonList(message));
        service.consume(processQueue, Collections.singletonList(message));
        assertEquals(0, listenerInvocations.get());
        assertEquals(1, processQueue.cachedMessagesCount());

        Future<?> draining = startDrain(service);
        assertWaiting(draining);
        retry.set(ChangeInvisibleDurationResponse.newBuilder().setStatus(okStatus()).build());
        draining.get(5, TimeUnit.SECONDS);
        assertEquals(0, processQueue.cachedMessagesCount());
        verify(consumer, times(1)).changeInvisibleDuration(any(MessageViewImpl.class), any(Duration.class));
    }

    @Test
    public void testDrainWaitsForCorruptedFifoMessageDeadLetterOperation() throws Exception {
        AtomicInteger listenerInvocations = new AtomicInteger();
        ConsumeService service = installService(true, message -> {
            listenerInvocations.incrementAndGet();
            return CompletableFuture.completedFuture(ConsumeResult.SUCCESS);
        });
        SettableFuture<ForwardMessageToDeadLetterQueueResponse> deadLetter = SettableFuture.create();
        when(consumer.forwardMessageToDeadLetterQueue(any(MessageViewImpl.class))).thenReturn(new RpcFuture<>(
            fakeRpcContext(), ForwardMessageToDeadLetterQueueRequest.newBuilder().build(), deadLetter));
        MessageViewImpl message = fakeMessageViewImpl(true);
        processQueue.cacheMessages(Collections.singletonList(message));
        service.consume(processQueue, Collections.singletonList(message));
        assertEquals(0, listenerInvocations.get());
        assertEquals(1, processQueue.cachedMessagesCount());

        Future<?> draining = startDrain(service);
        assertWaiting(draining);
        deadLetter.set(ForwardMessageToDeadLetterQueueResponse.newBuilder().setStatus(okStatus()).build());
        draining.get(5, TimeUnit.SECONDS);
        assertEquals(0, processQueue.cachedMessagesCount());
        verify(consumer, times(1)).forwardMessageToDeadLetterQueue(message);
    }

    @Test
    public void testReceiveAdmissionRejectsStoppedOrClosedConsumer() throws Exception {
        // Initialize only the local admission state; no client startup or network resources are needed.
        PushConsumerImpl gate = mock(PushConsumerImpl.class);
        Field admissionLock = PushConsumerImpl.class.getDeclaredField("receiveAdmissionLock");
        admissionLock.setAccessible(true);
        admissionLock.set(gate, new Object());
        doCallRealMethod().when(gate).beginReceiveOperation();
        doCallRealMethod().when(gate).finishReceiveOperation();
        when(gate.isRunning()).thenReturn(false);
        assertFalse(gate.beginReceiveOperation());

        when(gate.isRunning()).thenReturn(true);
        assertTrue(gate.beginReceiveOperation());
        Field closed = PushConsumerImpl.class.getDeclaredField("receiveAdmissionClosed");
        closed.setAccessible(true);
        closed.setBoolean(gate, true);
        assertFalse(gate.beginReceiveOperation());

        // Work admitted before closing the gate can still finish and release its drain entry.
        gate.finishReceiveOperation();
        Field admitted = PushConsumerImpl.class.getDeclaredField("admittedReceives");
        admitted.setAccessible(true);
        assertEquals(0, admitted.getLong(gate));
    }

    private ConsumeService installService(boolean fifo, AsyncMessageListener listener) {
        MessageInterceptor interceptor = mock(MessageInterceptor.class);
        ConsumeService service = fifo ? new FifoConsumeService(clientId, FAKE_CONSUMER_GROUP_0, listener,
            consumptionExecutor, interceptor, scheduler, 1, false) : new StandardConsumeService(clientId,
            FAKE_CONSUMER_GROUP_0, listener, consumptionExecutor, interceptor, scheduler, 1);
        when(consumer.getConsumeService()).thenReturn(service);
        return service;
    }

    private Future<?> startDrain(ConsumeService service) throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        Future<?> draining = drainExecutor.submit(() -> {
            started.countDown();
            service.awaitConsumption();
            return null;
        });
        assertTrue(started.await(5, TimeUnit.SECONDS));
        return draining;
    }

    private static void assertWaiting(Future<?> draining) throws Exception {
        try {
            draining.get(150, TimeUnit.MILLISECONDS);
            fail("Drain returned before the receive or terminal operation completed");
        } catch (TimeoutException expected) {
            // The controlled operation still owns an entry in the drain ledger.
        }
    }

    private static Status okStatus() {
        return Status.newBuilder().setCode(Code.OK).build();
    }

    private void setCounter(String name) throws Exception {
        Field field = PushConsumerImpl.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(consumer, new AtomicLong());
    }
}
