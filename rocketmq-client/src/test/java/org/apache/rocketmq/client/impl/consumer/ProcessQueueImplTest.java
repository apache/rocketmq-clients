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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.Resource;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Metadata;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.ConsumeContext;
import org.apache.rocketmq.client.consumer.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.PullMessageResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.ReceiveMessageResult;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.tools.TestBase;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ProcessQueueImplTest extends TestBase {
    private ProcessQueueImpl processQueueImpl;

    @Mock
    private PushConsumerImpl consumerImpl;
    @Mock
    private ConsumeService consumeService;
    @Mock
    private MessageListenerConcurrently messageListenerConcurrently;

    private final int messageMaxDeliveryAttempts = 6;

    private final AtomicLong receptionTimes = new AtomicLong(0);
    private final AtomicLong receivedMessagesQuantity = new AtomicLong(0);

    private final AtomicLong pullTimes = new AtomicLong(0);
    private final AtomicLong pulledMessagesQuantity = new AtomicLong(0);

    private final AtomicLong consumptionOkCounter = new AtomicLong(0);
    private final AtomicLong consumptionErrorCounter = new AtomicLong(0);

    private final FilterExpression filterExpression = new FilterExpression(FAKE_TAG_EXPRESSION_0);
    private final Metadata metadata = new Metadata();

    @BeforeMethod
    public void beforeMethod() throws ClientException {
        MockitoAnnotations.initMocks(this);
        final Resource dummyProtoGroup = Resource.newBuilder().setResourceNamespace(FAKE_ARN_0).setName(FAKE_GROUP_0)
                                                 .build();

        when(consumerImpl.getNamespace()).thenReturn(FAKE_ARN_0);
        when(consumerImpl.getGroup()).thenReturn(FAKE_GROUP_0);
        when(consumerImpl.getPbGroup()).thenReturn(dummyProtoGroup);
        when(consumerImpl.getMaxDeliveryAttempts()).thenReturn(messageMaxDeliveryAttempts);
        when(consumerImpl.getMessageListener()).thenReturn(messageListenerConcurrently);
        when(consumerImpl.getMessageModel()).thenReturn(MessageModel.CLUSTERING);
        when(consumerImpl.getConsumeFromWhere()).thenReturn(ConsumeFromWhere.CONSUME_FROM_MAX_OFFSET);
        when(consumerImpl.getConsumeService()).thenReturn(consumeService);
        when(consumerImpl.getReceptionTimes()).thenReturn(receptionTimes);
        when(consumerImpl.getReceivedMessagesQuantity()).thenReturn(receivedMessagesQuantity);
        when(consumerImpl.getPullTimes()).thenReturn(pullTimes);
        when(consumerImpl.getPulledMessagesQuantity()).thenReturn(pulledMessagesQuantity);
        when(consumerImpl.getConsumptionOkQuantity()).thenReturn(consumptionOkCounter);
        when(consumerImpl.getConsumptionErrorQuantity()).thenReturn(consumptionErrorCounter);
        when(consumerImpl.getScheduler()).thenReturn(SCHEDULER);
        when(consumerImpl.sign()).thenReturn(metadata);
        when(consumerImpl.getConsumptionExecutor()).thenReturn(SINGLE_THREAD_POOL_EXECUTOR);
        when(consumerImpl.id()).thenReturn(FAKE_CLIENT_ID_0);
        when(consumerImpl.isRunning()).thenReturn(true);

        processQueueImpl = new ProcessQueueImpl(consumerImpl, fakeMessageQueue(), filterExpression);
    }

    @AfterMethod
    public void afterMethod() {
        // reset counter.
        consumptionOkCounter.set(0);
        consumptionErrorCounter.set(0);
    }

    @BeforeTest
    public void beforeTest() {
        processQueueImpl = new ProcessQueueImpl(consumerImpl, fakeMessageQueue(), filterExpression);
    }

    @Test
    public void testExpired() {
        assertFalse(processQueueImpl.expired());
    }

    @Test
    public void testIsCacheFull() {
        when(consumerImpl.cachedMessagesQuantityThresholdPerQueue()).thenReturn(8);
        when(consumerImpl.cachedMessagesBytesThresholdPerQueue()).thenReturn(1024);
        assertFalse(processQueueImpl.isCacheFull());
    }

    @Test
    public void testReceiveMessageImmediately() throws InterruptedException {
        final int cachedMessagesQuantityThresholdPerQueue = 8;
        when(consumerImpl.cachedMessagesQuantityThresholdPerQueue())
                .thenReturn(cachedMessagesQuantityThresholdPerQueue);
        when(consumerImpl.cachedMessagesBytesThresholdPerQueue()).thenReturn(Integer.MAX_VALUE);
        SettableFuture<ReceiveMessageResult> future0 = SettableFuture.create();
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(fakeMessageExt());
        future0.set(fakeReceiveMessageResult(messageExtList));
        when(consumerImpl.receiveMessage(ArgumentMatchers.<ReceiveMessageRequest>any(),
                                         ArgumentMatchers.<Endpoints>any(),
                                         anyLong())).thenReturn(future0);
        processQueueImpl.fetchMessageImmediately();
        Thread.sleep(ProcessQueueImpl.PULL_LATER_DELAY_MILLIS / 2);
        verify(consumerImpl, times(cachedMessagesQuantityThresholdPerQueue))
                .receiveMessage(ArgumentMatchers.<ReceiveMessageRequest>any(),
                                ArgumentMatchers.<Endpoints>any(), anyLong());
    }

    @Test
    public void testPullMessageImmediately() throws InterruptedException {
        final int cachedMessagesQuantityThresholdPerQueue = 8;
        when(consumerImpl.getMessageModel()).thenReturn(MessageModel.BROADCASTING);
        when(consumerImpl.cachedMessagesQuantityThresholdPerQueue())
                .thenReturn(cachedMessagesQuantityThresholdPerQueue);
        when(consumerImpl.cachedMessagesBytesThresholdPerQueue()).thenReturn(Integer.MAX_VALUE);
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(fakeMessageExt());
        SettableFuture<PullMessageResult> future1 = SettableFuture.create();
        final PullMessageResult pullMessageResult = new PullMessageResult(PullStatus.OK, 1, 0, 1000, messageExtList);
        future1.set(pullMessageResult);
        when(consumerImpl.pullMessage(ArgumentMatchers.<PullMessageRequest>any(), ArgumentMatchers.<Endpoints>any(),
                                      anyLong())).thenReturn(future1);
        SettableFuture<Long> future0 = SettableFuture.create();
        future0.set(0L);
        when(consumerImpl.queryOffset(ArgumentMatchers.<QueryOffsetRequest>any(), ArgumentMatchers.<Endpoints>any()))
                .thenReturn(future0);
        processQueueImpl.fetchMessageImmediately();
        Thread.sleep(ProcessQueueImpl.PULL_LATER_DELAY_MILLIS / 2);
        verify(consumerImpl, times(cachedMessagesQuantityThresholdPerQueue))
                .pullMessage(ArgumentMatchers.<PullMessageRequest>any(), ArgumentMatchers.<Endpoints>any(), anyLong());
    }

    @Test
    public void testIsCacheFullWithQuantityExceed() {
        int messagesQuantityThreshold = 8;
        int messageBytesThreshold = Integer.MAX_VALUE;
        when(consumerImpl.cachedMessagesQuantityThresholdPerQueue()).thenReturn(messagesQuantityThreshold);
        when(consumerImpl.cachedMessagesBytesThresholdPerQueue()).thenReturn(messageBytesThreshold);
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < messagesQuantityThreshold; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        assertTrue(processQueueImpl.isCacheFull());
    }

    @Test
    public void testIsCacheFullWithBytesExceed() {
        int messagesQuantityThreshold = Integer.MAX_VALUE;
        int messageBytesThreshold = 1024;
        when(consumerImpl.cachedMessagesQuantityThresholdPerQueue()).thenReturn(messagesQuantityThreshold);
        when(consumerImpl.cachedMessagesBytesThresholdPerQueue()).thenReturn(messageBytesThreshold);
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(fakeMessageExt(messageBytesThreshold + 1));
        processQueueImpl.cacheMessages(messageExtList);
        assertTrue(processQueueImpl.isCacheFull());
    }

    @Test
    public void testTryTakePartialMessages() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        int batchMaxSize = 1;
        final List<MessageExt> messagesTaken = processQueueImpl.tryTakeMessages(batchMaxSize);
        assertEquals(messagesTaken.size(), batchMaxSize);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        assertEquals(processQueueImpl.inflightMessagesQuantity(), batchMaxSize);
    }

    @Test
    public void testTryTakeAllMessages() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        final List<MessageExt> messagesTaken = processQueueImpl.tryTakeMessages(cachedMessageQuantity);
        assertEquals(cachedMessageQuantity, messagesTaken.size());
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        assertEquals(processQueueImpl.inflightMessagesQuantity(), cachedMessageQuantity);
    }

    @Test
    public void testTryTakeExcessMessages() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        int batchMaxSize = 1 + cachedMessageQuantity;
        final List<MessageExt> messagesTaken = processQueueImpl.tryTakeMessages(batchMaxSize);
        assertEquals(cachedMessageQuantity, messagesTaken.size());
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        assertEquals(processQueueImpl.inflightMessagesQuantity(), cachedMessageQuantity);
    }

    @Test
    public void testTryTakeFifoMessage() throws InterruptedException {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        // first take.
        final Optional<MessageExt> optionalMessageExt0 = processQueueImpl.tryTakeFifoMessage();
        assertTrue(optionalMessageExt0.isPresent());
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        assertEquals(processQueueImpl.inflightMessagesQuantity(), 1);
        // second take, failed to lock.
        final Optional<MessageExt> optionalMessageExt1 = processQueueImpl.tryTakeFifoMessage();
        assertFalse(optionalMessageExt1.isPresent());
        // unlock.
        final ListenableFuture<AckMessageResponse> future0 = okAckMessageResponseFuture();
        when(consumerImpl.ackMessage(ArgumentMatchers.<MessageExt>any())).thenReturn(future0);
        processQueueImpl.eraseFifoMessage(optionalMessageExt0.get(), ConsumeStatus.OK);
        assertEquals(consumptionOkCounter.get(), 1);
        Thread.sleep(50);
        future0.addListener(new Runnable() {
            @Override
            public void run() {
                verify(consumerImpl, times(1)).ackMessage(ArgumentMatchers.<MessageExt>any());
                final Optional<MessageExt> optionalMessageExt2 = processQueueImpl.tryTakeFifoMessage();
                assertTrue(optionalMessageExt2.isPresent());
            }
        }, MoreExecutors.directExecutor());
    }

    @Test
    public void testTryTakeFifoMessageWithNoPendingMessages() {
        final Optional<MessageExt> optionalMessageExt = processQueueImpl.tryTakeFifoMessage();
        assertFalse(optionalMessageExt.isPresent());
    }

    @Test
    public void testTryTakeFifoMessageWithRateLimiter() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        RateLimiter rateLimiter = RateLimiter.create(1);
        when(consumerImpl.rateLimiter(anyString())).thenReturn(rateLimiter);
        // first take.
        final Optional<MessageExt> optionalMessageExt0 = processQueueImpl.tryTakeFifoMessage();
        assertTrue(optionalMessageExt0.isPresent());
        final ListenableFuture<AckMessageResponse> future0 = okAckMessageResponseFuture();
        when(consumerImpl.ackMessage(ArgumentMatchers.<MessageExt>any())).thenReturn(future0);
        processQueueImpl.eraseFifoMessage(optionalMessageExt0.get(), ConsumeStatus.OK);
        assertEquals(consumptionOkCounter.get(), 1);
        future0.addListener(new Runnable() {
            @Override
            public void run() {
                verify(consumerImpl, times(1)).ackMessage(ArgumentMatchers.<MessageExt>any());
                final Optional<MessageExt> optionalMessageExt1 = processQueueImpl.tryTakeFifoMessage();
                assertFalse(optionalMessageExt1.isPresent());
            }
        }, MoreExecutors.directExecutor());
    }

    @Test
    public void testTryTakeFifoMessageWithDeliveryAttemptsExhausted() {
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(fakeMessageExt(1));
        processQueueImpl.cacheMessages(messageExtList);
        final Optional<MessageExt> optionalMessageExt = processQueueImpl.tryTakeFifoMessage();
        assertTrue(optionalMessageExt.isPresent());
        SettableFuture<ConsumeStatus> future0 = SettableFuture.create();
        future0.set(ConsumeStatus.ERROR);
        when(consumeService.consume(ArgumentMatchers.<MessageExt>any(), anyLong(),
                                    ArgumentMatchers.<TimeUnit>any())).thenReturn(future0);
        when(consumerImpl.forwardMessageToDeadLetterQueue(ArgumentMatchers.<MessageExt>any()))
                .thenReturn(okForwardMessageToDeadLetterQueueResponseListenableFuture());
        processQueueImpl.eraseFifoMessage(optionalMessageExt.get(), ConsumeStatus.ERROR);
        // attempts is exhausted include the first attempt.
        verify(consumeService, times(messageMaxDeliveryAttempts - 1))
                .consume(ArgumentMatchers.<MessageExt>any(), anyLong(), ArgumentMatchers.<TimeUnit>any());
    }

    @Test
    public void testTryTakeFifoMessageWithBroadcasting() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        RateLimiter rateLimiter = RateLimiter.create(1);
        when(consumerImpl.rateLimiter(anyString())).thenReturn(rateLimiter);
        when(consumerImpl.getMessageModel()).thenReturn(MessageModel.BROADCASTING);
        // first take.
        final Optional<MessageExt> optionalMessageExt0 = processQueueImpl.tryTakeFifoMessage();
        assertTrue(optionalMessageExt0.isPresent());
        processQueueImpl.eraseFifoMessage(optionalMessageExt0.get(), ConsumeStatus.OK);
        verify(consumerImpl, never()).ackMessage(ArgumentMatchers.<MessageExt>any());
    }


    @Test
    public void testTryTakeMessagesWithRateLimiter() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        RateLimiter rateLimiter = RateLimiter.create(1);
        when(consumerImpl.rateLimiter(anyString())).thenReturn(rateLimiter);
        // first take.
        int batchMaxSize = 1;
        final List<MessageExt> messagesTaken0 = processQueueImpl.tryTakeMessages(batchMaxSize);
        assertEquals(batchMaxSize, messagesTaken0.size());
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        assertEquals(processQueueImpl.inflightMessagesQuantity(), batchMaxSize);
        // second take.
        final List<MessageExt> messagesTaken1 = processQueueImpl.tryTakeMessages(batchMaxSize);
        assertEquals(messagesTaken1.size(), 0);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        assertEquals(processQueueImpl.inflightMessagesQuantity(), batchMaxSize);
    }

    @Test
    public void testEraseMessagesWithOk() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        int batchMaxSize = 1;
        final List<MessageExt> messagesTaken = processQueueImpl.tryTakeMessages(batchMaxSize);
        assertEquals(messagesTaken.size(), batchMaxSize);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        assertEquals(processQueueImpl.inflightMessagesQuantity(), batchMaxSize);

        final ListenableFuture<AckMessageResponse> future0 = okAckMessageResponseFuture();
        when(consumerImpl.ackMessage(ArgumentMatchers.<MessageExt>any()))
                .thenReturn(future0);
        processQueueImpl.eraseMessages(messagesTaken, ConsumeStatus.OK);
        future0.addListener(new Runnable() {
            @Override
            public void run() {
                verify(consumerImpl, times(1)).ackMessage(ArgumentMatchers.<MessageExt>any());
            }
        }, MoreExecutors.directExecutor());
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity - messagesTaken.size());
        assertEquals(processQueueImpl.inflightMessagesQuantity(), 0);
        assertEquals(consumptionOkCounter.get(), messagesTaken.size());
    }

    @Test
    public void testEraseMessagesWithError() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        int batchMaxSize = 1;
        final List<MessageExt> messagesTaken = processQueueImpl.tryTakeMessages(batchMaxSize);
        assertEquals(messagesTaken.size(), batchMaxSize);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        assertEquals(processQueueImpl.inflightMessagesQuantity(), batchMaxSize);

        final ListenableFuture<NackMessageResponse> future0 = okNackMessageResponseFuture();
        processQueueImpl.eraseMessages(messagesTaken, ConsumeStatus.ERROR);

        future0.addListener(new Runnable() {
            @Override
            public void run() {
                verify(consumerImpl, times(1)).nackMessage(ArgumentMatchers.<MessageExt>any());
            }
        }, MoreExecutors.directExecutor());
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity - messagesTaken.size());
        assertEquals(processQueueImpl.inflightMessagesQuantity(), 0);
        assertEquals(consumptionErrorCounter.get(), messagesTaken.size());
    }

    @Test
    public void testEraseMessagesWithBroadcastingModel() {
        when(consumerImpl.getMessageModel()).thenReturn(MessageModel.BROADCASTING);
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        int batchMaxSize = 1;
        final List<MessageExt> messagesTaken = processQueueImpl.tryTakeMessages(batchMaxSize);
        assertEquals(messagesTaken.size(), batchMaxSize);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        assertEquals(processQueueImpl.inflightMessagesQuantity(), batchMaxSize);

        processQueueImpl.eraseMessages(messagesTaken, ConsumeStatus.OK);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity - messagesTaken.size());
        assertEquals(processQueueImpl.inflightMessagesQuantity(), 0);
        assertEquals(consumptionOkCounter.get(), messagesTaken.size());
    }

    @Test
    public void testCacheCorruptedMessages() {
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(fakeMessageExt(1, true));
        when(consumerImpl.getMessageModel()).thenReturn(MessageModel.BROADCASTING);
        processQueueImpl.cacheMessages(messageExtList);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), 0);

        when(consumerImpl.getMessageModel()).thenReturn(MessageModel.CLUSTERING);
        when(consumerImpl.getMessageListener()).thenReturn(new MessageListenerConcurrently() {
            @Override
            public ConsumeStatus consume(List<MessageExt> messages, ConsumeContext context) {
                return null;
            }
        });
        processQueueImpl.cacheMessages(messageExtList);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), 0);
        verify(consumerImpl, times(1)).nackMessage(ArgumentMatchers.<MessageExt>any());

        when(consumerImpl.getMessageListener()).thenReturn(new MessageListenerOrderly() {
            @Override
            public ConsumeStatus consume(List<MessageExt> messages, ConsumeContext context) {
                return null;
            }
        });
        when(consumerImpl.forwardMessageToDeadLetterQueue(ArgumentMatchers.<MessageExt>any()))
                .thenReturn(okForwardMessageToDeadLetterQueueResponseListenableFuture());
        processQueueImpl.cacheMessages(messageExtList);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), 0);
        verify(consumerImpl, times(1)).forwardMessageToDeadLetterQueue(ArgumentMatchers.<MessageExt>any());
    }
}
