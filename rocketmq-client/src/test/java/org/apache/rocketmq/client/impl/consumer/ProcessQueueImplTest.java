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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.Resource;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Metadata;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.PullMessageResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.ReceiveMessageResult;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.tools.TestBase;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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
        final Resource dummyProtoGroup = Resource.newBuilder().setArn(FAKE_ARN_0).setName(FAKE_GROUP_0).build();

        when(consumerImpl.getArn()).thenReturn(FAKE_ARN_0);
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
        when(consumerImpl.getClientId()).thenReturn(FAKE_CLIENT_ID_0);

        processQueueImpl = new ProcessQueueImpl(consumerImpl, fakeMessageQueue(), filterExpression);
    }

    @AfterMethod
    public void afterMethod() {
        // reset counter.
        consumptionOkCounter.set(0);
        consumptionErrorCounter.set(0);
    }

    @Test
    public void testExpired() {
        assertFalse(processQueueImpl.expired());
    }

    @Test
    public void testThrottled() {
        when(consumerImpl.cachedMessagesQuantityThresholdPerQueue()).thenReturn(8);
        when(consumerImpl.cachedMessagesBytesThresholdPerQueue()).thenReturn(1024);
        assertFalse(processQueueImpl.throttled());
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
    public void testThrottledWithQuantityExceed() {
        int messagesQuantityThreshold = 8;
        int messageBytesThreshold = Integer.MAX_VALUE;
        when(consumerImpl.cachedMessagesQuantityThresholdPerQueue()).thenReturn(messagesQuantityThreshold);
        when(consumerImpl.cachedMessagesBytesThresholdPerQueue()).thenReturn(messageBytesThreshold);
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < messagesQuantityThreshold; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        assertTrue(processQueueImpl.throttled());
    }

    @Test
    public void testThrottledWithBytesExceed() {
        int messagesQuantityThreshold = Integer.MAX_VALUE;
        int messageBytesThreshold = 1024;
        when(consumerImpl.cachedMessagesQuantityThresholdPerQueue()).thenReturn(messagesQuantityThreshold);
        when(consumerImpl.cachedMessagesBytesThresholdPerQueue()).thenReturn(messageBytesThreshold);
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(fakeMessageExt(messageBytesThreshold + 1));
        processQueueImpl.cacheMessages(messageExtList);
        assertTrue(processQueueImpl.throttled());
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
    public void testTryTakeFifoMessage() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(fakeMessageExt(1));
        }
        processQueueImpl.cacheMessages(messageExtList);
        // first take.
        final MessageExt messageExt0 = processQueueImpl.tryTakeFifoMessage();
        assertNotNull(messageExt0);
        assertEquals(processQueueImpl.cachedMessagesQuantity(), cachedMessageQuantity);
        assertEquals(processQueueImpl.inflightMessagesQuantity(), 1);
        // second take, failed to lock.
        final MessageExt messageExt1 = processQueueImpl.tryTakeFifoMessage();
        assertNull(messageExt1);
        // unlock.
        final ListenableFuture<AckMessageResponse> future0 = successAckMessageResponseFuture();
        when(consumerImpl.ackMessage(ArgumentMatchers.<MessageExt>any())).thenReturn(future0);
        processQueueImpl.eraseFifoMessage(messageExt0, ConsumeStatus.OK);
        assertEquals(consumptionOkCounter.get(), 1);
        future0.addListener(new Runnable() {
            @Override
            public void run() {
                verify(consumerImpl, times(1)).ackMessage(ArgumentMatchers.<MessageExt>any());
                final MessageExt messageExt2 = processQueueImpl.tryTakeFifoMessage();
                assertNotNull(messageExt2);
            }
        }, MoreExecutors.directExecutor());
    }

    @Test
    public void testTryTakeFifoMessageWithNoPendingMessages() {
        final MessageExt messageExt = processQueueImpl.tryTakeFifoMessage();
        assertNull(messageExt);
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
        final MessageExt messageExt0 = processQueueImpl.tryTakeFifoMessage();
        assertNotNull(messageExt0);
        final ListenableFuture<AckMessageResponse> future0 = successAckMessageResponseFuture();
        when(consumerImpl.ackMessage(ArgumentMatchers.<MessageExt>any())).thenReturn(future0);
        processQueueImpl.eraseFifoMessage(messageExt0, ConsumeStatus.OK);
        assertEquals(consumptionOkCounter.get(), 1);
        future0.addListener(new Runnable() {
            @Override
            public void run() {
                verify(consumerImpl, times(1)).ackMessage(ArgumentMatchers.<MessageExt>any());
                final MessageExt messageExt1 = processQueueImpl.tryTakeFifoMessage();
                assertNull(messageExt1);
            }
        }, MoreExecutors.directExecutor());
    }

    @Test
    public void testTryTakeFifoMessageWithDeliveryAttemptsExhausted() {
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(fakeMessageExt(1));
        processQueueImpl.cacheMessages(messageExtList);
        final MessageExt messageExt = processQueueImpl.tryTakeFifoMessage();
        SettableFuture<ConsumeStatus> future0 = SettableFuture.create();
        future0.set(ConsumeStatus.ERROR);
        when(consumeService.consume(ArgumentMatchers.<MessageExt>any(), anyLong(),
                                    ArgumentMatchers.<TimeUnit>any())).thenReturn(future0);
        when(consumerImpl.forwardMessageToDeadLetterQueue(ArgumentMatchers.<MessageExt>any()))
                .thenReturn(successForwardMessageToDeadLetterQueueResponseListenableFuture());
        processQueueImpl.eraseFifoMessage(messageExt, ConsumeStatus.ERROR);
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
        final MessageExt messageExt0 = processQueueImpl.tryTakeFifoMessage();
        assertNotNull(messageExt0);
        processQueueImpl.eraseFifoMessage(messageExt0, ConsumeStatus.OK);
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
    public void eraseMessagesWithOk() {
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

        final ListenableFuture<AckMessageResponse> future0 = successAckMessageResponseFuture();
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
    public void eraseMessagesWithError() {
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

        final ListenableFuture<NackMessageResponse> future0 = successNackMessageResponseFuture();
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
    public void eraseMessagesWithBroadcastingModel() {
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

    //    @Test
    //    public void testEraseMessages() {
    //        final DefaultMQPushConsumerImpl mockedConsumerImpl = Mockito.mock(DefaultMQPushConsumerImpl.class);
    //        final ProcessQueue processQueue = initProcessQueue(mockedConsumerImpl);
    //
    //
    //    }


    //
    //        {
    //            final ProcessQueue processQueue = initProcessQueue();
    //            Whitebox.setInternalState(
    //                    processQueue,
    //                    "lastThrottledTimestamp",
    //                    System.currentTimeMillis() - ProcessQueue.MAX_POP_MESSAGE_INTERVAL_MILLIS - 1);
    //            assertFalse(processQueue.isPopExpired());
    //        }
    //
    //        {
    //            final ProcessQueue processQueue = initProcessQueue();
    //            Whitebox.setInternalState(
    //                    processQueue,
    //                    "lastPopTimestamp",
    //                    System.currentTimeMillis() - ProcessQueue.MAX_POP_MESSAGE_INTERVAL_MILLIS - 1);
    //            Whitebox.setInternalState(
    //                    processQueue,
    //                    "lastThrottledTimestamp",
    //                    System.currentTimeMillis() - ProcessQueue.MAX_POP_MESSAGE_INTERVAL_MILLIS - 1);
    //            assertTrue(processQueue.isPopExpired());
    //        }
    //    }
    //
    //    @Test
    //    public void testCacheMessages() {
    //        final ProcessQueue processQueue = initProcessQueue();
    //
    //        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
    //
    //        final int msgCount = 32;
    //        int msgBodyLength = 64;
    //
    //        for (int i = 0; i < msgCount; i++) {
    //            byte[] bytes = new byte[msgBodyLength];
    //            new Random().nextBytes(bytes);
    //
    //            final MessageExt messageExt = new MessageExt(new MessageImpl());
    ////            messageExt.setTopic(dummyMessageQueue.getTopic());
    ////            messageExt.setBrokerName(dummyMessageQueue.getBrokerName());
    ////            messageExt.setQueueId(dummyMessageQueue.getQueueId());
    ////
    ////            messageExt.setQueueOffset(i);
    ////            messageExt.setBody(bytes);
    //
    //            messageExtList.add(messageExt);
    //        }
    //
    //        processQueue.cacheMessages(messageExtList);
    //        assertEquals(msgCount, processQueue.getCachedMsgCount());
    //        assertEquals(msgCount * msgBodyLength, processQueue.getCachedMsgSize());
    //        assertEquals(processQueue.getCachedMessages(), messageExtList);
    //
    //        processQueue.removeCachedMessages(messageExtList);
    //        assertEquals(0, processQueue.getCachedMsgCount());
    //        assertEquals(0, processQueue.getCachedMsgSize());
    //        assertTrue(processQueue.getCachedMessages().isEmpty());
    //    }
    //
    //    @Test
    //    public void testCacheExistMessages() {
    //        final ProcessQueue processQueue = initProcessQueue();
    //
    //        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
    //
    //        final int msgCount = 32;
    //        int msgBodyLength = 64;
    //        int queueOffset = 0;
    //
    //        for (int i = 0; i < msgCount; i++) {
    //            byte[] bytes = new byte[msgBodyLength];
    //            new Random().nextBytes(bytes);
    //
    //            final MessageExt messageExt = new MessageExt();
    //            messageExt.setTopic(dummyMessageQueue.getTopic());
    //            messageExt.setBrokerName(dummyMessageQueue.getBrokerName());
    //            messageExt.setQueueId(dummyMessageQueue.getQueueId());
    //
    //            messageExt.setQueueOffset(queueOffset);
    //            messageExt.setBody(bytes);
    //
    //            messageExtList.add(messageExt);
    //        }
    //
    //        processQueue.cacheMessages(messageExtList);
    //        assertEquals(1, processQueue.getCachedMsgCount());
    //        assertEquals(msgBodyLength, processQueue.getCachedMsgSize());
    //        assertNotEquals(processQueue.getCachedMessages(), messageExtList);
    //    }
    //
    //    @Test
    //    public void testHandlePopResult() throws Exception {
    //        final ConsumeService mockedConsumeService = initMockedConsumeService();
    //        final ProcessQueue mockedProcessQueue = initMockedProcessQueue(mockedConsumeService);
    //
    //        {
    //            List<MessageExt> messageExtList = new ArrayList<MessageExt>();
    //
    //            final MessageExt messageExt = new MessageExt();
    //            messageExt.setTopic(dummyMessageQueue.getTopic());
    //            messageExt.setBrokerName(dummyMessageQueue.getBrokerName());
    //            messageExt.setQueueId(dummyMessageQueue.getQueueId());
    //
    //            messageExtList.add(messageExt);
    //
    //            final PopResult popResult =
    //                    new PopResult(
    //                            dummyTarget,
    //                            PopStatus.FOUND,
    //                            dummyTermId,
    //                            dummyPopTimestamp,
    //                            dummyInvisibleTime,
    //                            dummyRestNum,
    //                            messageExtList);
    //            Whitebox.invokeMethod(mockedProcessQueue, "handlePopResult", popResult);
    //            verify(mockedConsumeService, times(1)).dispatch(mockedProcessQueue);
    //            verify(mockedProcessQueue, times(1)).cacheMessages(ArgumentMatchers.<MessageExt>anyList());
    //            verify(mockedProcessQueue, times(1)).prepareNextPop(anyLong(), anyString());
    //        }
    //
    //        {
    //            final PopResult popResult =
    //                    new PopResult(
    //                            dummyTarget,
    //                            PopStatus.NO_NEW_MSG,
    //                            dummyTermId,
    //                            dummyPopTimestamp,
    //                            dummyInvisibleTime,
    //                            dummyRestNum,
    //                            new ArrayList<MessageExt>());
    //            Whitebox.invokeMethod(mockedProcessQueue, "handlePopResult", popResult);
    //            verify(mockedConsumeService, times(1)).dispatch(mockedProcessQueue);
    //            verify(mockedProcessQueue, times(1)).cacheMessages(ArgumentMatchers.<MessageExt>anyList());
    //            verify(mockedProcessQueue, times(2)).prepareNextPop(anyLong(), anyString());
    //        }
    //
    //        {
    //            final PopResult popResult =
    //                    new PopResult(
    //                            dummyTarget,
    //                            PopStatus.POLLING_FULL,
    //                            dummyTermId,
    //                            dummyPopTimestamp,
    //                            dummyInvisibleTime,
    //                            dummyRestNum,
    //                            new ArrayList<MessageExt>());
    //            Whitebox.invokeMethod(mockedProcessQueue, "handlePopResult", popResult);
    //            verify(mockedConsumeService, times(1)).dispatch(mockedProcessQueue);
    //            verify(mockedProcessQueue, times(1)).cacheMessages(ArgumentMatchers.<MessageExt>anyList());
    //            verify(mockedProcessQueue, times(3)).prepareNextPop(anyLong(), anyString());
    //        }
    //
    //        {
    //            final PopResult popResult =
    //                    new PopResult(
    //                            dummyTarget,
    //                            PopStatus.POLLING_NOT_FOUND,
    //                            dummyTermId,
    //                            dummyPopTimestamp,
    //                            dummyInvisibleTime,
    //                            dummyRestNum,
    //                            new ArrayList<MessageExt>());
    //            Whitebox.invokeMethod(mockedProcessQueue, "handlePopResult", popResult);
    //            verify(mockedConsumeService, times(1)).dispatch(mockedProcessQueue);
    //            verify(mockedProcessQueue, times(1)).cacheMessages(ArgumentMatchers.<MessageExt>anyList());
    //            verify(mockedProcessQueue, times(4)).prepareNextPop(anyLong(), anyString());
    //        }
    //
    //        {
    //            final PopResult popResult =
    //                    new PopResult(
    //                            dummyTarget,
    //                            PopStatus.SERVICE_UNSTABLE,
    //                            dummyTermId,
    //                            dummyPopTimestamp,
    //                            dummyInvisibleTime,
    //                            dummyRestNum,
    //                            new ArrayList<MessageExt>());
    //            Whitebox.invokeMethod(mockedProcessQueue, "handlePopResult", popResult);
    //            verify(mockedConsumeService, times(1)).dispatch(mockedProcessQueue);
    //            verify(mockedProcessQueue, times(1)).cacheMessages(ArgumentMatchers.<MessageExt>anyList());
    //            verify(mockedProcessQueue, times(5)).prepareNextPop(anyLong(), anyString());
    //        }
    //
    //        {
    //            final PopResult popResult =
    //                    new PopResult(
    //                            dummyTarget,
    //                            PopStatus.STATUS_FOR_TEST,
    //                            dummyTermId,
    //                            dummyPopTimestamp,
    //                            dummyInvisibleTime,
    //                            dummyRestNum,
    //                            new ArrayList<MessageExt>());
    //            Whitebox.invokeMethod(mockedProcessQueue, "handlePopResult", popResult);
    //            verify(mockedConsumeService, times(1)).dispatch(mockedProcessQueue);
    //            verify(mockedProcessQueue, times(1)).cacheMessages(ArgumentMatchers.<MessageExt>anyList());
    //            verify(mockedProcessQueue, times(6)).prepareNextPop(anyLong(), anyString());
    //        }
    //    }
    //
    //    @Test
    //    public void testPrepareNextPop() {
    //    }
    //
    //    @Test
    //    public void testIsPopThrottled() throws Exception {
    //        {
    //            final ProcessQueue processQueue = initProcessQueue();
    //            assertFalse((Boolean) Whitebox.invokeMethod(processQueue, "isPopThrottled"));
    //        }
    //        {
    //            final ProcessQueue processQueue = initProcessQueue();
    //            List<MessageExt> messageExtList = new ArrayList<MessageExt>();
    //
    //            for (int i = 0; i < ProcessQueue.MAX_CACHED_MESSAGES_COUNT_PER_MESSAGE_QUEUE + 1; i++) {
    //                final MessageExt messageExt = new MessageExt();
    //                messageExt.setTopic(dummyMessageQueue.getTopic());
    //                messageExt.setBrokerName(dummyMessageQueue.getBrokerName());
    //                messageExt.setQueueId(dummyMessageQueue.getQueueId());
    //                messageExt.setQueueOffset(dummyQueueOffset + i);
    //
    //                messageExtList.add(messageExt);
    //            }
    //
    //            processQueue.cacheMessages(messageExtList);
    //            assertTrue((Boolean) Whitebox.invokeMethod(processQueue, "isPopThrottled"));
    //        }
    //        {
    //            final ProcessQueue processQueue = initProcessQueue();
    //            List<MessageExt> messageExtList = new ArrayList<MessageExt>();
    //
    //            final MessageExt messageExt = new MessageExt();
    //            messageExt.setTopic(dummyMessageQueue.getTopic());
    //            messageExt.setBrokerName(dummyMessageQueue.getBrokerName());
    //            messageExt.setQueueId(dummyMessageQueue.getQueueId());
    //            final int bodySize = (int) (ProcessQueue.MAX_CACHED_MESSAGES_SIZE_PER_MESSAGE_QUEUE + 1);
    //            messageExt.setBody(new byte[bodySize]);
    //
    //            messageExtList.add(messageExt);
    //
    //            processQueue.cacheMessages(messageExtList);
    //            assertTrue((Boolean) Whitebox.invokeMethod(processQueue, "isPopThrottled"));
    //        }
    //    }
    //
    //    @Test
    //    public void testAckMessage() throws MQClientException {
    //        final ProcessQueue processQueue = initProcessQueue();
    //
    //        MessageExt messageExt = new MessageExt();
    //        messageExt.setTopic(dummyMessageQueue.getTopic());
    //        messageExt.setBrokerName(dummyMessageQueue.getBrokerName());
    //        messageExt.setQueueId(dummyMessageQueue.getQueueId());
    //        messageExt.putProperty(MessageConst.PROPERTY_POP_CK, dummyMsgExtraInfo);
    //        messageExt.putProperty(MessageConst.PROPERTY_ACK_HOST_ADDRESS, dummyTarget);
    //
    //        ClientInstance mockedClientInstance = Mockito.mock(ClientInstance.class);
    //        when(mockedConsumerImpl.getClientInstance()).thenReturn(mockedClientInstance);
    //
    //        final DefaultMQPushConsumer mockedConsumer = mock(DefaultMQPushConsumer.class);
    //        when(mockedConsumer.getConsumerGroup()).thenReturn(dummyConsumerGroup);
    //        when(mockedConsumerImpl.getDefaultMQPushConsumer()).thenReturn(mockedConsumer);
    //        {
    //            // Message is expired, no need to ACK.
    //            when(mockedConsumer.isAckMessageAsync()).thenReturn(true);
    //            messageExt.setExpiredTimestamp(0);
    //            processQueue.ackMessage(messageExt);
    //            verify(mockedClientInstance, never()).ackMessage(anyString(), any(AckMessageRequest.class));
    //            verify(mockedClientInstance, never())
    //                    .ackMessageAsync(anyString(), any(AckMessageRequest.class));
    //        }
    //        {
    //            // Message is not expired, and enable ACK message asynchronously.
    //            when(mockedConsumer.isAckMessageAsync()).thenReturn(true);
    //            messageExt.setExpiredTimestamp(
    //                    System.currentTimeMillis() + ProcessQueue.MESSAGE_EXPIRED_TOLERANCE_MILLIS + 1000);
    //            processQueue.ackMessage(messageExt);
    //            verify(mockedClientInstance, never()).ackMessage(anyString(), any(AckMessageRequest.class));
    //            verify(mockedClientInstance, times(1))
    //                    .ackMessageAsync(anyString(), any(AckMessageRequest.class));
    //        }
    //        {
    //            // Message is not expired, and do not enable ACK message asynchronously.
    //            when(mockedConsumer.isAckMessageAsync()).thenReturn(false);
    //            messageExt.setExpiredTimestamp(
    //                    System.currentTimeMillis() + ProcessQueue.MESSAGE_EXPIRED_TOLERANCE_MILLIS + 1000);
    //            processQueue.ackMessage(messageExt);
    //            verify(mockedClientInstance, times(1)).ackMessage(anyString(), any(AckMessageRequest.class));
    //            verify(mockedClientInstance, times(1))
    //                    .ackMessageAsync(anyString(), any(AckMessageRequest.class));
    //        }
    //    }
    //
    //    @Test
    //    public void testNegativeAckMessage() throws MQClientException {
    //        final ProcessQueue processQueue = initProcessQueue();
    //
    //        ClientInstance mockedClientInstance = Mockito.mock(ClientInstance.class);
    //        when(mockedConsumerImpl.getClientInstance()).thenReturn(mockedClientInstance);
    //
    //        final DefaultMQPushConsumer mockedConsumer = mock(DefaultMQPushConsumer.class);
    //        when(mockedConsumer.getConsumerGroup()).thenReturn(dummyConsumerGroup);
    //        when(mockedConsumerImpl.getDefaultMQPushConsumer()).thenReturn(mockedConsumer);
    //
    //        MessageExt messageExt = new MessageExt();
    //        messageExt.setTopic(dummyMessageQueue.getTopic());
    //        messageExt.setBrokerName(dummyMessageQueue.getBrokerName());
    //        messageExt.setQueueId(dummyMessageQueue.getQueueId());
    //        messageExt.putProperty(MessageConst.PROPERTY_POP_CK, dummyMsgExtraInfo);
    //        messageExt.putProperty(MessageConst.PROPERTY_ACK_HOST_ADDRESS, dummyTarget);
    //
    //        processQueue.negativeAckMessage(messageExt);
    //    }
    //
    //    @Test
    //    public void testPopMessage() throws MQClientException {
    //        final ProcessQueue processQueue = initProcessQueue();
    //        ClientInstance mockedClientInstance = Mockito.mock(ClientInstance.class);
    //        final ScheduledExecutorService mockedScheduler = mock(ScheduledExecutorService.class);
    //        when(mockedClientInstance.getScheduler()).thenReturn(mockedScheduler);
    //        when(mockedConsumerImpl.getClientInstance()).thenReturn(mockedClientInstance);
    //        when(mockedClientInstance.resolveEndpoint(anyString(), anyString()))
    //               .thenReturn(dummyTarget);
    //
    //        final DefaultMQPushConsumer mockedConsumer = mock(DefaultMQPushConsumer.class);
    //        when(mockedConsumer.getConsumerGroup()).thenReturn(dummyConsumerGroup);
    //        when(mockedConsumer.getConsumeFromWhere())
    //               .thenReturn(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
    //        when(mockedConsumerImpl.getDefaultMQPushConsumer()).thenReturn(mockedConsumer);
    //
    //        {
    //            // Future was not set.
    //            final SettableFuture<PopResult> future = SettableFuture.create();
    //            when(
    //                    mockedClientInstance.receiveMessageAsync(
    //                            anyString(), any(ReceiveMessageRequest.class), anyLong(), any(TimeUnit.class)))
    //                   .thenReturn(future);
    //            processQueue.popMessage();
    //            verify(mockedClientInstance, times(1))
    //                    .receiveMessageAsync(
    //                            anyString(), any(ReceiveMessageRequest.class), anyLong(), any(TimeUnit.class));
    //            verify(mockedScheduler, never())
    //                    .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    //        }
    //        // FutureCallback#onFailure was invoked.
    //        {
    //            final SettableFuture<PopResult> future = SettableFuture.create();
    //            future.setException(new Throwable());
    //            when(
    //                    mockedClientInstance.receiveMessageAsync(
    //                            anyString(), any(ReceiveMessageRequest.class), anyLong(), any(TimeUnit.class)))
    //                   .thenReturn(future);
    //            processQueue.popMessage();
    //            verify(mockedClientInstance, times(2))
    //                    .receiveMessageAsync(
    //                            anyString(), any(ReceiveMessageRequest.class), anyLong(), any(TimeUnit.class));
    //            verify(mockedScheduler, times(1))
    //                    .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    //        }
    //        // Exception occurs while executing ClientInstance#popMessageAsync
    //        {
    //            final SettableFuture<PopResult> future = SettableFuture.create();
    //            future.setException(new Throwable());
    //            when(
    //                    mockedClientInstance.receiveMessageAsync(
    //                            anyString(), any(ReceiveMessageRequest.class), anyLong(), any(TimeUnit.class)))
    //                   .thenThrow(new RuntimeException());
    //            processQueue.popMessage();
    //            verify(mockedClientInstance, times(3))
    //                    .receiveMessageAsync(
    //                            anyString(), any(ReceiveMessageRequest.class), anyLong(), any(TimeUnit.class));
    //            verify(mockedScheduler, times(2))
    //                    .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    //        }
    //    }
}
