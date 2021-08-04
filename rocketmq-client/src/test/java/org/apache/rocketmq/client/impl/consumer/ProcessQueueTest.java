package org.apache.rocketmq.client.impl.consumer;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import io.grpc.Metadata;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.conf.TestBase;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@PrepareForTest(ProcessQueue.class)
public class ProcessQueueTest extends TestBase {

    private DefaultMQPushConsumerImpl mockedConsumerImpl;
    private AtomicLong consumptionOkCounter;
    private AtomicLong consumptionErrorCounter;
    private ClientInstance mockedClientInstance;
    private ProcessQueue processQueue;

    @BeforeMethod
    public void beforeMethod() throws ClientException {
        mockedConsumerImpl = mock(DefaultMQPushConsumerImpl.class);
        when(mockedConsumerImpl.getArn()).thenReturn(dummyArn0);

        consumptionOkCounter = new AtomicLong(0);
        consumptionErrorCounter = new AtomicLong(0);

        when(mockedConsumerImpl.getConsumptionOkCount()).thenReturn(consumptionOkCounter);
        when(mockedConsumerImpl.getConsumptionErrorCount()).thenReturn(consumptionErrorCounter);
        when(mockedConsumerImpl.sign()).thenReturn(new Metadata());
        when(mockedConsumerImpl.getConsumptionExecutor()).thenReturn(getSingleThreadPoolExecutor());
        when(mockedConsumerImpl.getGroup()).thenReturn(dummyConsumerGroup0);
        when(mockedConsumerImpl.getClientId()).thenReturn(dummyClientId0);

        mockedClientInstance = Mockito.mock(ClientInstance.class);

        when(mockedConsumerImpl.getClientInstance()).thenReturn(mockedClientInstance);

        final FilterExpression filterExpression = new FilterExpression(dummyTagExpression0);
        processQueue = new ProcessQueue(mockedConsumerImpl, getDummyMessageQueue(), filterExpression);
    }

    @Test
    public void testExpired() {
        Assert.assertFalse(processQueue.expired());
    }

    @Test
    public void testThrottled() {
        when(mockedConsumerImpl.cachedMessagesQuantityThresholdPerQueue()).thenReturn(8);
        when(mockedConsumerImpl.cachedMessagesBytesThresholdPerQueue()).thenReturn(1024);
        Assert.assertFalse(processQueue.throttled());
    }

    @Test
    public void testThrottledWithQuantityExceed() {
        int messagesQuantityThreshold = 8;
        int messageBytesThreshold = 1024;
        when(mockedConsumerImpl.cachedMessagesQuantityThresholdPerQueue()).thenReturn(messagesQuantityThreshold);
        when(mockedConsumerImpl.cachedMessagesBytesThresholdPerQueue()).thenReturn(messageBytesThreshold);
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < messagesQuantityThreshold; i++) {
            messageExtList.add(getDummyMessageExt(1));
        }
        processQueue.cacheMessages(messageExtList);
        Assert.assertTrue(processQueue.throttled());
    }

    @Test
    public void testThrottledWithBytesExceed() {
        int messagesQuantityThreshold = 8;
        int messageBytesThreshold = 1024;
        when(mockedConsumerImpl.cachedMessagesQuantityThresholdPerQueue()).thenReturn(messagesQuantityThreshold);
        when(mockedConsumerImpl.cachedMessagesBytesThresholdPerQueue()).thenReturn(messageBytesThreshold);
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(getDummyMessageExt(messageBytesThreshold + 1));
        processQueue.cacheMessages(messageExtList);
        Assert.assertTrue(processQueue.throttled());
    }

    @Test
    public void testTryTakePartialMessages() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(getDummyMessageExt(1));
        }
        processQueue.cacheMessages(messageExtList);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        int batchMaxSize = 1;
        final List<MessageExt> messagesTaken = processQueue.tryTakeMessages(batchMaxSize);
        Assert.assertEquals(messagesTaken.size(), batchMaxSize);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), batchMaxSize);
    }

    @Test
    public void testTryTakeAllMessages() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(getDummyMessageExt(1));
        }
        processQueue.cacheMessages(messageExtList);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        final List<MessageExt> messagesTaken = processQueue.tryTakeMessages(cachedMessageQuantity);
        Assert.assertEquals(cachedMessageQuantity, messagesTaken.size());
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), cachedMessageQuantity);
    }

    @Test
    public void testTryTakeExcessMessages() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(getDummyMessageExt(1));
        }
        processQueue.cacheMessages(messageExtList);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        int batchMaxSize = 1 + cachedMessageQuantity;
        final List<MessageExt> messagesTaken = processQueue.tryTakeMessages(batchMaxSize);
        Assert.assertEquals(cachedMessageQuantity, messagesTaken.size());
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), cachedMessageQuantity);
    }

    @Test
    public void testTryTakeFifoMessage() throws ClientException {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(getDummyMessageExt(1));
        }
        processQueue.cacheMessages(messageExtList);
        // first take.
        final MessageExt messageExt0 = processQueue.tryTakeFifoMessage();
        Assert.assertNotNull(messageExt0);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), 1);
        // second take, failed to lock.
        final MessageExt messageExt1 = processQueue.tryTakeFifoMessage();
        Assert.assertNull(messageExt1);
        // unlock.
        final ListenableFuture<AckMessageResponse> future0 = successAckMessageResponseFuture();
        when(mockedConsumerImpl.getClientInstance()).thenReturn(mockedClientInstance);
        when(mockedClientInstance.ackMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                             ArgumentMatchers.<AckMessageRequest>any(), anyLong(),
                                             ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(future0);
        processQueue.eraseFifoMessage(messageExt0, ConsumeStatus.OK);
        Assert.assertEquals(consumptionOkCounter.get(), 1);
        future0.addListener(new Runnable() {
            @Override
            public void run() {
                verify(mockedClientInstance, times(1)).ackMessage(ArgumentMatchers.<Endpoints>any(),
                                                                  ArgumentMatchers.<Metadata>any(),
                                                                  ArgumentMatchers.<AckMessageRequest>any(), anyLong(),
                                                                  ArgumentMatchers.<TimeUnit>any());
                final MessageExt messageExt2 = processQueue.tryTakeFifoMessage();
                Assert.assertNotNull(messageExt2);
            }
        }, MoreExecutors.directExecutor());
    }

    @Test
    public void testTryTakeFifoMessageWithNoPendingMessages() {
        final MessageExt messageExt = processQueue.tryTakeFifoMessage();
        Assert.assertNull(messageExt);
    }

    @Test
    public void testTryTakeFifoMessageWithRateLimiter() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(getDummyMessageExt(1));
        }
        processQueue.cacheMessages(messageExtList);
        RateLimiter rateLimiter = RateLimiter.create(1);
        when(mockedConsumerImpl.rateLimiter(anyString())).thenReturn(rateLimiter);
        // first take.
        final MessageExt messageExt0 = processQueue.tryTakeFifoMessage();
        Assert.assertNotNull(messageExt0);
        // unlock.
        //        processQueue.fifoConsumptionOutbound();
        // second take, failed to lock.
        final MessageExt messageExt1 = processQueue.tryTakeFifoMessage();
        Assert.assertNull(messageExt1);
    }

    @Test
    public void testTryTakeMessagesWithRateLimiter() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(getDummyMessageExt(1));
        }
        processQueue.cacheMessages(messageExtList);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        RateLimiter rateLimiter = RateLimiter.create(1);
        when(mockedConsumerImpl.rateLimiter(anyString())).thenReturn(rateLimiter);
        // first take.
        int batchMaxSize = 1;
        final List<MessageExt> messagesTaken0 = processQueue.tryTakeMessages(batchMaxSize);
        Assert.assertEquals(batchMaxSize, messagesTaken0.size());
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), batchMaxSize);
        // second take.
        final List<MessageExt> messagesTaken1 = processQueue.tryTakeMessages(batchMaxSize);
        Assert.assertEquals(messagesTaken1.size(), 0);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), batchMaxSize);
    }

    @Test
    public void eraseMessagesWithOk() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(getDummyMessageExt(1));
        }
        processQueue.cacheMessages(messageExtList);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        int batchMaxSize = 1;
        final List<MessageExt> messagesTaken = processQueue.tryTakeMessages(batchMaxSize);
        Assert.assertEquals(messagesTaken.size(), batchMaxSize);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), batchMaxSize);

        final ListenableFuture<AckMessageResponse> future0 = successAckMessageResponseFuture();
        when(mockedConsumerImpl.getClientInstance()).thenReturn(mockedClientInstance);
        when(mockedClientInstance.ackMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                             ArgumentMatchers.<AckMessageRequest>any(), anyLong(),
                                             ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(future0);
        processQueue.eraseMessages(messagesTaken, ConsumeStatus.OK);
        future0.addListener(new Runnable() {
            @Override
            public void run() {
                verify(mockedClientInstance, times(1)).ackMessage(ArgumentMatchers.<Endpoints>any(),
                                                                  ArgumentMatchers.<Metadata>any(),
                                                                  ArgumentMatchers.<AckMessageRequest>any(),
                                                                  anyLong(),
                                                                  ArgumentMatchers.<TimeUnit>any());
            }
        }, MoreExecutors.directExecutor());
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity - messagesTaken.size());
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), 0);
        Assert.assertEquals(consumptionOkCounter.get(), messagesTaken.size());
    }

    @Test
    public void eraseMessagesWithError() {
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            messageExtList.add(getDummyMessageExt(1));
        }
        processQueue.cacheMessages(messageExtList);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        int batchMaxSize = 1;
        final List<MessageExt> messagesTaken = processQueue.tryTakeMessages(batchMaxSize);
        Assert.assertEquals(messagesTaken.size(), batchMaxSize);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), batchMaxSize);

        final ListenableFuture<NackMessageResponse> future0 = successNackMessageResponseFuture();
        when(mockedConsumerImpl.getClientInstance()).thenReturn(mockedClientInstance);
        when(mockedClientInstance.nackMessage(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<Metadata>any(),
                                              ArgumentMatchers.<NackMessageRequest>any(), anyLong(),
                                              ArgumentMatchers.<TimeUnit>any()))
                .thenReturn(future0);

        processQueue.eraseMessages(messagesTaken, ConsumeStatus.ERROR);

        future0.addListener(new Runnable() {
            @Override
            public void run() {
                verify(mockedClientInstance, times(1)).nackMessage(ArgumentMatchers.<Endpoints>any(),
                                                                   ArgumentMatchers.<Metadata>any(),
                                                                   ArgumentMatchers.<NackMessageRequest>any(),
                                                                   anyLong(),
                                                                   ArgumentMatchers.<TimeUnit>any());
            }
        }, MoreExecutors.directExecutor());
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity - messagesTaken.size());
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), 0);
        Assert.assertEquals(consumptionErrorCounter.get(), messagesTaken.size());
    }

    @Test
    public void eraseMessagesWithBroadcastingModel() {
        when(mockedConsumerImpl.getMessageModel()).thenReturn(MessageModel.BROADCASTING);
        int cachedMessageQuantity = 8;
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < cachedMessageQuantity; i++) {
            byte[] bytes = new byte[1];
            random.nextBytes(bytes);
            final Message message = new Message(dummyTopic0, dummyTag0, bytes);
            messageExtList.add(message.getMessageExt());
        }
        processQueue.cacheMessages(messageExtList);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        int batchMaxSize = 1;
        final List<MessageExt> messagesTaken = processQueue.tryTakeMessages(batchMaxSize);
        Assert.assertEquals(messagesTaken.size(), batchMaxSize);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity);
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), batchMaxSize);

        processQueue.eraseMessages(messagesTaken, ConsumeStatus.OK);
        Assert.assertEquals(processQueue.cachedMessagesQuantity(), cachedMessageQuantity - messagesTaken.size());
        Assert.assertEquals(processQueue.inflightMessagesQuantity(), 0);
        Assert.assertEquals(consumptionOkCounter.get(), messagesTaken.size());
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
    //            Assert.assertFalse(processQueue.isPopExpired());
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
    //            Assert.assertTrue(processQueue.isPopExpired());
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
    //        Assert.assertEquals(msgCount, processQueue.getCachedMsgCount());
    //        Assert.assertEquals(msgCount * msgBodyLength, processQueue.getCachedMsgSize());
    //        Assert.assertEquals(processQueue.getCachedMessages(), messageExtList);
    //
    //        processQueue.removeCachedMessages(messageExtList);
    //        Assert.assertEquals(0, processQueue.getCachedMsgCount());
    //        Assert.assertEquals(0, processQueue.getCachedMsgSize());
    //        Assert.assertTrue(processQueue.getCachedMessages().isEmpty());
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
    //        Assert.assertEquals(1, processQueue.getCachedMsgCount());
    //        Assert.assertEquals(msgBodyLength, processQueue.getCachedMsgSize());
    //        Assert.assertNotEquals(processQueue.getCachedMessages(), messageExtList);
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
    //            Assert.assertFalse((Boolean) Whitebox.invokeMethod(processQueue, "isPopThrottled"));
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
    //            Assert.assertTrue((Boolean) Whitebox.invokeMethod(processQueue, "isPopThrottled"));
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
    //            Assert.assertTrue((Boolean) Whitebox.invokeMethod(processQueue, "isPopThrottled"));
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
