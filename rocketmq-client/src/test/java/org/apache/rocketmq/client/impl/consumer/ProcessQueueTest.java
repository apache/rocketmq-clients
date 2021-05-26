package org.apache.rocketmq.client.impl.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.ReceiveMessageRequest;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.conf.BaseConfig;
import org.apache.rocketmq.client.constant.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.message.MessageConst;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageImpl;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.Test;

@PrepareForTest(ProcessQueue.class)
public class ProcessQueueTest extends BaseConfig {
//
//    private final DefaultMQPushConsumerImpl mockedConsumerImpl =
//            Mockito.mock(DefaultMQPushConsumerImpl.class);
//    private final FilterExpression filterExpression = new FilterExpression(dummyTagExpression);
//
//    private ProcessQueue initProcessQueue() {
//        ProcessQueue processQueue =
//                new ProcessQueue(mockedConsumerImpl, dummyMessageQueue, filterExpression);
//        Assert.assertTrue(processQueue.getCachedMessages().isEmpty());
//        Assert.assertEquals(0, processQueue.getCachedMsgCount());
//        Assert.assertEquals(0, processQueue.getCachedMsgSize());
//        return processQueue;
//    }
//
//    private ConsumeService initMockedConsumeService() {
//        final ConsumeService mockedConsumeService = mock(ConsumeService.class);
//        doNothing().when(mockedConsumeService).dispatch(any(ProcessQueue.class));
//        return mockedConsumeService;
//    }
//
//    private ProcessQueue initMockedProcessQueue(ConsumeService mockedConsumeService) {
//        final ProcessQueue mockedProcessQueue = mock(ProcessQueue.class);
//        Whitebox.setInternalState(mockedProcessQueue, "messageQueue", dummyMessageQueue);
//
//        Whitebox.setInternalState(mockedProcessQueue, "termId", new AtomicLong(dummyTermId - 1));
//
//        final AtomicLong dummyPopTimes = new AtomicLong(0);
//        Whitebox.setInternalState(mockedConsumerImpl, "popTimes", dummyPopTimes);
//
//        final AtomicLong dummyPopMsgCount = new AtomicLong(0);
//        Whitebox.setInternalState(mockedConsumerImpl, "popMsgCount", dummyPopMsgCount);
//
//        Mockito.when(mockedConsumerImpl.getConsumeService()).thenReturn(mockedConsumeService);
//        Whitebox.setInternalState(mockedProcessQueue, "consumerImpl", mockedConsumerImpl);
//        return mockedProcessQueue;
//    }
//
//    @Test
//    public void testIsPopExpired() {
//        {
//            final ProcessQueue processQueue = initProcessQueue();
//            Assert.assertFalse(processQueue.isPopExpired());
//        }
//
//        {
//            final ProcessQueue processQueue = initProcessQueue();
//            Whitebox.setInternalState(
//                    processQueue,
//                    "lastPopTimestamp",
//                    System.currentTimeMillis() - ProcessQueue.MAX_POP_MESSAGE_INTERVAL_MILLIS - 1);
//            Assert.assertFalse(processQueue.isPopExpired());
//        }
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
//        Mockito.when(mockedConsumerImpl.getClientInstance()).thenReturn(mockedClientInstance);
//
//        final DefaultMQPushConsumer mockedConsumer = mock(DefaultMQPushConsumer.class);
//        Mockito.when(mockedConsumer.getConsumerGroup()).thenReturn(dummyConsumerGroup);
//        Mockito.when(mockedConsumerImpl.getDefaultMQPushConsumer()).thenReturn(mockedConsumer);
//        {
//            // Message is expired, no need to ACK.
//            Mockito.when(mockedConsumer.isAckMessageAsync()).thenReturn(true);
//            messageExt.setExpiredTimestamp(0);
//            processQueue.ackMessage(messageExt);
//            verify(mockedClientInstance, never()).ackMessage(anyString(), any(AckMessageRequest.class));
//            verify(mockedClientInstance, never())
//                    .ackMessageAsync(anyString(), any(AckMessageRequest.class));
//        }
//        {
//            // Message is not expired, and enable ACK message asynchronously.
//            Mockito.when(mockedConsumer.isAckMessageAsync()).thenReturn(true);
//            messageExt.setExpiredTimestamp(
//                    System.currentTimeMillis() + ProcessQueue.MESSAGE_EXPIRED_TOLERANCE_MILLIS + 1000);
//            processQueue.ackMessage(messageExt);
//            verify(mockedClientInstance, never()).ackMessage(anyString(), any(AckMessageRequest.class));
//            verify(mockedClientInstance, times(1))
//                    .ackMessageAsync(anyString(), any(AckMessageRequest.class));
//        }
//        {
//            // Message is not expired, and do not enable ACK message asynchronously.
//            Mockito.when(mockedConsumer.isAckMessageAsync()).thenReturn(false);
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
//        Mockito.when(mockedConsumerImpl.getClientInstance()).thenReturn(mockedClientInstance);
//
//        final DefaultMQPushConsumer mockedConsumer = mock(DefaultMQPushConsumer.class);
//        Mockito.when(mockedConsumer.getConsumerGroup()).thenReturn(dummyConsumerGroup);
//        Mockito.when(mockedConsumerImpl.getDefaultMQPushConsumer()).thenReturn(mockedConsumer);
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
//        Mockito.when(mockedClientInstance.getScheduler()).thenReturn(mockedScheduler);
//        Mockito.when(mockedConsumerImpl.getClientInstance()).thenReturn(mockedClientInstance);
//        Mockito.when(mockedClientInstance.resolveEndpoint(anyString(), anyString()))
//               .thenReturn(dummyTarget);
//
//        final DefaultMQPushConsumer mockedConsumer = mock(DefaultMQPushConsumer.class);
//        Mockito.when(mockedConsumer.getConsumerGroup()).thenReturn(dummyConsumerGroup);
//        Mockito.when(mockedConsumer.getConsumeFromWhere())
//               .thenReturn(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
//        Mockito.when(mockedConsumerImpl.getDefaultMQPushConsumer()).thenReturn(mockedConsumer);
//
//        {
//            // Future was not set.
//            final SettableFuture<PopResult> future = SettableFuture.create();
//            Mockito.when(
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
//            Mockito.when(
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
//            Mockito.when(
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
