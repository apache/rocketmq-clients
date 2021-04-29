package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.conf.BaseConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.message.MessageExt;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ProcessQueueTest extends BaseConfig {

  private final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(dummyConsumerGroup);
  private final DefaultMQPushConsumerImpl consumerImpl = new DefaultMQPushConsumerImpl(consumer);
  private final FilterExpression filterExpression = new FilterExpression(dummyTagExpression);

  private ProcessQueue initProcessQueue() {
    ProcessQueue processQueue = new ProcessQueue(consumerImpl, dummyMessageQueue, filterExpression);
    Assert.assertTrue(processQueue.getCachedMessages().isEmpty());
    Assert.assertEquals(0, processQueue.getCachedMsgCount());
    Assert.assertEquals(0, processQueue.getCachedMsgSize());
    return processQueue;
  }

  @Test
  public void testCacheMessages() {
    final ProcessQueue processQueue = initProcessQueue();

    List<MessageExt> messageExtList = new ArrayList<MessageExt>();

    final int msgCount = 32;
    int msgBodyLength = 64;

    for (int i = 0; i < msgCount; i++) {
      byte[] bytes = new byte[msgBodyLength];
      new Random().nextBytes(bytes);

      final MessageExt messageExt = new MessageExt();
      messageExt.setTopic(dummyMessageQueue.getTopic());
      messageExt.setBrokerName(dummyMessageQueue.getBrokerName());
      messageExt.setQueueId(dummyMessageQueue.getQueueId());

      messageExt.setQueueOffset(i);
      messageExt.setBody(bytes);

      messageExtList.add(messageExt);
    }

    processQueue.cacheMessages(messageExtList);
    Assert.assertEquals(msgCount, processQueue.getCachedMsgCount());
    Assert.assertEquals(msgCount * msgBodyLength, processQueue.getCachedMsgSize());
    Assert.assertEquals(processQueue.getCachedMessages(), messageExtList);

    processQueue.removeCachedMessages(messageExtList);
    Assert.assertEquals(0, processQueue.getCachedMsgCount());
    Assert.assertEquals(0, processQueue.getCachedMsgSize());
    Assert.assertTrue(processQueue.getCachedMessages().isEmpty());
  }

  @Test
  public void testCacheExistMessages() {
    final ProcessQueue processQueue = initProcessQueue();

    List<MessageExt> messageExtList = new ArrayList<MessageExt>();

    final int msgCount = 32;
    int msgBodyLength = 64;
    int queueOffset = 0;

    for (int i = 0; i < msgCount; i++) {
      byte[] bytes = new byte[msgBodyLength];
      new Random().nextBytes(bytes);

      final MessageExt messageExt = new MessageExt();
      messageExt.setTopic(dummyMessageQueue.getTopic());
      messageExt.setBrokerName(dummyMessageQueue.getBrokerName());
      messageExt.setQueueId(dummyMessageQueue.getQueueId());

      messageExt.setQueueOffset(queueOffset);
      messageExt.setBody(bytes);

      messageExtList.add(messageExt);
    }

    processQueue.cacheMessages(messageExtList);
    Assert.assertEquals(1, processQueue.getCachedMsgCount());
    Assert.assertEquals(msgBodyLength, processQueue.getCachedMsgSize());
    Assert.assertNotEquals(processQueue.getCachedMessages(), messageExtList);
  }
}
