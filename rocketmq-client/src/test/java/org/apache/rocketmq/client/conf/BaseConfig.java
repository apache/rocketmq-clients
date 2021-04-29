package org.apache.rocketmq.client.conf;

import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.message.MessageQueue;

public class BaseConfig {
  protected String dummyTopic = "TestTopic";
  protected String dummyTopic0 = "TestTopic0";
  protected String dummyTopic1 = "TestTopic1";

  protected String dummyBrokerName = "TestBrokerName";
  protected String dummyBrokerName0 = "TestBrokerName0";
  protected String dummyBrokerName1 = "TestBrokerName1";

  protected int dummyQueueId = 0;
  protected int dummyQueueId0 = 1;
  protected int dummyQueueId1 = 2;

  protected MessageQueue dummyMessageQueue =
      new MessageQueue(dummyTopic, dummyBrokerName, dummyQueueId);
  protected MessageQueue dummyMessageQueue0 =
      new MessageQueue(dummyTopic0, dummyBrokerName0, dummyQueueId0);
  protected MessageQueue dummyMessageQueue1 =
      new MessageQueue(dummyTopic1, dummyBrokerName1, dummyQueueId1);

  protected String dummyConsumerGroup = "TestConsumerGroup";
  protected String dummyConsumerGroup0 = "TestConsumerGroup0";
  protected String dummyConsumerGroup1 = "TestConsumerGroup1";

  protected String dummyProducerGroup = "TestProducerGroup";
  protected String dummyProducerGroup0 = "TestProducerGroup0";
  protected String dummyProducerGroup1 = "TestProducerGroup1";

  protected String dummyNameServerAddr = "11.167.164.105:9876";

  protected String dummyTagExpression = FilterExpression.TAG_EXPRESSION_SUB_ALL;
  protected String dummyTag0 = "TestTagA";
  protected String dummyTag1 = "TestTagB";
}
