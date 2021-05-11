package org.apache.rocketmq.client.conf;

import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;

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

    protected long dummyQueueOffset = 0;

    protected String dummyMsgId = "1EE10C774F0D18B4AAC24CAB60130000";

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

    protected String dummyTarget = "127.0.0.1:10911";

    protected long dummyTermId = 1;
    protected long dummyPopTimestamp = 1619772316494L;
    protected long dummyInvisibleTime = 30 * 1000L;
    protected long dummyRestNum = 32;

    protected String dummyMsgExtraInfo = "ExtraInfo";

    protected String dummyTransactionId = "123456";
}
