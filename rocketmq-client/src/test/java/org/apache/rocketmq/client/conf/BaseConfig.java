package org.apache.rocketmq.client.conf;

import apache.rocketmq.v1.Address;
import apache.rocketmq.v1.AddressScheme;
import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.Endpoints;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.Permission;
import apache.rocketmq.v1.Resource;
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

    protected Resource dummyTopicResource = Resource.newBuilder().setName(dummyTopic).build();
    protected Address dummyAddress = Address.newBuilder().setHost("127.0.0.1").setPort(8080).build();
    protected Endpoints dummyEndpoints =
            Endpoints.newBuilder().setScheme(AddressScheme.IPv4).addAddresses(dummyAddress).build();
    protected Broker dummyBroker =
            Broker.newBuilder().setName(dummyBrokerName).setId(0).setEndpoints(dummyEndpoints).build();
    protected Partition dummyPartition =
            Partition.newBuilder().setTopic(dummyTopicResource).setBroker(dummyBroker).build();

    protected org.apache.rocketmq.client.route.Partition dummyMqPartition =
            new org.apache.rocketmq.client.route.Partition(dummyPartition);

    protected MessageQueue dummyMessageQueue = new MessageQueue(dummyMqPartition);

    protected String dummyConsumerGroup = "TestConsumerGroup";
    protected String dummyConsumerGroup0 = "TestConsumerGroup0";
    protected String dummyConsumerGroup1 = "TestConsumerGroup1";

    protected String dummyProducerGroup = "TestProducerGroup";
    protected String dummyProducerGroup0 = "TestProducerGroup0";
    protected String dummyProducerGroup1 = "TestProducerGroup1";

    protected String dummyNameServerAddr = "11.167.164.105:9876";
    protected String dummyNameServerAddr0 = "11.167.164.106:9876";
    protected String dummyNameServerAddr1 = "11.167.164.107:9876";

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
