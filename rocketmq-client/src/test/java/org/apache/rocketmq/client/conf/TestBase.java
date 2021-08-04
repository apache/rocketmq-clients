package org.apache.rocketmq.client.conf;

import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.Address;
import apache.rocketmq.v1.AddressScheme;
import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.Endpoints;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.ResponseCommon;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.rpc.Code;
import com.google.rpc.Status;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageIdGenerator;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.utility.ThreadFactoryImpl;

public class TestBase {
    protected Random random = new Random();

    protected String dummyArn0 = "TestArn0";

    protected String dummyTopic0 = "TestTopic0";

    protected String dummyBrokerName0 = "TestBrokerName";

    protected int dummyQueueId = 0;
    protected int dummyQueueId0 = 1;
    protected int dummyQueueId1 = 2;

    protected long dummyQueueOffset = 0;

    protected String dummyClientId0 = "TestClientId0";

    protected String dummyConsumerGroup0 = "TestConsumerGroup0";
    protected String dummyConsumerGroup1 = "TestConsumerGroup1";
    protected String dummyConsumerGroup2 = "TestConsumerGroup2";

    protected String dummyProducerGroup = "TestProducerGroup";
    protected String dummyProducerGroup0 = "TestProducerGroup0";
    protected String dummyProducerGroup1 = "TestProducerGroup1";

    protected String dummyNameServerAddr = "11.167.164.105:9876";
    protected String dummyNameServerAddr0 = "11.167.164.106:9876";
    protected String dummyNameServerAddr1 = "11.167.164.107:9876";

    protected String dummyTagExpression0 = FilterExpression.TAG_EXPRESSION_SUB_ALL;

    protected String dummyTag0 = "TestTagA";
    protected String dummyTag1 = "TestTagB";

    protected String dummyHost0 = "127.0.0.1";
    protected int dummyPort0 = 8080;

    protected String dummyTarget = "127.0.0.1:10911";

    protected long dummyTermId = 1;
    protected long dummyPopTimestamp = 1619772316494L;
    protected long dummyInvisibleTime = 30 * 1000L;
    protected long dummyRestNum = 32;

    protected String dummyMsgExtraInfo = "ExtraInfo";
    protected String dummyTransactionId = "123456";
    protected String dummyReceiptHandle = "handle";

    protected Resource getDummyTopicResource() {
        return Resource.newBuilder().setArn(dummyArn0).setName(dummyTopic0).build();
    }

    protected Address getDummyAddress() {
        return Address.newBuilder().setHost(dummyHost0).setPort(dummyPort0).build();
    }

    protected Endpoints getDummyEndpoints() {
        return Endpoints.newBuilder().setScheme(AddressScheme.IPv4).addAddresses(getDummyAddress()).build();
    }

    protected Broker getDummyBroker() {
        return Broker.newBuilder().setName(dummyBrokerName0).setId(MixAll.MASTER_BROKER_ID).setEndpoints(getDummyEndpoints()).build();
    }

    protected Partition getDummyPartition() {
        final apache.rocketmq.v1.Partition partition =
                apache.rocketmq.v1.Partition.newBuilder().setTopic(getDummyTopicResource()).setBroker(getDummyBroker()).build();
        return new Partition(partition);
    }

    protected MessageQueue getDummyMessageQueue() {
        return new MessageQueue(getDummyPartition());
    }

    protected ThreadPoolExecutor getSingleThreadPoolExecutor() {
        return new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                                      new ThreadFactoryImpl("TestSingleWorker"));
    }

    protected ResponseCommon getSuccessResponseCommon() {
        final Status status = Status.newBuilder().setCode(Code.OK_VALUE).build();
        return ResponseCommon.newBuilder().setStatus(status).build();
    }

    protected ListenableFuture<AckMessageResponse> successAckMessageResponseFuture() {
        final ResponseCommon common = getSuccessResponseCommon();
        SettableFuture<AckMessageResponse> future0 = SettableFuture.create();
        final AckMessageResponse response = AckMessageResponse.newBuilder().setCommon(common).build();
        future0.set(response);
        return future0;
    }

    protected ListenableFuture<NackMessageResponse> successNackMessageResponseFuture() {
        final ResponseCommon common = getSuccessResponseCommon();
        SettableFuture<NackMessageResponse> future0 = SettableFuture.create();
        final NackMessageResponse response = NackMessageResponse.newBuilder().setCommon(common).build();
        future0.set(response);
        return future0;
    }

    protected MessageExt getDummyMessageExt(int bodySize) {
        final SystemAttribute systemAttribute = new SystemAttribute();
        systemAttribute.setMessageId(MessageIdGenerator.getInstance().next());
        systemAttribute.setReceiptHandle(dummyReceiptHandle);
        final ConcurrentMap<String, String> userAttribute = new ConcurrentHashMap<String, String>();
        final byte[] bytes = new byte[bodySize];
        random.nextBytes(bytes);
        final MessageImpl messageImpl = new MessageImpl(dummyTopic0, systemAttribute, userAttribute, bytes);
        return new MessageExt(messageImpl);
    }
}
