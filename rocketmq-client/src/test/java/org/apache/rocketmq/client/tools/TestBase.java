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

package org.apache.rocketmq.client.tools;

import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.Address;
import apache.rocketmq.v1.AddressScheme;
import apache.rocketmq.v1.Assignment;
import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.Digest;
import apache.rocketmq.v1.DigestType;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.GenericPollingResponse;
import apache.rocketmq.v1.MultiplexingResponse;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.Permission;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.ResponseCommon;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.VerifyMessageConsumptionRequest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import com.google.rpc.Status;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.consumer.ReceiveMessageResult;
import org.apache.rocketmq.client.consumer.ReceiveStatus;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageIdGenerator;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageImplAccessor;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.apache.rocketmq.utility.UtilAll;

public class TestBase {
    protected static final String FAKE_ACCESS_KEY = "foo-bar-access-key";
    protected static final String FAKE_SECRET_KEY = "foo-bar-secret-key";
    protected static final String FAKE_SECURITY_TOKEN = "foo-bar-security-token";

    protected static final String FAKE_ARN_0 = "foo-bar-arn-0";

    protected static final String FAKE_TOPIC_0 = "foo-bar-topic-0";
    protected static final String FAKE_TOPIC_1 = "foo-bar-topic-1";

    protected static final String FAKE_BROKER_NAME_0 = "foo-bar-broker-name-0";

    protected static final String FAKE_MESSAGE_GROUP_0 = "foo-bar-message-group-0";

    protected static final String FAKE_CLIENT_ID_0 = "foo-bar-client-id-0";

    protected static final String FAKE_GROUP_0 = "foo-bar-group-0";
    protected static final String FAKE_GROUP_1 = "foo-bar-group-1";
    protected static final String FAKE_GROUP_2 = "foo-bar-group-2";

    protected static final String FAKE_NAME_SERVER_ADDR_0 = "127.0.0.1:9876";

    protected static final String FAKE_TAG_EXPRESSION_0 = FilterExpression.TAG_EXPRESSION_SUB_ALL;

    protected static final String FAKE_TAG_0 = "foo-bar-tag-0";
    protected static final String FAKE_TAG_1 = "foo-bar-tag-1";

    protected static final String FAKE_HOST_0 = "127.0.0.1";

    protected static final int FAKE_PORT_0 = 8080;

    protected static final String FAKE_TRANSACTION_ID = "foo-bar-transaction-id";

    protected static final String FAKE_RECEIPT_HANDLE = "foo-bar-handle";

    protected static final ThreadPoolExecutor SINGLE_THREAD_POOL_EXECUTOR =
            new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
                                   new LinkedBlockingQueue<Runnable>(), new ThreadFactoryImpl("TestSingleWorker"));

    protected static final ThreadPoolExecutor SEND_CALLBACK_EXECUTOR =
            new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                                   new ThreadFactoryImpl("TestSendCallback"));

    protected static final ScheduledExecutorService SCHEDULER =
            new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("TestScheduler"));

    protected Resource fakePbTopic0() {
        return Resource.newBuilder().setResourceNamespace(FAKE_ARN_0).setName(FAKE_TOPIC_0).build();
    }

    protected Address fakePbAddress0() {
        return fakePbAddress(FAKE_HOST_0, FAKE_PORT_0);
    }

    protected Address fakePbAddress(String host, int port) {
        return Address.newBuilder().setHost(host).setPort(port).build();
    }

    protected apache.rocketmq.v1.Endpoints fakePbEndpoints0() {
        return fakePbEndpoints(fakePbAddress0());
    }

    protected apache.rocketmq.v1.Endpoints fakePbEndpoints(Address address) {
        return apache.rocketmq.v1.Endpoints.newBuilder().setScheme(AddressScheme.IPv4).addAddresses(address).build();
    }

    protected Endpoints fakeEndpoints0() {
        return new Endpoints(fakePbEndpoints0());
    }

    protected Broker fakePbBroker0() {
        return fakePbBroker(FAKE_BROKER_NAME_0, MixAll.MASTER_BROKER_ID, fakePbEndpoints0());
    }

    protected Broker fakePbBroker(String name) {
        return fakePbBroker(name, MixAll.MASTER_BROKER_ID, fakePbEndpoints0());
    }

    protected Broker fakePbBroker(String name, int id, apache.rocketmq.v1.Endpoints endpoints) {
        return Broker.newBuilder().setName(name).setId(id).setEndpoints(endpoints).build();
    }

    protected apache.rocketmq.v1.Partition fakePbPartition0() {
        return fakePbPartition0(Permission.READ_WRITE, 0);
    }

    protected apache.rocketmq.v1.Partition fakePbPartition0(Permission permission) {
        return fakePbPartition0(permission, 0);
    }

    protected apache.rocketmq.v1.Partition fakePbPartition0(Permission permission, int id) {
        return fakePbPartition(fakePbTopic0(), fakePbBroker0(), permission, id);
    }

    protected apache.rocketmq.v1.Partition fakePbPartition(Resource protoTopic, Broker broker,
                                                           Permission permission, int id) {
        return apache.rocketmq.v1.Partition.newBuilder().setTopic(protoTopic).setBroker(broker)
                                           .setPermission(permission).setId(id).build();
    }

    protected Partition fakePartition0() {
        return new Partition(fakePbPartition0());
    }

    protected MessageQueue fakeMessageQueue() {
        return new MessageQueue(fakePartition0());
    }

    protected TopicRouteData fakeTopicRouteData(Permission permission) {
        List<apache.rocketmq.v1.Partition> partitionList = new ArrayList<apache.rocketmq.v1.Partition>();
        partitionList.add(fakePbPartition0(permission));
        return new TopicRouteData(partitionList);
    }

    protected ReceiveMessageResult fakeReceiveMessageResult(List<MessageExt> messageExtList) {
        return new ReceiveMessageResult(fakeEndpoints0(), ReceiveStatus.OK, 0, 0, messageExtList);
    }

    protected ResponseCommon okResponseCommon() {
        final Status status = Status.newBuilder().setCode(Code.OK_VALUE).build();
        return ResponseCommon.newBuilder().setStatus(status).build();
    }

    protected ListenableFuture<AckMessageResponse> okAckMessageResponseFuture() {
        final ResponseCommon common = okResponseCommon();
        SettableFuture<AckMessageResponse> future0 = SettableFuture.create();
        final AckMessageResponse response = AckMessageResponse.newBuilder().setCommon(common).build();
        future0.set(response);
        return future0;
    }

    protected ListenableFuture<NackMessageResponse> okNackMessageResponseFuture() {
        final ResponseCommon common = okResponseCommon();
        SettableFuture<NackMessageResponse> future0 = SettableFuture.create();
        final NackMessageResponse response = NackMessageResponse.newBuilder().setCommon(common).build();
        future0.set(response);
        return future0;
    }

    protected VerifyMessageConsumptionRequest fakeVerifyMessageConsumptionRequest()
            throws UnsupportedEncodingException {
        return VerifyMessageConsumptionRequest.newBuilder().setMessage(fakePbMessage0())
                                              .setPartition(fakePbPartition0()).build();
    }

    protected ListenableFuture<ForwardMessageToDeadLetterQueueResponse>
        okForwardMessageToDeadLetterQueueResponseListenableFuture() {
        final ResponseCommon common = okResponseCommon();
        SettableFuture<ForwardMessageToDeadLetterQueueResponse> future0 = SettableFuture.create();
        final ForwardMessageToDeadLetterQueueResponse response =
                ForwardMessageToDeadLetterQueueResponse.newBuilder().setCommon(common).build();
        future0.set(response);
        return future0;
    }

    protected ListenableFuture<QueryRouteResponse> okQueryRouteResponseFuture() {
        final ResponseCommon common = okResponseCommon();
        SettableFuture<QueryRouteResponse> future0 = SettableFuture.create();
        final QueryRouteResponse response =
                QueryRouteResponse.newBuilder().setCommon(common).addPartitions(fakePbPartition0()).build();
        future0.set(response);
        return future0;
    }

    protected ListenableFuture<QueryAssignmentResponse> okQueryAssignmentResponseFuture() {
        final ResponseCommon common = okResponseCommon();
        SettableFuture<QueryAssignmentResponse> future0 = SettableFuture.create();
        Assignment assignment = Assignment.newBuilder().setPartition(fakePbPartition0()).build();
        final QueryAssignmentResponse response =
                QueryAssignmentResponse.newBuilder().setCommon(common).addAssignments(assignment).build();
        future0.set(response);
        return future0;
    }

    protected ListenableFuture<EndTransactionResponse> okEndTransactionResponseFuture() {
        final ResponseCommon common = okResponseCommon();
        SettableFuture<EndTransactionResponse> future0 = SettableFuture.create();
        final EndTransactionResponse response =
                EndTransactionResponse.newBuilder().setCommon(common).build();
        future0.set(response);
        return future0;
    }

    protected GenericPollingResponse okGenericPollingResponseFuture() {
        final ResponseCommon common = okResponseCommon();
        return GenericPollingResponse.newBuilder().setCommon(common).build();
    }

    protected ListenableFuture<MultiplexingResponse> multiplexingResponseWithGenericPollingFuture(long delayMillis) {
        final SettableFuture<MultiplexingResponse> future0 = SettableFuture.create();
        final MultiplexingResponse response =
                MultiplexingResponse.newBuilder().setPollingResponse(okGenericPollingResponseFuture()).build();
        SCHEDULER.schedule(new Runnable() {
            @Override
            public void run() {
                future0.set(response);
            }
        }, delayMillis, TimeUnit.MILLISECONDS);
        return future0;
    }

    protected ListenableFuture<SendMessageResponse> okSendMessageResponseFuture() {
        final SettableFuture<SendMessageResponse> future0 = SettableFuture.create();
        final SendMessageResponse response = SendMessageResponse.newBuilder()
                                                                .setCommon(okResponseCommon()).build();
        future0.set(response);
        return future0;
    }

    protected MessageExt fakeMessageExt() {
        return fakeMessageExt(1);
    }

    protected MessageExt fakeMessageExt(int bodySize) {
        return fakeMessageExt(bodySize, false);
    }

    protected MessageExt fakeMessageExt(int bodySize, boolean corrupted) {
        final SystemAttribute systemAttribute = new SystemAttribute();
        systemAttribute.setMessageId(MessageIdGenerator.getInstance().next());
        systemAttribute.setReceiptHandle(FAKE_RECEIPT_HANDLE);
        systemAttribute.setDeliveryAttempt(1);
        final ConcurrentMap<String, String> userAttribute = new ConcurrentHashMap<String, String>();
        final byte[] bytes = RandomUtils.nextBytes(bodySize);
        final MessageImpl messageImpl = new MessageImpl(FAKE_TOPIC_0, systemAttribute, userAttribute, bytes, corrupted);
        return new MessageExt(messageImpl);
    }

    protected Message fakeMessage() {
        return fakeMessage(1);
    }

    protected apache.rocketmq.v1.Message fakeTransactionMessage0() throws UnsupportedEncodingException {
        final apache.rocketmq.v1.Message message = fakePbMessage0();
        final apache.rocketmq.v1.SystemAttribute systemAttribute =
                message.getSystemAttribute().toBuilder().setMessageType(apache.rocketmq.v1.MessageType.TRANSACTION)
                       .build();
        return message.toBuilder().setSystemAttribute(systemAttribute).build();
    }

    protected apache.rocketmq.v1.Message fakePbMessage0() throws UnsupportedEncodingException {
        apache.rocketmq.v1.SystemAttribute systemAttribute =
                apache.rocketmq.v1.SystemAttribute.newBuilder()
                                                  .setMessageType(apache.rocketmq.v1.MessageType.NORMAL)
                                                  .setMessageId(MessageIdGenerator.getInstance().next())
                                                  .setBornHost(FAKE_HOST_0)
                                                  .setBodyDigest(Digest.newBuilder()
                                                                       .setType(DigestType.CRC32)
                                                                       .setChecksum("9EF61F95")
                                                                       .build())
                                                  .build();
        return apache.rocketmq.v1.Message.newBuilder().setTopic(fakePbTopic0()).setBody(ByteString.copyFrom(
                "foobar", UtilAll.DEFAULT_CHARSET)).setSystemAttribute(systemAttribute).build();
    }

    protected Message fakeMessage(int bodySize) {
        final byte[] bytes = new byte[bodySize];
        for (int i = 0; i < bodySize; i++) {
            bytes[i] = 0x20;
        }
        return new Message(FAKE_TOPIC_0, FAKE_TAG_0, bytes);
    }

    protected Message fakeFifoMessage() {
        final Message message = fakeMessage();
        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(message);
        final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
        systemAttribute.setMessageType(MessageType.FIFO);
        systemAttribute.setMessageGroup(FAKE_MESSAGE_GROUP_0);
        return message;
    }

    protected Message fakeDelayMessage(int delayLevel, long delayTimestamp) {
        final Message message = fakeMessage();
        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(message);
        final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
        systemAttribute.setMessageType(MessageType.DELAY);
        systemAttribute.setDelayLevel(delayLevel);
        systemAttribute.setDeliveryTimeMillis(delayTimestamp);
        return message;
    }

    protected Message fakeTransactionMessage() {
        final Message message = fakeMessage();
        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(message);
        final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
        systemAttribute.setMessageType(MessageType.TRANSACTION);
        return message;
    }

    protected Message fakeDelayMessage() {
        return fakeDelayMessage(0, System.currentTimeMillis() + 10 * 1000);
    }
}
