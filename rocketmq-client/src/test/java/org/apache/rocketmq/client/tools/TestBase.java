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
import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.Digest;
import apache.rocketmq.v1.DigestType;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.Endpoints;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.GenericPollingResponse;
import apache.rocketmq.v1.MultiplexingResponse;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.Permission;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.ResponseCommon;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import com.google.rpc.Status;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageAccessor;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageIdGenerator;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.apache.rocketmq.utility.UtilAll;

public class TestBase {
    protected Random random = new Random();

    protected String dummyArn0 = "TestArn0";

    protected String dummyTopic0 = "TestTopic0";
    protected String dummyTopic1 = "TestTopic1";

    protected String dummyBrokerName0 = "TestBrokerName";

    protected String dummyMessageGroup0 = "TestMessageGroup";

    protected String dummyClientId0 = "TestClientId0";

    protected String dummyGroup0 = "TestGroup0";
    protected String dummyGroup1 = "TestGroup1";
    protected String dummyGroup2 = "TestGroup2";

    protected String dummyNameServerAddr0 = "11.167.164.105:9876";

    protected String dummyTagExpression0 = FilterExpression.TAG_EXPRESSION_SUB_ALL;

    protected String dummyTag0 = "TestTagA";

    protected String dummyHost0 = "127.0.0.1";

    protected int dummyPort0 = 8080;

    protected String dummyTransactionId = "123456";

    protected String dummyReceiptHandle = "handle";

    protected Resource dummyTopicResource0() {
        return Resource.newBuilder().setArn(dummyArn0).setName(dummyTopic0).build();
    }

    protected Address dummyAddress() {
        return Address.newBuilder().setHost(dummyHost0).setPort(dummyPort0).build();
    }

    protected Endpoints dummyEndpoints0() {
        return Endpoints.newBuilder().setScheme(AddressScheme.IPv4).addAddresses(dummyAddress()).build();
    }

    protected Broker dummyBroker0() {
        return Broker.newBuilder().setName(dummyBrokerName0)
                     .setId(MixAll.MASTER_BROKER_ID)
                     .setEndpoints(dummyEndpoints0())
                     .build();
    }

    protected apache.rocketmq.v1.Partition dummyPartition0() {
        return apache.rocketmq.v1.Partition.newBuilder()
                                           .setTopic(dummyTopicResource0())
                                           .setBroker(dummyBroker0())
                                           .setPermission(Permission.READ_WRITE)
                                           .build();
    }

    protected apache.rocketmq.v1.Partition dummyPartition0(Permission permission) {
        return apache.rocketmq.v1.Partition.newBuilder()
                                           .setTopic(dummyTopicResource0())
                                           .setBroker(dummyBroker0())
                                           .setPermission(permission)
                                           .build();
    }

    protected MessageQueue dummyMessageQueue() {
        return new MessageQueue(new Partition(dummyPartition0()));
    }

    protected ThreadPoolExecutor singleThreadPoolExecutor() {
        return new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                                      new ThreadFactoryImpl("TestSingleWorker"));
    }

    protected ThreadPoolExecutor sendCallbackExecutor() {
        return new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                                      new ThreadFactoryImpl("TestSendCallback"));
    }

    protected ScheduledExecutorService scheduler() {
        return new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("TestScheduler"));
    }

    protected TopicRouteData dummyTopicRouteData(Permission permission) {
        List<apache.rocketmq.v1.Partition> partitionList = new ArrayList<apache.rocketmq.v1.Partition>();
        partitionList.add(dummyPartition0(permission));
        return new TopicRouteData(partitionList);
    }

    protected ResponseCommon successResponseCommon() {
        final Status status = Status.newBuilder().setCode(Code.OK_VALUE).build();
        return ResponseCommon.newBuilder().setStatus(status).build();
    }

    protected ListenableFuture<AckMessageResponse> successAckMessageResponseFuture() {
        final ResponseCommon common = successResponseCommon();
        SettableFuture<AckMessageResponse> future0 = SettableFuture.create();
        final AckMessageResponse response = AckMessageResponse.newBuilder().setCommon(common).build();
        future0.set(response);
        return future0;
    }

    protected ListenableFuture<NackMessageResponse> successNackMessageResponseFuture() {
        final ResponseCommon common = successResponseCommon();
        SettableFuture<NackMessageResponse> future0 = SettableFuture.create();
        final NackMessageResponse response = NackMessageResponse.newBuilder().setCommon(common).build();
        future0.set(response);
        return future0;
    }

    protected ListenableFuture<ForwardMessageToDeadLetterQueueResponse>
        successForwardMessageToDeadLetterQueueResponseListenableFuture() {
        final ResponseCommon common = successResponseCommon();
        SettableFuture<ForwardMessageToDeadLetterQueueResponse> future0 = SettableFuture.create();
        final ForwardMessageToDeadLetterQueueResponse response =
                ForwardMessageToDeadLetterQueueResponse.newBuilder().setCommon(common).build();
        future0.set(response);
        return future0;
    }

    protected ListenableFuture<QueryRouteResponse> successQueryRouteResponse() {
        final ResponseCommon common = successResponseCommon();
        SettableFuture<QueryRouteResponse> future0 = SettableFuture.create();
        final QueryRouteResponse response =
                QueryRouteResponse.newBuilder().setCommon(common).addPartitions(dummyPartition0()).build();
        future0.set(response);
        return future0;
    }

    protected ListenableFuture<EndTransactionResponse> successEndTransactionResponse() {
        final ResponseCommon common = successResponseCommon();
        SettableFuture<EndTransactionResponse> future0 = SettableFuture.create();
        final EndTransactionResponse response =
                EndTransactionResponse.newBuilder().setCommon(common).build();
        future0.set(response);
        return future0;
    }

    protected GenericPollingResponse successGenericPollingResponse() {
        final ResponseCommon common = successResponseCommon();
        return GenericPollingResponse.newBuilder().setCommon(common).build();
    }

    protected ListenableFuture<MultiplexingResponse> multiplexingResponseWithGenericPolling(long delay,
                                                                                            TimeUnit timeUnit) {
        final SettableFuture<MultiplexingResponse> future0 = SettableFuture.create();
        final MultiplexingResponse response =
                MultiplexingResponse.newBuilder().setPollingResponse(successGenericPollingResponse()).build();
        scheduler().schedule(new Runnable() {
            @Override
            public void run() {
                future0.set(response);
            }
        }, delay, timeUnit);
        return future0;
    }

    protected ListenableFuture<SendMessageResponse> successSendMessageResponse() {
        final SettableFuture<SendMessageResponse> future0 = SettableFuture.create();
        final SendMessageResponse response = SendMessageResponse.newBuilder()
                                                                .setCommon(successResponseCommon()).build();
        future0.set(response);
        return future0;
    }

    protected ListenableFuture<ReceiveMessageResponse>
        successReceiveMessageResponse(List<apache.rocketmq.v1.Message> messageList) {
        final SettableFuture<ReceiveMessageResponse> future0 = SettableFuture.create();
        final ReceiveMessageResponse.Builder builder = ReceiveMessageResponse.newBuilder()
                                                                             .setCommon(successResponseCommon());
        future0.set(builder.addAllMessages(messageList).build());
        return future0;
    }

    protected MessageExt dummyMessageExt() {
        return dummyMessageExt(1);
    }

    protected MessageExt dummyMessageExt(int bodySize) {
        final SystemAttribute systemAttribute = new SystemAttribute();
        systemAttribute.setMessageId(MessageIdGenerator.getInstance().next());
        systemAttribute.setReceiptHandle(dummyReceiptHandle);
        systemAttribute.setDeliveryAttempt(1);
        final ConcurrentMap<String, String> userAttribute = new ConcurrentHashMap<String, String>();
        final byte[] bytes = new byte[bodySize];
        random.nextBytes(bytes);
        final MessageImpl messageImpl = new MessageImpl(dummyTopic0, systemAttribute, userAttribute, bytes);
        return new MessageExt(messageImpl);
    }

    protected Message dummyMessage() {
        return dummyMessage(1);
    }

    protected apache.rocketmq.v1.Message dummyTransactionMessage0() throws UnsupportedEncodingException {
        final apache.rocketmq.v1.Message message = dummyMessage0();
        final apache.rocketmq.v1.SystemAttribute systemAttribute =
                message.getSystemAttribute().toBuilder().setMessageType(apache.rocketmq.v1.MessageType.TRANSACTION)
                       .build();
        return message.toBuilder().setSystemAttribute(systemAttribute).build();
    }

    protected apache.rocketmq.v1.Message dummyMessage0() throws UnsupportedEncodingException {
        apache.rocketmq.v1.SystemAttribute systemAttribute =
                apache.rocketmq.v1.SystemAttribute.newBuilder()
                                                  .setMessageType(apache.rocketmq.v1.MessageType.NORMAL)
                                                  .setMessageId(MessageIdGenerator.getInstance().next())
                                                  .setBornHost(dummyHost0)
                                                  .setBodyDigest(Digest.newBuilder()
                                                                       .setType(DigestType.CRC32)
                                                                       .setChecksum("9EF61F95")
                                                                       .build())
                                                  .build();
        return apache.rocketmq.v1.Message.newBuilder().setTopic(dummyTopicResource0()).setBody(ByteString.copyFrom(
                "foobar", UtilAll.DEFAULT_CHARSET)).setSystemAttribute(systemAttribute).build();
    }

    protected Message dummyMessage(int bodySize) {
        final byte[] bytes = new byte[bodySize];
        for (int i = 0; i < bodySize; i++) {
            bytes[i] = 0x20;
        }
        return new Message(dummyTopic0, dummyTag0, bytes);
    }

    protected Message dummyFifoMessage() {
        final Message message = dummyMessage();
        final MessageImpl messageImpl = MessageAccessor.getMessageImpl(message);
        final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
        systemAttribute.setMessageType(MessageType.FIFO);
        systemAttribute.setMessageGroup(dummyMessageGroup0);
        return message;
    }

    protected Message dummyDelayMessage(int delayLevel, long delayTimestamp) {
        final Message message = dummyMessage();
        final MessageImpl messageImpl = MessageAccessor.getMessageImpl(message);
        final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
        systemAttribute.setMessageType(MessageType.DELAY);
        systemAttribute.setDelayLevel(delayLevel);
        systemAttribute.setDeliveryTimeMillis(delayTimestamp);
        return message;
    }

    protected Message dummyTransactionMessage() {
        final Message message = dummyMessage();
        final MessageImpl messageImpl = MessageAccessor.getMessageImpl(message);
        final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
        systemAttribute.setMessageType(MessageType.TRANSACTION);
        return message;
    }

    protected Message dummyDelayMessage() {
        return dummyDelayMessage(0, System.currentTimeMillis() + 10 * 1000);
    }
}
