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

package org.apache.rocketmq.client.java.tool;

import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.Assignment;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Digest;
import apache.rocketmq.v2.DigestType;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SendResultEntry;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.SystemProperties;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.java.impl.producer.ProducerSettings;
import org.apache.rocketmq.client.java.impl.producer.SendReceiptImpl;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;
import org.apache.rocketmq.client.java.message.MessageIdCodec;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.RequestIdGenerator;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.retry.CustomizedBackoffRetryPolicy;
import org.apache.rocketmq.client.java.retry.ExponentialBackoffRetryPolicy;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.Context;
import org.apache.rocketmq.client.java.rpc.RpcInvocation;
import org.apache.rocketmq.client.java.rpc.Signature;

public class TestBase {
    protected static final String FAKE_CLIENT_ID = "mbp@29848@cno0nhxy";

    protected static final String FAKE_TOPIC_0 = "foo-bar-topic-0";
    protected static final String FAKE_TOPIC_1 = "foo-bar-topic-1";

    protected static final byte[] FAKE_MESSAGE_BODY = "foobar".getBytes(StandardCharsets.UTF_8);

    protected static final String FAKE_TAG_0 = "foo-bar-tag-0";

    protected static final String FAKE_BROKER_NAME_0 = "foo-bar-broker-name-0";
    protected static final String FAKE_BROKER_NAME_1 = "foo-bar-broker-name-1";

    protected static final String FAKE_RECEIPT_HANDLE_0 = "foo-bar-handle-0";
    protected static final String FAKE_RECEIPT_HANDLE_1 = "foo-bar-handle-1";

    protected static final String FAKE_ACCESS_POINT = "127.0.0.1:9876";

    protected static final String FAKE_HOST_0 = "127.0.0.1";
    protected static final int FAKE_PORT_0 = 8080;

    protected static final String FAKE_HOST_1 = "127.0.0.2";
    protected static final int FAKE_PORT_1 = 8081;

    protected static final String FAKE_GROUP_0 = "foo-bar-group-0";
    protected static final String FAKE_TRANSACTION_ID = "foo-bar-transaction-id";

    protected static final long FAKE_OFFSET = 1;

    protected static final ScheduledExecutorService SCHEDULER =
        new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("TestScheduler"));

    protected static final ThreadPoolExecutor SINGLE_THREAD_POOL_EXECUTOR =
        new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(), new ThreadFactoryImpl("TestSingleWorker"));

    protected Address fakePbAddress0() {
        return fakePbAddress(FAKE_HOST_0, FAKE_PORT_0);
    }

    protected Address fakePbAddress1() {
        return fakePbAddress(FAKE_HOST_1, FAKE_PORT_1);
    }

    protected Address fakePbAddress(String host, int port) {
        return Address.newBuilder().setHost(host).setPort(port).build();
    }

    protected apache.rocketmq.v2.Endpoints fakePbEndpoints0() {
        return fakePbEndpoints(fakePbAddress0());
    }

    protected apache.rocketmq.v2.Endpoints fakePbEndpoints1() {
        return fakePbEndpoints(fakePbAddress1());
    }

    protected apache.rocketmq.v2.Endpoints fakePbEndpoints(Address address) {
        return apache.rocketmq.v2.Endpoints.newBuilder().setScheme(AddressScheme.IPv4).addAddresses(address).build();
    }

    protected Endpoints fakeEndpoints() {
        return new Endpoints(fakePbEndpoints0());
    }

    protected Context fakeRpcContext() {
        final Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(Signature.REQUEST_ID_KEY, Metadata.ASCII_STRING_MARSHALLER),
            RequestIdGenerator.getInstance().next());
        return new Context(fakeEndpoints(), metadata);
    }

    protected Message fakeMessage(String topic) {
        return new MessageBuilderImpl().setTopic(topic).setBody(RandomUtils.nextBytes(1)).build();
    }

    protected MessageViewImpl fakeMessageViewImpl() {
        return fakeMessageViewImpl(1, false);
    }

    protected MessageViewImpl fakeMessageViewImpl(MessageQueueImpl mq) {
        return fakeMessageViewImpl(mq, 1, false);
    }

    protected MessageViewImpl fakeMessageViewImpl(boolean corrupted) {
        return fakeMessageViewImpl(1, corrupted);
    }

    protected MessageViewImpl fakeMessageViewImpl(int bodySize, boolean corrupted) {
        return fakeMessageViewImpl(fakeMessageQueueImpl0(), bodySize, corrupted);
    }

    protected MessageViewImpl fakeMessageViewImpl(MessageQueueImpl mq, int bodySize, boolean corrupted) {
        MessageId messageId = MessageIdCodec.getInstance().nextMessageId();
        final byte[] body = RandomUtils.nextBytes(bodySize);
        Map<String, String> properties = new HashMap<>();
        List<String> keys = new ArrayList<>();
        return new MessageViewImpl(messageId, FAKE_TOPIC_0, body, null, null, null,
            keys, properties, FAKE_HOST_0, 1, 1, mq, FAKE_RECEIPT_HANDLE_0, null, 1, corrupted, null);
    }

    protected MessageQueueImpl fakeMessageQueueImpl(String topic) {
        return new MessageQueueImpl(fakePbMessageQueue0(Resource.newBuilder().setName(topic).build()));
    }

    protected MessageQueueImpl fakeMessageQueueImpl0() {
        return new MessageQueueImpl(fakePbMessageQueue0());
    }

    protected MessageQueueImpl fakeMessageQueueImpl1() {
        return new MessageQueueImpl(fakePbMessageQueue1());
    }

    protected Resource fakePbTopic0() {
        return Resource.newBuilder().setName(FAKE_TOPIC_0).build();
    }

    protected Broker fakePbBroker0() {
        return Broker.newBuilder().setEndpoints(fakePbEndpoints0()).setName(FAKE_BROKER_NAME_0)
            .setId(Utilities.MASTER_BROKER_ID).build();
    }

    protected Broker fakePbBroker1() {
        return Broker.newBuilder().setEndpoints(fakePbEndpoints1()).setName(FAKE_BROKER_NAME_1)
            .setId(Utilities.MASTER_BROKER_ID).build();

    }

    protected MessageQueue fakePbMessageQueue0() {
        return fakePbMessageQueue0(fakePbTopic0());
    }

    protected MessageQueue fakePbMessageQueue1() {
        return fakePbMessageQueue1(fakePbTopic0());
    }

    protected MessageQueue fakePbMessageQueue0(Resource topicResource) {
        return MessageQueue.newBuilder().setTopic(topicResource).setBroker(fakePbBroker0())
            .setPermission(Permission.READ_WRITE).build();
    }

    protected MessageQueue fakePbMessageQueue1(Resource topicResource) {
        return MessageQueue.newBuilder().setTopic(topicResource).setBroker(fakePbBroker1())
            .setPermission(Permission.READ_WRITE).build();
    }

    protected ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>>
    okChangeInvisibleDurationCtxFuture() {
        Status status = Status.newBuilder().setCode(Code.OK).build();
        final ChangeInvisibleDurationResponse response =
            ChangeInvisibleDurationResponse.newBuilder().setStatus(status).build();
        return Futures.immediateFuture(new RpcInvocation<>(response, fakeRpcContext()));
    }

    protected ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>>
    changInvisibleDurationCtxFuture(Code code) {
        Status status = Status.newBuilder().setCode(code).build();
        final ChangeInvisibleDurationResponse response =
            ChangeInvisibleDurationResponse.newBuilder().setStatus(status).build();
        return Futures.immediateFuture(new RpcInvocation<>(response, fakeRpcContext()));
    }

    protected ListenableFuture<RpcInvocation<QueryAssignmentResponse>> okQueryAssignmentResponseFuture() {
        final SettableFuture<QueryAssignmentResponse> future = SettableFuture.create();
        final Status status = Status.newBuilder().setCode(Code.OK).build();
        Assignment assignment = Assignment.newBuilder().setMessageQueue(fakePbMessageQueue0()).build();
        QueryAssignmentResponse response = QueryAssignmentResponse.newBuilder().setStatus(status)
            .addAssignments(assignment).build();
        return Futures.immediateFuture(new RpcInvocation<>(response, fakeRpcContext()));
    }

    protected ListenableFuture<RpcInvocation<QueryAssignmentResponse>> okEmptyQueryAssignmentResponseFuture() {
        final SettableFuture<QueryAssignmentResponse> future = SettableFuture.create();
        final Status status = Status.newBuilder().setCode(Code.OK).build();
        final QueryAssignmentResponse response = QueryAssignmentResponse.newBuilder().setStatus(status).build();
        return Futures.immediateFuture(new RpcInvocation<>(response, fakeRpcContext()));
    }

    protected Map<String, FilterExpression> createSubscriptionExpressions(String topic) {
        Map<String, FilterExpression> map = new HashMap<>();
        final FilterExpression filterExpression = FilterExpression.SUB_ALL;
        map.put(topic, filterExpression);
        return map;
    }

    protected ListenableFuture<RpcInvocation<AckMessageResponse>> ackMessageResponseFuture(Code code) {
        final Status status = Status.newBuilder().setCode(code).build();
        final AckMessageResponse response = AckMessageResponse.newBuilder().setStatus(status).build();
        return Futures.immediateFuture(new RpcInvocation<>(response, fakeRpcContext()));
    }

    protected ListenableFuture<RpcInvocation<AckMessageResponse>> okAckMessageResponseFuture() {
        final Status status = Status.newBuilder().setCode(Code.OK).build();
        final AckMessageResponse response = AckMessageResponse.newBuilder().setStatus(status).build();
        return Futures.immediateFuture(new RpcInvocation<>(response, fakeRpcContext()));
    }

    protected ListenableFuture<RpcInvocation<ForwardMessageToDeadLetterQueueResponse>>
    okForwardMessageToDeadLetterQueueResponseFuture() {
        final Status status = Status.newBuilder().setCode(Code.OK).build();
        final ForwardMessageToDeadLetterQueueResponse response =
            ForwardMessageToDeadLetterQueueResponse.newBuilder().setStatus(status).build();
        return Futures.immediateFuture(new RpcInvocation<>(response, fakeRpcContext()));
    }

    protected ListenableFuture<RpcInvocation<SendMessageResponse>> okSendMessageResponseFutureWithSingleEntry() {
        final Status status = Status.newBuilder().setCode(Code.OK).build();
        final String messageId = MessageIdCodec.getInstance().nextMessageId().toString();
        SendResultEntry entry = SendResultEntry.newBuilder().setMessageId(messageId)
            .setTransactionId(FAKE_TRANSACTION_ID).setStatus(status).setOffset(1).build();
        SendMessageResponse response = SendMessageResponse.newBuilder().setStatus(status).addEntries(entry).build();
        return Futures.immediateFuture(new RpcInvocation<>(response, fakeRpcContext()));
    }

    protected ListenableFuture<RpcInvocation<SendMessageResponse>> failureSendMessageResponseFuture() {
        final Status status = Status.newBuilder().setCode(Code.FORBIDDEN).build();
        SendResultEntry sendResultEntry = SendResultEntry.newBuilder().setStatus(status).setStatus(status).build();
        SendMessageResponse response = SendMessageResponse.newBuilder().setStatus(status)
            .addEntries(sendResultEntry).build();
        return Futures.immediateFuture(new RpcInvocation<>(response, fakeRpcContext()));
    }

    protected ListenableFuture<SendMessageResponse> okBatchSendMessageResponseFuture() {
        final Status status = Status.newBuilder().setCode(Code.OK).build();
        SettableFuture<SendMessageResponse> future0 = SettableFuture.create();
        final String messageId = MessageIdCodec.getInstance().nextMessageId().toString();
        SendResultEntry entry0 = SendResultEntry.newBuilder().setMessageId(messageId).setStatus(status)
            .setOffset(1).build();
        SendResultEntry entry1 = SendResultEntry.newBuilder().setMessageId(messageId).setStatus(status)
            .setOffset(2).build();
        SendMessageResponse response = SendMessageResponse.newBuilder().setStatus(status).addEntries(entry0)
            .addEntries(entry1).build();
        future0.set(response);
        return future0;
    }

    protected apache.rocketmq.v2.Message fakePbMessage(String topic) {
        final Digest digest = Digest.newBuilder().setType(DigestType.CRC32).setChecksum("9EF61F95").build();
        SystemProperties systemProperties = SystemProperties.newBuilder().setMessageType(MessageType.NORMAL)
            .setMessageId(MessageIdCodec.getInstance().nextMessageId().toString())
            .setBornHost(FAKE_HOST_0)
            .setBodyDigest(digest)
            .build();
        Resource resource = Resource.newBuilder().setName(topic).build();
        return apache.rocketmq.v2.Message.newBuilder().setSystemProperties(systemProperties)
            .setTopic(resource).setBody(ByteString.copyFrom("foobar", StandardCharsets.UTF_8))
            .setSystemProperties(systemProperties).build();
    }

    protected ListenableFuture<RpcInvocation<Iterator<ReceiveMessageResponse>>> okReceiveMessageResponsesFuture(
        String topic, int messageCount) {
        final Status status = Status.newBuilder().setCode(Code.OK).build();
        final apache.rocketmq.v2.Message message = fakePbMessage(topic);
        List<ReceiveMessageResponse> responses = new ArrayList<>();
        ReceiveMessageResponse statusResponse = ReceiveMessageResponse.newBuilder().setStatus(status).build();
        responses.add(statusResponse);
        for (int i = 0; i < messageCount; i++) {
            ReceiveMessageResponse messageResponse = ReceiveMessageResponse.newBuilder().setMessage(message).build();
            responses.add(messageResponse);
        }
        return Futures.immediateFuture(new RpcInvocation<>(responses.iterator(), fakeRpcContext()));
    }

    protected ListenableFuture<EndTransactionResponse> okEndTransactionResponseFuture() {
        SettableFuture<EndTransactionResponse> future = SettableFuture.create();
        final Status status = Status.newBuilder().setCode(Code.OK).build();
        EndTransactionResponse response = EndTransactionResponse.newBuilder().setStatus(status).build();
        future.set(response);
        return future;
    }

    protected CustomizedBackoffRetryPolicy createCustomizedBackoffRetryPolicy(int maxAttempts) {
        List<Duration> durations = new ArrayList<>();
        durations.add(Duration.ofSeconds(1));
        return new CustomizedBackoffRetryPolicy(durations, maxAttempts);
    }

    protected ExponentialBackoffRetryPolicy fakeExponentialBackoffRetryPolicy() {
        return new ExponentialBackoffRetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(3), 2);
    }

    protected ProducerSettings fakeProducerSettings() {
        return new ProducerSettings(FAKE_CLIENT_ID, fakeEndpoints(), fakeExponentialBackoffRetryPolicy(),
            Duration.ofSeconds(1), new HashSet<>());
    }

    protected SendReceiptImpl fakeSendReceiptImpl(
        MessageQueueImpl mq) throws ExecutionException, InterruptedException, ClientException {
        final ListenableFuture<RpcInvocation<SendMessageResponse>> future =
            okSendMessageResponseFutureWithSingleEntry();
        final List<SendReceiptImpl> receipts = SendReceiptImpl.processResponseInvocation(mq, future.get());
        return receipts.iterator().next();
    }
}
