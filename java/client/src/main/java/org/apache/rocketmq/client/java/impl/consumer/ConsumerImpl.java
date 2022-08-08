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

package org.apache.rocketmq.client.java.impl.consumer;

import apache.rocketmq.v2.AckMessageEntry;
import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import io.grpc.Metadata;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.impl.ClientImpl;
import org.apache.rocketmq.client.java.message.MessageCommon;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.RpcInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
abstract class ConsumerImpl extends ClientImpl {
    static final Pattern CONSUMER_GROUP_PATTERN = Pattern.compile("^[%a-zA-Z0-9_-]+$");
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerImpl.class);
    private final String consumerGroup;

    ConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup, Set<String> topics) {
        super(clientConfiguration, topics);
        this.consumerGroup = consumerGroup;
    }

    @SuppressWarnings("SameParameterValue")
    protected ListenableFuture<ReceiveMessageResult> receiveMessage(ReceiveMessageRequest request,
        MessageQueueImpl mq, Duration awaitDuration) {
        List<MessageViewImpl> messages = new ArrayList<>();
        try {
            Metadata metadata = sign();
            final Endpoints endpoints = mq.getBroker().getEndpoints();
            final Duration tolerance = clientConfiguration.getRequestTimeout();
            final Duration timeout = Duration.ofNanos(awaitDuration.toNanos() + tolerance.toNanos());
            final ListenableFuture<RpcInvocation<Iterator<ReceiveMessageResponse>>> future =
                clientManager.receiveMessage(endpoints, metadata, request, timeout);
            return Futures.transformAsync(future, invocation -> {
                final Iterator<ReceiveMessageResponse> it = invocation.getResponse();
                Status status = Status.newBuilder().setCode(Code.INTERNAL_SERVER_ERROR)
                    .setMessage("status was not set by server")
                    .build();
                Timestamp deliveryTimestampFromRemote = null;
                List<Message> messageList = new ArrayList<>();
                while (it.hasNext()) {
                    final ReceiveMessageResponse response = it.next();
                    switch (response.getContentCase()) {
                        case STATUS:
                            status = response.getStatus();
                            break;
                        case MESSAGE:
                            messageList.add(response.getMessage());
                            break;
                        case DELIVERY_TIMESTAMP:
                            deliveryTimestampFromRemote = response.getDeliveryTimestamp();
                            break;
                        default:
                            LOGGER.warn("[Bug] Not recognized content for receive message response, mq={}, " +
                                "clientId={}, resp={}", mq, clientId, response);
                    }
                }
                for (Message message : messageList) {
                    final MessageViewImpl view = MessageViewImpl.fromProtobuf(message, mq, deliveryTimestampFromRemote);
                    messages.add(view);
                }
                StatusChecker.check(status, invocation);
                final ReceiveMessageResult receiveMessageResult = new ReceiveMessageResult(endpoints, messages);
                return Futures.immediateFuture(receiveMessageResult);
            }, MoreExecutors.directExecutor());
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    private AckMessageRequest wrapAckMessageRequest(MessageViewImpl messageView) {
        final Resource topicResource = Resource.newBuilder().setName(messageView.getTopic()).build();
        final AckMessageEntry entry = AckMessageEntry.newBuilder()
            .setMessageId(messageView.getMessageId().toString())
            .setReceiptHandle(messageView.getReceiptHandle())
            .build();
        return AckMessageRequest.newBuilder().setGroup(getProtobufGroup()).setTopic(topicResource)
            .setGroup(getProtobufGroup()).addEntries(entry).build();
    }

    private ChangeInvisibleDurationRequest wrapChangeInvisibleDuration(MessageViewImpl messageView,
        Duration invisibleDuration) {
        final Resource topicResource = Resource.newBuilder().setName(messageView.getTopic()).build();
        return ChangeInvisibleDurationRequest.newBuilder().setGroup(getProtobufGroup()).setTopic(topicResource)
            .setReceiptHandle(messageView.getReceiptHandle())
            .setInvisibleDuration(Durations.fromNanos(invisibleDuration.toNanos()))
            .setMessageId(messageView.getMessageId().toString()).build();

    }

    protected ListenableFuture<RpcInvocation<AckMessageResponse>> ackMessage(MessageViewImpl messageView) {
        final Endpoints endpoints = messageView.getEndpoints();
        ListenableFuture<RpcInvocation<AckMessageResponse>> future;

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final List<MessageCommon> messageCommons = Collections.singletonList(messageView.getMessageCommon());
        doBefore(MessageHookPoints.ACK, messageCommons);
        try {
            final AckMessageRequest request = wrapAckMessageRequest(messageView);
            final Metadata metadata = sign();
            future = clientManager.ackMessage(endpoints, metadata, request, clientConfiguration.getRequestTimeout());
        } catch (Throwable t) {
            final SettableFuture<RpcInvocation<AckMessageResponse>> future0 = SettableFuture.create();
            future0.setException(t);
            future = future0;
        }
        Futures.addCallback(future, new FutureCallback<RpcInvocation<AckMessageResponse>>() {
            @Override
            public void onSuccess(RpcInvocation<AckMessageResponse> invocation) {
                final AckMessageResponse response = invocation.getResponse();
                final Status status = response.getStatus();
                final Code code = status.getCode();
                final Duration duration = stopwatch.elapsed();
                MessageHookPointsStatus messageHookPointsStatus = Code.OK.equals(code) ?
                    MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
                doAfter(MessageHookPoints.ACK, messageCommons, duration, messageHookPointsStatus);
            }

            @Override
            public void onFailure(Throwable t) {
                final Duration duration = stopwatch.elapsed();
                doAfter(MessageHookPoints.ACK, messageCommons, duration, MessageHookPointsStatus.ERROR);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    public ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> changeInvisibleDuration(
        MessageViewImpl messageView, Duration invisibleDuration) {
        final Endpoints endpoints = messageView.getEndpoints();
        ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> future;

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final List<MessageCommon> messageCommons = Collections.singletonList(messageView.getMessageCommon());
        doBefore(MessageHookPoints.CHANGE_INVISIBLE_DURATION, messageCommons);
        try {
            final ChangeInvisibleDurationRequest request = wrapChangeInvisibleDuration(messageView, invisibleDuration);
            final Metadata metadata = sign();
            future = clientManager.changeInvisibleDuration(endpoints, metadata, request,
                clientConfiguration.getRequestTimeout());
        } catch (Throwable t) {
            final SettableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> future0 = SettableFuture.create();
            future0.setException(t);
            future = future0;
        }
        final MessageId messageId = messageView.getMessageId();
        Futures.addCallback(future, new FutureCallback<RpcInvocation<ChangeInvisibleDurationResponse>>() {
            @Override
            public void onSuccess(RpcInvocation<ChangeInvisibleDurationResponse> invocation) {
                final ChangeInvisibleDurationResponse response = invocation.getResponse();
                final Status status = response.getStatus();
                final Code code = status.getCode();
                final Duration duration = stopwatch.elapsed();
                MessageHookPointsStatus messageHookPointsStatus = Code.OK.equals(code) ?
                    MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
                if (!Code.OK.equals(code)) {
                    LOGGER.error("Failed to change message invisible duration, messageId={}, endpoints={}, code={}, " +
                        "status message=[{}], clientId={}", messageId, endpoints, code, status.getMessage(), clientId);
                }
                doAfter(MessageHookPoints.CHANGE_INVISIBLE_DURATION, messageCommons, duration, messageHookPointsStatus);
            }

            @Override
            public void onFailure(Throwable t) {
                final Duration duration = stopwatch.elapsed();
                doAfter(MessageHookPoints.ACK, messageCommons, duration, MessageHookPointsStatus.ERROR);
                LOGGER.error("Exception raised during message acknowledgement, messageId={}, endpoints={}, clientId={}",
                    messageId, endpoints, clientId, t);

            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        return HeartbeatRequest.newBuilder().setGroup(getProtobufGroup()).build();
    }

    protected Resource getProtobufGroup() {
        return Resource.newBuilder().setName(consumerGroup).build();
    }

    @Override
    public NotifyClientTerminationRequest wrapNotifyClientTerminationRequest() {
        return NotifyClientTerminationRequest.newBuilder().setGroup(getProtobufGroup()).build();
    }

    private apache.rocketmq.v2.FilterExpression wrapFilterExpression(FilterExpression filterExpression) {
        apache.rocketmq.v2.FilterExpression.Builder expressionBuilder =
            apache.rocketmq.v2.FilterExpression.newBuilder();
        expressionBuilder.setExpression(filterExpression.getExpression());
        switch (filterExpression.getFilterExpressionType()) {
            case SQL92:
                expressionBuilder.setType(FilterType.SQL);
                break;
            case TAG:
            default:
                expressionBuilder.setType(FilterType.TAG);
        }
        return expressionBuilder.build();
    }

    public ReceiveMessageRequest wrapReceiveMessageRequest(int batchSize, MessageQueueImpl mq,
        FilterExpression filterExpression) {
        return ReceiveMessageRequest.newBuilder().setGroup(getProtobufGroup())
            .setMessageQueue(mq.toProtobuf()).setFilterExpression(wrapFilterExpression(filterExpression))
            .setBatchSize(batchSize).setAutoRenew(true).build();
    }

    public ReceiveMessageRequest wrapReceiveMessageRequest(int batchSize, MessageQueueImpl mq,
        FilterExpression filterExpression, Duration invisibleDuration) {
        final com.google.protobuf.Duration duration = Durations.fromNanos(invisibleDuration.toNanos());
        return ReceiveMessageRequest.newBuilder().setGroup(getProtobufGroup())
            .setMessageQueue(mq.toProtobuf()).setFilterExpression(wrapFilterExpression(filterExpression))
            .setBatchSize(batchSize).setAutoRenew(false).setInvisibleDuration(duration).build();
    }
}
