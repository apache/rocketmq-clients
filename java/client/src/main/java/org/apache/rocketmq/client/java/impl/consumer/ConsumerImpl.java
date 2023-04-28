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
import apache.rocketmq.v2.GetOffsetRequest;
import apache.rocketmq.v2.GetOffsetResponse;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.PullMessageRequest;
import apache.rocketmq.v2.PullMessageResponse;
import apache.rocketmq.v2.QueryOffsetPolicy;
import apache.rocketmq.v2.QueryOffsetRequest;
import apache.rocketmq.v2.QueryOffsetResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.UpdateOffsetRequest;
import apache.rocketmq.v2.UpdateOffsetResponse;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContextImpl;
import org.apache.rocketmq.client.java.impl.ClientImpl;
import org.apache.rocketmq.client.java.impl.ClientManager;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.GeneralMessageImpl;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
abstract class ConsumerImpl extends ClientImpl {
    static final Pattern CONSUMER_GROUP_PATTERN = Pattern.compile("^[%a-zA-Z0-9_-]+$");
    private static final Logger log = LoggerFactory.getLogger(ConsumerImpl.class);
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
            final Endpoints endpoints = mq.getBroker().getEndpoints();
            final Duration tolerance = clientConfiguration.getRequestTimeout();
            final Duration timeout = awaitDuration.plus(tolerance);
            final ClientManager clientManager = this.getClientManager();
            final RpcFuture<ReceiveMessageRequest, List<ReceiveMessageResponse>> future =
                clientManager.receiveMessage(endpoints, request, timeout);
            return Futures.transformAsync(future, responses -> {
                Status status = Status.newBuilder().setCode(Code.INTERNAL_SERVER_ERROR)
                    .setMessage("status was not set by server")
                    .build();
                Long transportDeliveryTimestamp = null;
                List<Message> messageList = new ArrayList<>();
                for (ReceiveMessageResponse response : responses) {
                    switch (response.getContentCase()) {
                        case STATUS:
                            status = response.getStatus();
                            break;
                        case MESSAGE:
                            messageList.add(response.getMessage());
                            break;
                        case DELIVERY_TIMESTAMP:
                            final Timestamp deliveryTimestamp = response.getDeliveryTimestamp();
                            transportDeliveryTimestamp = Timestamps.toMillis(deliveryTimestamp);
                            break;
                        default:
                            log.warn("[Bug] Not recognized content for receive message response, mq={}, " +
                                "clientId={}, response={}", mq, clientId, response);
                    }
                }
                for (Message message : messageList) {
                    final MessageViewImpl view = MessageViewImpl.fromProtobuf(message, mq, transportDeliveryTimestamp);
                    messages.add(view);
                }
                StatusChecker.check(status, future);
                final ReceiveMessageResult receiveMessageResult = new ReceiveMessageResult(endpoints, messages);
                return Futures.immediateFuture(receiveMessageResult);
            }, MoreExecutors.directExecutor());
        } catch (Throwable t) {
            // Should never reach here.
            log.error("[Bug] Exception raised during message receiving, mq={}, clientId={}", mq, clientId, t);
            return Futures.immediateFailedFuture(t);
        }
    }

    @SuppressWarnings("SameParameterValue")
    protected ListenableFuture<PullMessageResult> pullMessage(PullMessageRequest request, MessageQueueImpl mq,
        Duration awaitDuration) {
        List<MessageViewImpl> messages = new ArrayList<>();
        try {
            final Endpoints endpoints = mq.getBroker().getEndpoints();
            final Duration tolerance = clientConfiguration.getRequestTimeout();
            final Duration timeout = awaitDuration.plus(tolerance);
            final ClientManager clientManager = this.getClientManager();
            final RpcFuture<PullMessageRequest, List<PullMessageResponse>> future =
                clientManager.pullMessage(endpoints, request, timeout);
            return Futures.transformAsync(future, responses -> {
                Status status = Status.newBuilder().setCode(Code.INTERNAL_SERVER_ERROR)
                    .setMessage("status was not set by server")
                    .build();
                List<Message> messageList = new ArrayList<>();
                long nextOffset = 0;
                for (PullMessageResponse response : responses) {
                    switch (response.getContentCase()) {
                        case STATUS:
                            status = response.getStatus();
                            break;
                        case MESSAGE:
                            messageList.add(response.getMessage());
                            break;
                        case NEXT_OFFSET:
                            nextOffset = response.getNextOffset();
                            break;
                        default:
                            log.warn("[Bug] Not recognized content for pull message response, mq={}, clientId={}," +
                                " response={}", mq, clientId, response);
                    }
                }
                for (Message message : messageList) {
                    final MessageViewImpl view = MessageViewImpl.fromProtobuf(message, mq);
                    messages.add(view);
                }
                StatusChecker.check(status, future);
                if (Code.ILLEGAL_OFFSET.equals(status.getCode())) {
                    log.warn("Offset is illegal, requested offset={}, next offset={}, mq={}, clientId={}",
                        request.getOffset(), nextOffset, mq, clientId);
                }
                final PullMessageResult pullMessageResult = new PullMessageResult(endpoints, messages, nextOffset);
                return Futures.immediateFuture(pullMessageResult);
            }, MoreExecutors.directExecutor());
        } catch (Throwable t) {
            // Should never reach here.
            log.error("[Bug] Exception raised during message pulling, mq={}, clientId={}", mq, clientId, t);
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
            .addEntries(entry).build();
    }

    private ChangeInvisibleDurationRequest wrapChangeInvisibleDuration(MessageViewImpl messageView,
        Duration invisibleDuration) {
        final Resource topicResource = Resource.newBuilder().setName(messageView.getTopic()).build();
        return ChangeInvisibleDurationRequest.newBuilder().setGroup(getProtobufGroup()).setTopic(topicResource)
            .setReceiptHandle(messageView.getReceiptHandle())
            .setInvisibleDuration(Durations.fromNanos(invisibleDuration.toNanos()))
            .setMessageId(messageView.getMessageId().toString()).build();

    }

    protected RpcFuture<AckMessageRequest, AckMessageResponse> ackMessage(MessageViewImpl messageView) {
        final Endpoints endpoints = messageView.getEndpoints();
        RpcFuture<AckMessageRequest, AckMessageResponse> future;
        final List<GeneralMessage> generalMessages = Collections.singletonList(new GeneralMessageImpl(messageView));
        final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.ACK);
        doBefore(context, generalMessages);
        try {
            final AckMessageRequest request = wrapAckMessageRequest(messageView);
            final Duration requestTimeout = clientConfiguration.getRequestTimeout();
            future = this.getClientManager().ackMessage(endpoints, request, requestTimeout);
        } catch (Throwable t) {
            future = new RpcFuture<>(t);
        }
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                final Status status = response.getStatus();
                final Code code = status.getCode();
                MessageHookPointsStatus hookPointsStatus = Code.OK.equals(code) ?
                    MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
                MessageInterceptorContextImpl context0 = new MessageInterceptorContextImpl(context, hookPointsStatus);
                doAfter(context0, generalMessages);
            }

            @Override
            public void onFailure(Throwable t) {
                MessageInterceptorContextImpl context0 = new MessageInterceptorContextImpl(context,
                    MessageHookPointsStatus.ERROR);
                doAfter(context0, generalMessages);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> changeInvisibleDuration(
        MessageViewImpl messageView, Duration invisibleDuration) {
        final Endpoints endpoints = messageView.getEndpoints();
        RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> future;
        final List<GeneralMessage> generalMessages = Collections.singletonList(new GeneralMessageImpl(messageView));
        final MessageInterceptorContextImpl context =
            new MessageInterceptorContextImpl(MessageHookPoints.CHANGE_INVISIBLE_DURATION);
        doBefore(context, generalMessages);
        final ChangeInvisibleDurationRequest request = wrapChangeInvisibleDuration(messageView, invisibleDuration);
        final Duration requestTimeout = clientConfiguration.getRequestTimeout();
        future = this.getClientManager().changeInvisibleDuration(endpoints, request, requestTimeout);
        final MessageId messageId = messageView.getMessageId();
        Futures.addCallback(future, new FutureCallback<ChangeInvisibleDurationResponse>() {
            @Override
            public void onSuccess(ChangeInvisibleDurationResponse response) {
                final Status status = response.getStatus();
                final Code code = status.getCode();
                MessageHookPointsStatus hookPointsStatus = Code.OK.equals(code) ?
                    MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
                if (!Code.OK.equals(code)) {
                    log.error("Failed to change message invisible duration, messageId={}, endpoints={}, code={}, " +
                        "status message=[{}], clientId={}", messageId, endpoints, code, status.getMessage(), clientId);
                }
                MessageInterceptorContextImpl context0 = new MessageInterceptorContextImpl(context,
                    hookPointsStatus);
                doAfter(context0, generalMessages);
            }

            @Override
            public void onFailure(Throwable t) {
                MessageInterceptorContextImpl context0 = new MessageInterceptorContextImpl(context,
                    MessageHookPointsStatus.ERROR);
                doAfter(context0, generalMessages);
                log.error("Exception raised while changing message invisible duration, messageId={}, endpoints={}, "
                        + "clientId={}", messageId, endpoints, clientId, t);

            }
        }, MoreExecutors.directExecutor());
        return future;
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

    protected RpcFuture<QueryOffsetRequest, QueryOffsetResponse> queryOffset(MessageQueueImpl mq,
        OffsetPolicy offsetPolicy) {
        return queryOffset(mq, offsetPolicy, null);
    }

    protected RpcFuture<QueryOffsetRequest, QueryOffsetResponse> queryOffset(MessageQueueImpl mq,
        Long timestampMillis) {
        return queryOffset(mq, null, timestampMillis);
    }

    protected RpcFuture<GetOffsetRequest, GetOffsetResponse> getOffset(MessageQueueImpl mq) {
        final GetOffsetRequest request = wrapGetOffsetRequest(mq);
        final Duration requestTimeout = clientConfiguration.getRequestTimeout();
        return this.getClientManager().getOffset(mq.getBroker().getEndpoints(), request, requestTimeout);
    }

    protected RpcFuture<UpdateOffsetRequest, UpdateOffsetResponse> updateOffset(MessageQueueImpl mq, long offset) {
        final UpdateOffsetRequest request = wrapUpdateOffsetRequest(mq, offset);
        final Duration requestTimeout = clientConfiguration.getRequestTimeout();
        return this.getClientManager().updateOffset(mq.getBroker().getEndpoints(), request, requestTimeout);
    }

    private RpcFuture<QueryOffsetRequest, QueryOffsetResponse> queryOffset(MessageQueueImpl mq,
        OffsetPolicy offsetPolicy, Long timestampMillis) {
        QueryOffsetRequest request = null == offsetPolicy ? wrapQueryOffsetRequest(mq, timestampMillis) :
            wrapQueryOffsetRequest(mq, offsetPolicy);
        final Duration requestTimeout = clientConfiguration.getRequestTimeout();
        return this.getClientManager().queryOffset(mq.getBroker().getEndpoints(), request, requestTimeout);
    }

    QueryOffsetRequest wrapQueryOffsetRequest(MessageQueueImpl mq, Long timestampInMillis) {
        return QueryOffsetRequest.newBuilder().setMessageQueue(mq.toProtobuf())
            .setQueryOffsetPolicy(QueryOffsetPolicy.TIMESTAMP).setTimestamp(Timestamps.fromMillis(timestampInMillis))
            .build();
    }

    GetOffsetRequest wrapGetOffsetRequest(MessageQueueImpl mq) {
        return GetOffsetRequest.newBuilder().setMessageQueue(mq.toProtobuf()).setGroup(getProtobufGroup()).build();
    }

    UpdateOffsetRequest wrapUpdateOffsetRequest(MessageQueueImpl mq, long offset) {
        return UpdateOffsetRequest.newBuilder().setMessageQueue(mq.toProtobuf()).setGroup(getProtobufGroup())
            .setOffset(offset).build();
    }

    QueryOffsetRequest wrapQueryOffsetRequest(MessageQueueImpl mq, OffsetPolicy offsetPolicy) {
        final QueryOffsetRequest.Builder builder = QueryOffsetRequest.newBuilder().setMessageQueue(mq.toProtobuf());
        switch (offsetPolicy) {
            case BEGINNING:
                builder.setQueryOffsetPolicy(QueryOffsetPolicy.BEGINNING);
                break;
            case END:
                builder.setQueryOffsetPolicy(QueryOffsetPolicy.END);
                break;
            default:
                log.error("Unrecognized offset policy={}", offsetPolicy);
        }
        return builder.build();
    }

    PullMessageRequest wrapPullMessageRequest(long offset, int batchSize, MessageQueueImpl mq,
        FilterExpression filterExpression) {
        return PullMessageRequest.newBuilder().setGroup(getProtobufGroup()).setMessageQueue(mq.toProtobuf())
            .setLongPollingTimeout(Durations.fromNanos(PullProcessQueueImpl.LONG_POLLING_TIMEOUT.toNanos()))
            .setBatchSize(batchSize).setOffset(offset).setFilterExpression(wrapFilterExpression(filterExpression))
            .build();
    }

    ReceiveMessageRequest wrapReceiveMessageRequest(int batchSize, MessageQueueImpl mq,
        FilterExpression filterExpression, Duration longPollingTimeout, String attemptId) {
        attemptId = null == attemptId ? UUID.randomUUID().toString() : attemptId;
        return ReceiveMessageRequest.newBuilder().setGroup(getProtobufGroup())
            .setMessageQueue(mq.toProtobuf()).setFilterExpression(wrapFilterExpression(filterExpression))
            .setLongPollingTimeout(Durations.fromNanos(longPollingTimeout.toNanos()))
            .setBatchSize(batchSize).setAutoRenew(true).setAttemptId(attemptId).build();
    }

    ReceiveMessageRequest wrapReceiveMessageRequest(int batchSize, MessageQueueImpl mq,
        FilterExpression filterExpression, Duration invisibleDuration, Duration longPollingTimeout) {
        final com.google.protobuf.Duration duration = Durations.fromNanos(invisibleDuration.toNanos());
        return ReceiveMessageRequest.newBuilder().setGroup(getProtobufGroup())
            .setMessageQueue(mq.toProtobuf()).setFilterExpression(wrapFilterExpression(filterExpression))
            .setLongPollingTimeout(Durations.fromNanos(longPollingTimeout.toNanos()))
            .setBatchSize(batchSize).setAutoRenew(false).setInvisibleDuration(duration).build();
    }
}
