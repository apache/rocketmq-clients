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
import apache.rocketmq.v2.Status;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.rocketmq.client.java.impl.ClientType;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.GeneralMessageImpl;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"NullableProblems"})
public abstract class ConsumerImpl extends ClientImpl {
    static final Pattern CONSUMER_GROUP_PATTERN = Pattern.compile("^[%a-zA-Z0-9_-]+$");

    /**
     * Maximum number of ack entries packed into a single {@link AckMessageRequest}.
     * Prevents oversized gRPC payloads and reduces server-side per-request pressure.
     */
    static final int MAX_ACK_ENTRIES_PER_RPC = 512;

    private static final Logger log = LoggerFactory.getLogger(ConsumerImpl.class);

    protected final Resource groupResource;

    ConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup, Set<String> topics) {
        super(clientConfiguration, topics);
        this.groupResource = new Resource(clientConfiguration.getNamespace(), consumerGroup);
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

    private AckMessageRequest wrapAckMessageRequest(MessageViewImpl messageView) {
        final apache.rocketmq.v2.Resource topicResource = apache.rocketmq.v2.Resource.newBuilder()
            .setResourceNamespace(clientConfiguration.getNamespace())
            .setName(messageView.getTopic())
            .build();
        final AckMessageEntry.Builder builder = AckMessageEntry.newBuilder()
            .setMessageId(messageView.getMessageId().toString())
            .setReceiptHandle(messageView.getReceiptHandle());
        if (isLiteConsumer()) {
            messageView.getLiteTopic().ifPresent(builder::setLiteTopic);
        }
        final AckMessageEntry entry = builder.build();
        return AckMessageRequest.newBuilder().setGroup(getProtobufGroup()).setTopic(topicResource)
            .addEntries(entry).build();
    }

    protected ChangeInvisibleDurationRequest wrapChangeInvisibleDuration(MessageViewImpl messageView,
        Duration invisibleDuration) {
        return wrapChangeInvisibleDuration(messageView, invisibleDuration, false);
    }

    protected ChangeInvisibleDurationRequest wrapChangeInvisibleDuration(MessageViewImpl messageView,
        Duration invisibleDuration, boolean suspend) {
        final apache.rocketmq.v2.Resource topicResource = apache.rocketmq.v2.Resource.newBuilder()
            .setResourceNamespace(clientConfiguration.getNamespace())
            .setName(messageView.getTopic()).build();
        ChangeInvisibleDurationRequest.Builder builder = ChangeInvisibleDurationRequest.newBuilder()
            .setGroup(getProtobufGroup())
            .setTopic(topicResource)
            .setReceiptHandle(messageView.getReceiptHandle())
            .setInvisibleDuration(Durations.fromNanos(invisibleDuration.toNanos()))
            .setMessageId(messageView.getMessageId().toString());
        if (suspend) {
            builder.setSuspend(true);
        }
        if (isLiteConsumer()) {
            if (messageView.getLiteTopic().isPresent()) {
                builder.setLiteTopic(messageView.getLiteTopic().get());
                builder.setSuspend(true);
            }
        }
        return builder.build();
    }

    /**
     * Acknowledges a batch of messages grouped by (endpoints, topic).
     *
     * <p>Messages are first grouped by their target endpoints and topic.  Each group is then
     * further split into chunks of at most {@link #MAX_ACK_ENTRIES_PER_RPC} entries to keep
     * individual gRPC payloads small and to reduce server-side per-request pressure.
     *
     * @param messageViews the messages to acknowledge; all elements must be {@link MessageViewImpl}.
     * @return a future that completes when <em>all</em> underlying RPCs have completed.
     */
    protected ListenableFuture<Void> batchAckMessages(List<MessageViewImpl> messageViews) {
        // Group messages by (endpoints, topic) so that each RPC covers one broker + topic.
        final Map<Endpoints, Map<String /* topic */, List<MessageViewImpl>>> grouped = new HashMap<>();
        for (MessageViewImpl mv : messageViews) {
            grouped.computeIfAbsent(mv.getEndpoints(), k -> new HashMap<>())
                .computeIfAbsent(mv.getTopic(), k -> new ArrayList<>())
                .add(mv);
        }

        final List<ListenableFuture<Void>> futures = new ArrayList<>();
        for (Map.Entry<Endpoints, Map<String, List<MessageViewImpl>>> endpointsEntry : grouped.entrySet()) {
            final Endpoints endpoints = endpointsEntry.getKey();
            for (Map.Entry<String, List<MessageViewImpl>> topicEntry : endpointsEntry.getValue().entrySet()) {
                final List<MessageViewImpl> batch = topicEntry.getValue();
                // Split into chunks to avoid oversized RPC payloads.
                for (int i = 0; i < batch.size(); i += MAX_ACK_ENTRIES_PER_RPC) {
                    final int end = Math.min(i + MAX_ACK_ENTRIES_PER_RPC, batch.size());
                    futures.add(doBatchAck(endpoints, batch.subList(i, end)));
                }
            }
        }
        return Futures.whenAllComplete(futures).call(() -> {
            // Propagate the first failure if any.
            for (ListenableFuture<Void> f : futures) {
                f.get(); // throws ExecutionException on failure
            }
            return null;
        }, MoreExecutors.directExecutor());
    }

    private ListenableFuture<Void> doBatchAck(Endpoints endpoints, List<MessageViewImpl> batch) {
        final List<GeneralMessage> generalMessages = new ArrayList<>(batch.size());
        for (MessageViewImpl mv : batch) {
            generalMessages.add(new GeneralMessageImpl(mv));
        }
        final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.ACK);
        doBefore(context, generalMessages);

        final AckMessageRequest.Builder requestBuilder = AckMessageRequest.newBuilder()
            .setGroup(getProtobufGroup())
            .setTopic(apache.rocketmq.v2.Resource.newBuilder()
                .setResourceNamespace(clientConfiguration.getNamespace())
                .setName(batch.get(0).getTopic())
                .build());
        for (MessageViewImpl mv : batch) {
            final AckMessageEntry.Builder entryBuilder = AckMessageEntry.newBuilder()
                .setMessageId(mv.getMessageId().toString())
                .setReceiptHandle(mv.getReceiptHandle());
            if (isLiteConsumer()) {
                mv.getLiteTopic().ifPresent(entryBuilder::setLiteTopic);
            }
            requestBuilder.addEntries(entryBuilder.build());
        }

        final RpcFuture<AckMessageRequest, AckMessageResponse> future;
        try {
            final Duration requestTimeout = clientConfiguration.getRequestTimeout();
            future = this.getClientManager().ackMessage(endpoints, requestBuilder.build(), requestTimeout);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
        return Futures.transformAsync(future, response -> {
            final Status status = response.getStatus();
            final Code code = status.getCode();
            MessageHookPointsStatus hookPointsStatus = Code.OK.equals(code)
                ? MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
            MessageInterceptorContextImpl context0 =
                new MessageInterceptorContextImpl(context, hookPointsStatus);
            doAfter(context0, generalMessages);
            StatusChecker.check(status, future);
            return Futures.immediateVoidFuture();
        }, MoreExecutors.directExecutor());
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
        return changeInvisibleDuration(messageView, invisibleDuration, false);
    }

    RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> changeInvisibleDuration(
        MessageViewImpl messageView, Duration invisibleDuration, boolean suspend) {
        final Endpoints endpoints = messageView.getEndpoints();
        RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> future;
        final List<GeneralMessage> generalMessages = Collections.singletonList(new GeneralMessageImpl(messageView));
        final MessageInterceptorContextImpl context =
            new MessageInterceptorContextImpl(MessageHookPoints.CHANGE_INVISIBLE_DURATION);
        doBefore(context, generalMessages);
        final ChangeInvisibleDurationRequest request =
            wrapChangeInvisibleDuration(messageView, invisibleDuration, suspend);
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
                        + "clientId={}",
                    messageId, endpoints, clientId, t);

            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    protected apache.rocketmq.v2.Resource getProtobufGroup() {
        return groupResource.toProtobuf();
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

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        return HeartbeatRequest.newBuilder().setGroup(getProtobufGroup())
            .setClientType(clientType().toProtobuf()).build();
    }

    /**
     * Get client type for this client instance
     *
     * @return corresponding ClientType
     */
    protected abstract ClientType clientType();

    public boolean isLiteConsumer() {
        return ClientType.LITE_PUSH_CONSUMER == clientType() || ClientType.LITE_SIMPLE_CONSUMER == clientType();
    }

    public String getConsumerGroup() {
        return groupResource.getName();
    }
}
