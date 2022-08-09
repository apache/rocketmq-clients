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

package org.apache.rocketmq.client.java.impl.producer;

import static com.google.common.base.Preconditions.checkNotNull;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.Status;
import com.google.common.base.Stopwatch;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Metadata;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import net.javacrumbs.futureconverter.java8guava.FutureConverter;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;
import org.apache.rocketmq.client.java.exception.InternalErrorException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.impl.ClientImpl;
import org.apache.rocketmq.client.java.impl.ClientSettings;
import org.apache.rocketmq.client.java.message.MessageCommon;
import org.apache.rocketmq.client.java.message.MessageType;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.message.PublishingMessageImpl;
import org.apache.rocketmq.client.java.retry.ExponentialBackoffRetryPolicy;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.rpc.RpcInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link Producer}
 *
 * @see Producer
 */
@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
class ProducerImpl extends ClientImpl implements Producer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerImpl.class);

    protected final ProducerSettings producerSettings;
    final ConcurrentMap<String/* topic */, PublishingLoadBalancer> publishingRouteDataCache;
    private final TransactionChecker checker;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     */
    ProducerImpl(ClientConfiguration clientConfiguration, Set<String> topics, int maxAttempts,
        TransactionChecker checker) {
        super(clientConfiguration, topics);
        ExponentialBackoffRetryPolicy retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(maxAttempts);
        this.producerSettings = new ProducerSettings(clientId, endpoints, retryPolicy,
            clientConfiguration.getRequestTimeout(), topics);
        this.checker = checker;
        this.publishingRouteDataCache = new ConcurrentHashMap<>();
    }

    @Override
    protected void startUp() throws Exception {
        try {
            LOGGER.info("Begin to start the rocketmq producer, clientId={}", clientId);
            super.startUp();
            LOGGER.info("The rocketmq producer starts successfully, clientId={}", clientId);
        } catch (Throwable t) {
            LOGGER.error("Failed to start the rocketmq producer, try to shutdown it, clientId={}", clientId, t);
            shutDown();
            throw t;
        }
    }

    @Override
    protected void shutDown() throws InterruptedException {
        LOGGER.info("Begin to shutdown the rocketmq producer, clientId={}", clientId);
        super.shutDown();
        LOGGER.info("Shutdown the rocketmq producer successfully, clientId={}", clientId);
    }

    @Override
    public void onRecoverOrphanedTransactionCommand(Endpoints endpoints, RecoverOrphanedTransactionCommand command) {
        final String transactionId = command.getTransactionId();
        final String messageId = command.getMessage().getSystemProperties().getMessageId();
        if (null == checker) {
            LOGGER.error("No transaction checker registered, ignore it, messageId={}, transactionId={}, endpoints={},"
                + " clientId={}", messageId, transactionId, endpoints, clientId);
            return;
        }
        MessageViewImpl messageView;
        try {
            messageView = MessageViewImpl.fromProtobuf(command.getMessage());
        } catch (Throwable t) {
            LOGGER.error("[Bug] Failed to decode message during orphaned transaction message recovery, messageId={}, "
                + "transactionId={}, endpoints={}, clientId={}", messageId, transactionId, endpoints, clientId, t);
            return;
        }
        ListenableFuture<TransactionResolution> future;
        try {
            final ListeningExecutorService service = MoreExecutors.listeningDecorator(telemetryCommandExecutor);
            final Callable<TransactionResolution> task = () -> checker.check(messageView);
            future = service.submit(task);
        } catch (Throwable t) {
            final SettableFuture<TransactionResolution> future0 = SettableFuture.create();
            future0.setException(t);
            future = future0;
        }
        Futures.addCallback(future, new FutureCallback<TransactionResolution>() {
            @Override
            public void onSuccess(TransactionResolution resolution) {
                try {
                    if (null == resolution || TransactionResolution.UNKNOWN.equals(resolution)) {
                        return;
                    }
                    endTransaction(endpoints, messageView.getMessageCommon(), messageView.getMessageId(),
                        transactionId, resolution);
                } catch (Throwable t) {
                    LOGGER.error("Exception raised while ending the transaction, messageId={}, transactionId={}, "
                        + "endpoints={}, clientId={}", messageId, transactionId, endpoints, clientId, t);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.error("Exception raised while checking the transaction, messageId={}, transactionId={}, "
                    + "endpoints={}, clientId={}", messageId, transactionId, endpoints, clientId, t);

            }
        }, MoreExecutors.directExecutor());
    }

    @Override
    public ClientSettings getClientSettings() {
        return producerSettings;
    }

    @Override
    public NotifyClientTerminationRequest wrapNotifyClientTerminationRequest() {
        return NotifyClientTerminationRequest.newBuilder().build();
    }

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        return HeartbeatRequest.newBuilder().build();
    }

    /**
     * @see Producer#send(Message)
     */
    @Override
    public SendReceipt send(Message message) throws ClientException {
        final ListenableFuture<SendReceipt> future = Futures.transform(send(Collections.singletonList(message), false),
            sendReceipts -> sendReceipts.iterator().next(), MoreExecutors.directExecutor());
        return handleClientFuture(future);
    }

    /**
     * @see Producer#send(Message, Transaction)
     */
    @Override
    public SendReceipt send(Message message, Transaction transaction) throws ClientException {
        if (!(transaction instanceof TransactionImpl)) {
            throw new IllegalArgumentException("Failed downcasting for transaction");
        }
        TransactionImpl transactionImpl = (TransactionImpl) transaction;
        final PublishingMessageImpl publishingMessage;
        try {
            publishingMessage = transactionImpl.tryAddMessage(message);
        } catch (Throwable t) {
            throw new ClientException(t);
        }
        final ListenableFuture<List<SendReceiptImpl>> future = send(Collections.singletonList(publishingMessage),
            true);
        final List<SendReceiptImpl> receipts = handleClientFuture(future);
        final SendReceiptImpl sendReceipt = receipts.iterator().next();
        ((TransactionImpl) transaction).tryAddReceipt(publishingMessage, sendReceipt);
        return sendReceipt;
    }

    /**
     * @see Producer#sendAsync(Message)
     */
    @Override
    public CompletableFuture<SendReceipt> sendAsync(Message message) {
        final ListenableFuture<SendReceipt> future = Futures.transform(send(Collections.singletonList(message), false),
            sendReceipts -> sendReceipts.iterator().next(), MoreExecutors.directExecutor());
        return FutureConverter.toCompletableFuture(future);
    }

    /**
     * @see Producer#beginTransaction()
     */
    @Override
    public Transaction beginTransaction() {
        checkNotNull(checker, "Transaction checker should not be null");
        if (!this.isRunning()) {
            LOGGER.error("Unable to begin a transaction because producer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Producer is not running now");
        }
        return new TransactionImpl(this);
    }

    @Override
    public void close() {
        this.stopAsync().awaitTerminated();
    }

    public void endTransaction(Endpoints endpoints, MessageCommon messageCommon, MessageId messageId,
        String transactionId, final TransactionResolution resolution) throws ClientException {
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            throw new ClientException(t);
        }
        final EndTransactionRequest.Builder builder = EndTransactionRequest.newBuilder()
            .setMessageId(messageId.toString()).setTransactionId(transactionId)
            .setTopic(apache.rocketmq.v2.Resource.newBuilder().setName(messageCommon.getTopic()).build());
        switch (resolution) {
            case COMMIT:
                builder.setResolution(apache.rocketmq.v2.TransactionResolution.COMMIT);
                break;
            case ROLLBACK:
            default:
                builder.setResolution(apache.rocketmq.v2.TransactionResolution.ROLLBACK);
        }
        final Duration requestTimeout = clientConfiguration.getRequestTimeout();
        final EndTransactionRequest request = builder.build();

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final List<MessageCommon> messageCommons = Collections.singletonList(messageCommon);
        MessageHookPoints messageHookPoints = TransactionResolution.COMMIT.equals(resolution) ?
            MessageHookPoints.COMMIT_TRANSACTION : MessageHookPoints.ROLLBACK_TRANSACTION;
        doBefore(messageHookPoints, messageCommons);

        final ListenableFuture<RpcInvocation<EndTransactionResponse>> future =
            clientManager.endTransaction(endpoints, metadata, request, requestTimeout);
        Futures.addCallback(future, new FutureCallback<RpcInvocation<EndTransactionResponse>>() {
            @Override
            public void onSuccess(RpcInvocation<EndTransactionResponse> invocation) {
                final Duration duration = stopwatch.elapsed();
                final EndTransactionResponse response = invocation.getResponse();
                final Status status = response.getStatus();
                final Code code = status.getCode();
                MessageHookPointsStatus messageHookPointsStatus = Code.OK.equals(code) ? MessageHookPointsStatus.OK :
                    MessageHookPointsStatus.ERROR;
                doAfter(messageHookPoints, messageCommons, duration, messageHookPointsStatus);
            }

            @Override
            public void onFailure(Throwable t) {
                final Duration duration = stopwatch.elapsed();
                doAfter(messageHookPoints, messageCommons, duration, MessageHookPointsStatus.ERROR);
            }
        }, MoreExecutors.directExecutor());
        final RpcInvocation<EndTransactionResponse> invocation = handleClientFuture(future);
        final EndTransactionResponse response = invocation.getResponse();
        final Status status = response.getStatus();
        final Code code = status.getCode();
        if (!Code.OK.equals(code)) {
            throw new ClientException(code.getNumber(), invocation.getContext().getRequestId(), status.getMessage());
        }
    }

    /**
     * Isolate specified {@link Endpoints}.
     */
    private void isolate(Endpoints endpoints) {
        isolated.add(endpoints);
    }

    private RetryPolicy getRetryPolicy() {
        return producerSettings.getRetryPolicy();
    }

    /**
     * Take message queue(s) from route for message publishing.
     */
    private List<MessageQueueImpl> takeMessageQueues(PublishingLoadBalancer result) {
        return result.takeMessageQueues(isolated, this.getRetryPolicy().getMaxAttempts());
    }

    private ListenableFuture<List<SendReceiptImpl>> send(List<Message> messages, boolean txEnabled) {
        SettableFuture<List<SendReceiptImpl>> future = SettableFuture.create();

        // Check producer state before message publishing.
        if (!this.isRunning()) {
            final IllegalStateException e = new IllegalStateException("Producer is not running now");
            future.setException(e);
            LOGGER.error("Unable to send message because producer is not running, state={}, clientId={}",
                this.state(), clientId);
            return future;
        }

        List<PublishingMessageImpl> pubMessages = new ArrayList<>();
        for (Message message : messages) {
            try {
                final PublishingMessageImpl pubMessage = new PublishingMessageImpl(message, producerSettings,
                    txEnabled);
                pubMessages.add(pubMessage);
            } catch (Throwable t) {
                // Failed to refine message, no need to proceed.
                LOGGER.error("Failed to refine message to send, clientId={}, message={}", clientId, message, t);
                future.setException(t);
                return future;
            }
        }

        // Collect topics to send message.
        final Set<String> topics = pubMessages.stream().map(Message::getTopic).collect(Collectors.toSet());
        if (1 < topics.size()) {
            // Messages have different topics, no need to proceed.
            final IllegalArgumentException e = new IllegalArgumentException("Messages to send have different topics");
            future.setException(e);
            LOGGER.error("Messages to be sent have different topics, no need to proceed, topic(s)={}, clientId={}",
                topics, clientId);
            return future;
        }

        final String topic = topics.iterator().next();
        // Collect message types.
        final Set<MessageType> messageTypes = pubMessages.stream()
            .map(PublishingMessageImpl::getMessageType)
            .collect(Collectors.toSet());
        if (1 < messageTypes.size()) {
            // Messages have different message type, no need to proceed.
            final IllegalArgumentException e = new IllegalArgumentException("Messages to send have different types, "
                + "please check");
            future.setException(e);
            LOGGER.error("Messages to be sent have different message types, no need to proceed, topic={}, messageType"
                + "(s)={}, clientId={}", topic, messageTypes, clientId, e);
            return future;
        }

        final MessageType messageType = messageTypes.iterator().next();
        final String messageGroup;

        // Message group must be same if message type is FIFO, or no need to proceed.
        if (MessageType.FIFO.equals(messageType)) {
            final Set<String> messageGroups = pubMessages.stream()
                .map(PublishingMessageImpl::getMessageGroup).filter(Optional::isPresent)
                .map(Optional::get).collect(Collectors.toSet());

            if (1 < messageGroups.size()) {
                final IllegalArgumentException e = new IllegalArgumentException("FIFO messages to send have different "
                    + "message groups, messageGroups=" + messageGroups);
                future.setException(e);
                LOGGER.error("FIFO messages to be sent have different message groups, no need to proceed, topic={}, "
                    + "messageGroups={}, clientId={}", topic, messageGroups, clientId, e);
                return future;
            }
            messageGroup = messageGroups.iterator().next();
        } else {
            messageGroup = null;
        }

        this.topics.add(topic);
        // Get publishing topic route.
        final ListenableFuture<PublishingLoadBalancer> routeFuture = getPublishingTopicRouteResult(topic);
        return Futures.transformAsync(routeFuture, result -> {
            // Prepare the candidate message queue(s) for retry-sending in advance.
            final List<MessageQueueImpl> candidates = null == messageGroup ? takeMessageQueues(result) :
                Collections.singletonList(result.takeMessageQueueByMessageGroup(messageGroup));
            final SettableFuture<List<SendReceiptImpl>> future0 = SettableFuture.create();
            send0(future0, topic, messageType, candidates, pubMessages, 1);
            return future0;
        }, MoreExecutors.directExecutor());
    }

    /**
     * The caller is supposed to make sure different messages have the same message type and same topic.
     */
    private SendMessageRequest wrapSendMessageRequest(List<PublishingMessageImpl> pubMessages, MessageQueueImpl mq) {
        final List<apache.rocketmq.v2.Message> messages = pubMessages.stream()
            .map(publishingMessage -> publishingMessage.toProtobuf(mq)).collect(Collectors.toList());
        return SendMessageRequest.newBuilder().addAllMessages(messages).build();
    }

    ListenableFuture<List<SendReceiptImpl>> send0(Metadata metadata, Endpoints endpoints,
        List<PublishingMessageImpl> pubMessages, MessageQueueImpl mq) {
        final SendMessageRequest request = wrapSendMessageRequest(pubMessages, mq);
        final ListenableFuture<RpcInvocation<SendMessageResponse>> future0 =
            clientManager.sendMessage(endpoints, metadata, request, clientConfiguration.getRequestTimeout());
        return Futures.transformAsync(future0,
            invocation -> Futures.immediateFuture(SendReceiptImpl.processResponseInvocation(mq, invocation)),
            MoreExecutors.directExecutor());
    }

    private void send0(SettableFuture<List<SendReceiptImpl>> future0, String topic, MessageType messageType,
        final List<MessageQueueImpl> candidates, final List<PublishingMessageImpl> messages, final int attempt) {
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            // Failed to sign, no need to proceed.
            future0.setException(t);
            return;
        }
        // Calculate the current message queue.
        final MessageQueueImpl mq = candidates.get(IntMath.mod(attempt - 1, candidates.size()));
        final List<MessageType> acceptMessageTypes = mq.getAcceptMessageTypes();
        if (producerSettings.isValidateMessageType() && !acceptMessageTypes.contains(messageType)) {
            final IllegalArgumentException e = new IllegalArgumentException("Current message type not match with "
                + "topic accept message types, topic=" + topic + ", actualMessageType=" + messageType + ", "
                + "acceptMessageTypes=" + acceptMessageTypes);
            future0.setException(e);
            return;
        }
        final Endpoints endpoints = mq.getBroker().getEndpoints();
        final ListenableFuture<List<SendReceiptImpl>> future = send0(metadata, endpoints, messages, mq);
        final int maxAttempts = this.getRetryPolicy().getMaxAttempts();
        // Intercept before message publishing.
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final List<MessageCommon> messageCommons =
            messages.stream().map(PublishingMessageImpl::getMessageCommon).collect(Collectors.toList());
        doBefore(MessageHookPoints.SEND, messageCommons);

        Futures.addCallback(future, new FutureCallback<List<SendReceiptImpl>>() {
            @Override
            public void onSuccess(List<SendReceiptImpl> sendReceipts) {
                // Intercept after message publishing.
                final Duration duration = stopwatch.elapsed();
                doAfter(MessageHookPoints.SEND, messageCommons, duration, MessageHookPointsStatus.OK);
                // Should never reach here.
                if (sendReceipts.size() != messages.size()) {
                    final InternalErrorException e = new InternalErrorException("[Bug] due to an"
                        + " unknown reason from remote, received send receipt's quantity " + sendReceipts.size()
                        + " is not equal to sent message's quantity " + messages.size());
                    future0.setException(e);
                    return;
                }
                // No need more attempts.
                future0.set(sendReceipts);
                // Resend message(s) successfully.
                if (1 < attempt) {
                    // Collect messageId(s) for logging.
                    List<MessageId> messageIds = new ArrayList<>();
                    for (SendReceipt receipt : sendReceipts) {
                        messageIds.add(receipt.getMessageId());
                    }
                    LOGGER.info("Resend message successfully, topic={}, messageId(s)={}, maxAttempts={}, "
                            + "attempt={}, endpoints={}, clientId={}", topic, messageIds, maxAttempts, attempt,
                        endpoints, clientId);
                }
                // Send message(s) successfully on first attempt, return directly.
            }

            @Override
            public void onFailure(Throwable t) {
                // Intercept after message publishing.
                final Duration duration = stopwatch.elapsed();
                doAfter(MessageHookPoints.SEND, messageCommons, duration, MessageHookPointsStatus.ERROR);

                // Collect messageId(s) for logging.
                List<MessageId> messageIds = new ArrayList<>();
                for (PublishingMessageImpl message : messages) {
                    messageIds.add(message.getMessageId());
                }
                // Isolate endpoints because of sending failure.
                isolate(endpoints);
                if (attempt >= maxAttempts) {
                    // No need more attempts.
                    future0.setException(t);
                    LOGGER.error("Failed to send message(s) finally, run out of attempt times, maxAttempts={}, " +
                            "attempt={}, topic={}, messageId(s)={}, endpoints={}, clientId={}",
                        maxAttempts, attempt, topic, messageIds, endpoints, clientId, t);
                    return;
                }
                // No need more attempts for transactional message.
                if (MessageType.TRANSACTION.equals(messageType)) {
                    future0.setException(t);
                    LOGGER.error("Failed to send transactional message finally, maxAttempts=1, attempt={}, " +
                            "topic={}, messageId(s)={}, endpoints={}, clientId={}", attempt, topic, messageIds,
                        endpoints, clientId, t);
                    return;
                }
                // Try to do more attempts.
                int nextAttempt = 1 + attempt;
                // Retry immediately if the request is not throttled.
                if (!(t instanceof TooManyRequestsException)) {
                    LOGGER.warn("Failed to send message, would attempt to resend right now, maxAttempts={}, "
                            + "attempt={}, topic={}, messageId(s)={}, endpoints={}, clientId={}", maxAttempts, attempt,
                        topic, messageIds, endpoints, clientId, t);
                    send0(future0, topic, messageType, candidates, messages, nextAttempt);
                    return;
                }
                final Duration delay = ProducerImpl.this.getRetryPolicy().getNextAttemptDelay(nextAttempt);
                LOGGER.warn("Failed to send message due to too many requests, would attempt to resend after {}, "
                        + "maxAttempts={}, attempt={}, topic={}, messageId(s)={}, endpoints={}, clientId={}", delay,
                    maxAttempts, attempt, topic, messageIds, endpoints, clientId, t);
                clientManager.getScheduler().schedule(() -> send0(future0, topic, messageType, candidates, messages,
                    nextAttempt), delay.toNanos(), TimeUnit.NANOSECONDS);
            }
        }, clientCallbackExecutor);
    }

    @Override
    public void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData) {
        final PublishingLoadBalancer publishingLoadBalancer = new PublishingLoadBalancer(topicRouteData);
        publishingRouteDataCache.put(topic, publishingLoadBalancer);
    }

    private ListenableFuture<PublishingLoadBalancer> getPublishingTopicRouteResult(final String topic) {
        final PublishingLoadBalancer result = publishingRouteDataCache.get(topic);
        if (null != result) {
            return Futures.immediateFuture(result);
        }
        return Futures.transformAsync(getRouteData(topic), topicRouteDataResult -> {
            final PublishingLoadBalancer loadBalancer = new PublishingLoadBalancer(topicRouteDataResult);
            publishingRouteDataCache.put(topic, loadBalancer);
            return Futures.immediateFuture(loadBalancer);
        }, MoreExecutors.directExecutor());
    }
}
