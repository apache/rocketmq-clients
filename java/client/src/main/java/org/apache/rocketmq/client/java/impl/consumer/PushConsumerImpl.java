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

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import apache.rocketmq.v2.VerifyMessageResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContext;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContextImpl;
import org.apache.rocketmq.client.java.impl.Settings;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.GeneralMessageImpl;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.metrics.GaugeObserver;
import org.apache.rocketmq.client.java.misc.ExecutorServices;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link PushConsumer}
 *
 * <p>It is worth noting that in the implementation of push consumer, the message is not actively pushed by the server
 * to the client, but is obtained by the client actively going to the server.
 *
 * @see PushConsumer
 */
@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
class PushConsumerImpl extends ConsumerImpl implements PushConsumer {
    private static final Logger log = LoggerFactory.getLogger(PushConsumerImpl.class);

    final AtomicLong consumptionOkQuantity;
    final AtomicLong consumptionErrorQuantity;

    private final ClientConfiguration clientConfiguration;
    private final PushSubscriptionSettings pushSubscriptionSettings;
    private final String consumerGroup;
    private final Map<String /* topic */, FilterExpression> subscriptionExpressions;
    private final ConcurrentMap<String /* topic */, Assignments> cacheAssignments;
    private final MessageListener messageListener;
    private final int maxCacheMessageCount;
    private final int maxCacheMessageSizeInBytes;

    /**
     * Indicates the times of message reception.
     */
    private final AtomicLong receptionTimes;
    /**
     * Indicates the quantity of received messages.
     */
    private final AtomicLong receivedMessagesQuantity;

    private final ThreadPoolExecutor consumptionExecutor;
    private final ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable;
    private ConsumeService consumeService;

    private volatile ScheduledFuture<?> scanAssignmentsFuture;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     */
    public PushConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup,
        Map<String, FilterExpression> subscriptionExpressions, MessageListener messageListener,
        int maxCacheMessageCount, int maxCacheMessageSizeInBytes, int consumptionThreadCount) {
        super(clientConfiguration, consumerGroup, subscriptionExpressions.keySet());
        this.clientConfiguration = clientConfiguration;
        Resource groupResource = new Resource(consumerGroup);
        this.pushSubscriptionSettings = new PushSubscriptionSettings(clientId, endpoints, groupResource,
            clientConfiguration.getRequestTimeout(), subscriptionExpressions);
        this.consumerGroup = consumerGroup;
        this.subscriptionExpressions = subscriptionExpressions;
        this.cacheAssignments = new ConcurrentHashMap<>();
        this.messageListener = messageListener;
        this.maxCacheMessageCount = maxCacheMessageCount;
        this.maxCacheMessageSizeInBytes = maxCacheMessageSizeInBytes;

        this.receptionTimes = new AtomicLong(0);
        this.receivedMessagesQuantity = new AtomicLong(0);
        this.consumptionOkQuantity = new AtomicLong(0);
        this.consumptionErrorQuantity = new AtomicLong(0);

        this.processQueueTable = new ConcurrentHashMap<>();

        this.consumptionExecutor = new ThreadPoolExecutor(
            consumptionThreadCount,
            consumptionThreadCount,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("MessageConsumption", this.getClientId().getIndex()));
    }

    @Override
    protected void startUp() throws Exception {
        try {
            log.info("Begin to start the rocketmq push consumer, clientId={}", clientId);
            GaugeObserver gaugeObserver = new ProcessQueueGaugeObserver(processQueueTable, clientId, consumerGroup);
            this.clientMeterManager.setGaugeObserver(gaugeObserver);
            super.startUp();
            final ScheduledExecutorService scheduler = this.getClientManager().getScheduler();
            this.consumeService = createConsumeService();
            // Scan assignments periodically.
            scanAssignmentsFuture = scheduler.scheduleWithFixedDelay(() -> {
                try {
                    scanAssignments();
                } catch (Throwable t) {
                    log.error("Exception raised while scanning the load assignments, clientId={}", clientId, t);
                }
            }, 1, 5, TimeUnit.SECONDS);
            log.info("The rocketmq push consumer starts successfully, clientId={}", clientId);
        } catch (Throwable t) {
            log.error("Exception raised while starting the rocketmq push consumer, clientId={}", clientId, t);
            shutDown();
            throw t;
        }
    }

    @Override
    protected void shutDown() throws InterruptedException {
        log.info("Begin to shutdown the rocketmq push consumer, clientId={}", clientId);
        if (null != scanAssignmentsFuture) {
            scanAssignmentsFuture.cancel(false);
        }
        super.shutDown();
        this.consumptionExecutor.shutdown();
        ExecutorServices.awaitTerminated(consumptionExecutor);
        log.info("Shutdown the rocketmq push consumer successfully, clientId={}", clientId);
    }

    private ConsumeService createConsumeService() {
        final ScheduledExecutorService scheduler = this.getClientManager().getScheduler();
        if (pushSubscriptionSettings.isFifo()) {
            log.info("Create FIFO consume service, consumerGroup={}, clientId={}", consumerGroup, clientId);
            return new FifoConsumeService(clientId, messageListener, consumptionExecutor, this, scheduler);
        }
        log.info("Create standard consume service, consumerGroup={}, clientId={}", consumerGroup, clientId);
        return new StandardConsumeService(clientId, messageListener, consumptionExecutor, this, scheduler);
    }

    /**
     * @see PushConsumer#getConsumerGroup()
     */
    @Override
    public String getConsumerGroup() {
        return consumerGroup;
    }

    public PushSubscriptionSettings getPushConsumerSettings() {
        return pushSubscriptionSettings;
    }

    /**
     * @see PushConsumer#getSubscriptionExpressions()
     */
    @Override
    public Map<String, FilterExpression> getSubscriptionExpressions() {
        return new HashMap<>(subscriptionExpressions);
    }

    /**
     * @see PushConsumer#subscribe(String, FilterExpression)
     */
    @Override
    public PushConsumer subscribe(String topic, FilterExpression filterExpression) throws ClientException {
        // Check consumer status.
        if (!this.isRunning()) {
            log.error("Unable to add subscription because push consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Push consumer is not running now");
        }
        final ListenableFuture<TopicRouteData> future = getRouteData(topic);
        handleClientFuture(future);
        subscriptionExpressions.put(topic, filterExpression);
        return this;
    }

    /**
     * @see PushConsumer#unsubscribe(String)
     */
    @Override
    public PushConsumer unsubscribe(String topic) {
        // Check consumer status.
        if (!this.isRunning()) {
            log.error("Unable to remove subscription because push consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Push consumer is not running now");
        }
        subscriptionExpressions.remove(topic);
        return this;
    }

    private ListenableFuture<Endpoints> pickEndpointsToQueryAssignments(String topic) {
        final ListenableFuture<TopicRouteData> future = getRouteData(topic);
        return Futures.transformAsync(future, topicRouteData -> {
            Endpoints endpoints = topicRouteData.pickEndpointsToQueryAssignments();
            return Futures.immediateFuture(endpoints);
        }, MoreExecutors.directExecutor());
    }

    private QueryAssignmentRequest wrapQueryAssignmentRequest(String topic) {
        apache.rocketmq.v2.Resource topicResource = apache.rocketmq.v2.Resource.newBuilder().setName(topic).build();
        return QueryAssignmentRequest.newBuilder().setTopic(topicResource)
            .setEndpoints(endpoints.toProtobuf()).setGroup(getProtobufGroup()).build();
    }

    ListenableFuture<Assignments> queryAssignment(final String topic) {
        final ListenableFuture<Endpoints> future0 = pickEndpointsToQueryAssignments(topic);
        return Futures.transformAsync(future0, endpoints -> {
            final QueryAssignmentRequest request = wrapQueryAssignmentRequest(topic);
            final Duration requestTimeout = clientConfiguration.getRequestTimeout();
            final RpcFuture<QueryAssignmentRequest, QueryAssignmentResponse> future1 =
                this.getClientManager().queryAssignment(endpoints, request, requestTimeout);
            return Futures.transformAsync(future1, response -> {
                final Status status = response.getStatus();
                StatusChecker.check(status, future1);
                final List<Assignment> assignmentList = response.getAssignmentsList().stream().map(assignment ->
                    new Assignment(new MessageQueueImpl(assignment.getMessageQueue()))).collect(Collectors.toList());
                final Assignments assignments = new Assignments(assignmentList);
                return Futures.immediateFuture(assignments);
            }, MoreExecutors.directExecutor());
        }, MoreExecutors.directExecutor());
    }

    /**
     * Drop {@link ProcessQueue} by {@link MessageQueueImpl}, {@link ProcessQueue} must be removed before it is dropped.
     *
     * @param mq message queue.
     */
    void dropProcessQueue(MessageQueueImpl mq) {
        final ProcessQueue pq = processQueueTable.remove(mq);
        if (null != pq) {
            pq.drop();
        }
    }

    /**
     * Create process queue and add it into {@link #processQueueTable}, return {@link Optional#empty()} if mapped
     * process queue already exists.
     * <p>
     * This function and {@link #dropProcessQueue(MessageQueueImpl)} make sures that process queue is not dropped if
     * it is contained in {@link #processQueueTable}, once process queue is dropped, it must have been removed
     * from {@link #processQueueTable}.
     *
     * @param mq               message queue.
     * @param filterExpression filter expression of topic.
     * @return optional process queue.
     */
    protected Optional<ProcessQueue> createProcessQueue(MessageQueueImpl mq, final FilterExpression filterExpression) {
        final ProcessQueueImpl processQueue = new ProcessQueueImpl(this, mq, filterExpression);
        final ProcessQueue previous = processQueueTable.putIfAbsent(mq, processQueue);
        if (null != previous) {
            return Optional.empty();
        }
        return Optional.of(processQueue);
    }

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        return HeartbeatRequest.newBuilder().setGroup(getProtobufGroup())
            .setClientType(ClientType.PUSH_CONSUMER).build();
    }


    @VisibleForTesting
    void syncProcessQueue(String topic, Assignments assignments, FilterExpression filterExpression) {
        Set<MessageQueueImpl> latest = new HashSet<>();

        final List<Assignment> assignmentList = assignments.getAssignmentList();
        for (Assignment assignment : assignmentList) {
            latest.add(assignment.getMessageQueue());
        }

        Set<MessageQueueImpl> activeMqs = new HashSet<>();

        for (Map.Entry<MessageQueueImpl, ProcessQueue> entry : processQueueTable.entrySet()) {
            final MessageQueueImpl mq = entry.getKey();
            final ProcessQueue pq = entry.getValue();
            if (!topic.equals(mq.getTopic())) {
                continue;
            }

            if (!latest.contains(mq)) {
                log.info("Drop message queue according to the latest assignmentList, mq={}, clientId={}", mq,
                    clientId);
                dropProcessQueue(mq);
                continue;
            }

            if (pq.expired()) {
                log.warn("Drop message queue because it is expired, mq={}, clientId={}", mq, clientId);
                dropProcessQueue(mq);
                continue;
            }
            activeMqs.add(mq);
        }

        for (MessageQueueImpl mq : latest) {
            if (activeMqs.contains(mq)) {
                continue;
            }
            final Optional<ProcessQueue> optionalProcessQueue = createProcessQueue(mq, filterExpression);
            if (optionalProcessQueue.isPresent()) {
                log.info("Start to fetch message from remote, mq={}, clientId={}", mq, clientId);
                optionalProcessQueue.get().fetchMessageImmediately();
            }
        }
    }

    @VisibleForTesting
    void scanAssignments() {
        try {
            log.debug("Start to scan assignments periodically, clientId={}", clientId);
            for (Map.Entry<String, FilterExpression> entry : subscriptionExpressions.entrySet()) {
                final String topic = entry.getKey();
                final FilterExpression filterExpression = entry.getValue();
                final Assignments existed = cacheAssignments.get(topic);
                final ListenableFuture<Assignments> future = queryAssignment(topic);
                Futures.addCallback(future, new FutureCallback<Assignments>() {
                    @Override
                    public void onSuccess(Assignments latest) {
                        if (latest.getAssignmentList().isEmpty()) {
                            if (null == existed || existed.getAssignmentList().isEmpty()) {
                                log.info("Acquired empty assignments from remote, would scan later, topic={}, "
                                    + "clientId={}", topic, clientId);
                                return;
                            }
                            log.info("Attention!!! acquired empty assignments from remote, but existed assignments"
                                + " is not empty, topic={}, clientId={}", topic, clientId);
                        }

                        if (!latest.equals(existed)) {
                            log.info("Assignments of topic={} has changed, {} => {}, clientId={}", topic, existed,
                                latest, clientId);
                            syncProcessQueue(topic, latest, filterExpression);
                            cacheAssignments.put(topic, latest);
                            return;
                        }
                        log.debug("Assignments of topic={} remains the same, assignments={}, clientId={}", topic,
                            existed, clientId);
                        // Process queue may be dropped, need to be synchronized anyway.
                        syncProcessQueue(topic, latest, filterExpression);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.error("Exception raised while scanning the assignments, topic={}, clientId={}", topic,
                            clientId, t);
                    }
                }, MoreExecutors.directExecutor());
            }
        } catch (Throwable t) {
            log.error("Exception raised while scanning the assignments for all topics, clientId={}", clientId, t);
        }
    }

    @Override
    public Settings getSettings() {
        return pushSubscriptionSettings;
    }

    /**
     * @see PushConsumer#close()
     */
    @Override
    public void close() {
        this.stopAsync().awaitTerminated();
    }

    int getQueueSize() {
        return processQueueTable.size();
    }

    int cacheMessageBytesThresholdPerQueue() {
        final int size = this.getQueueSize();
        // ALl process queues are removed, no need to cache messages.
        if (size <= 0) {
            return 0;
        }
        return Math.max(1, maxCacheMessageSizeInBytes / size);
    }

    int cacheMessageCountThresholdPerQueue() {
        final int size = this.getQueueSize();
        // All process queues are removed, no need to cache messages.
        if (size <= 0) {
            return 0;
        }
        return Math.max(1, maxCacheMessageCount / size);
    }

    public AtomicLong getReceptionTimes() {
        return receptionTimes;
    }

    public AtomicLong getReceivedMessagesQuantity() {
        return receivedMessagesQuantity;
    }

    public ConsumeService getConsumeService() {
        return consumeService;
    }

    @Override
    public void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand verifyMessageCommand) {
        final String nonce = verifyMessageCommand.getNonce();
        final MessageViewImpl messageView = MessageViewImpl.fromProtobuf(verifyMessageCommand.getMessage());
        final MessageId messageId = messageView.getMessageId();
        final ListenableFuture<ConsumeResult> future = consumeService.consume(messageView);
        Futures.addCallback(future, new FutureCallback<ConsumeResult>() {
            @Override
            public void onSuccess(ConsumeResult consumeResult) {
                Code code = ConsumeResult.SUCCESS.equals(consumeResult) ? Code.OK : Code.FAILED_TO_CONSUME_MESSAGE;
                Status status = Status.newBuilder().setCode(code).build();
                final VerifyMessageResult verifyMessageResult =
                    VerifyMessageResult.newBuilder().setNonce(nonce).build();
                TelemetryCommand command = TelemetryCommand.newBuilder()
                    .setVerifyMessageResult(verifyMessageResult)
                    .setStatus(status)
                    .build();
                try {
                    telemetry(endpoints, command);
                } catch (Throwable t) {
                    log.error("Failed to send message verification result command, endpoints={}, command={}, "
                        + "messageId={}, clientId={}", endpoints, command, messageId, clientId, t);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                // Should never reach here.
                log.error("[Bug] Failed to get message verification result, endpoints={}, messageId={}, "
                    + "clientId={}", endpoints, messageId, clientId, t);
            }
        }, MoreExecutors.directExecutor());
    }

    private ForwardMessageToDeadLetterQueueRequest wrapForwardMessageToDeadLetterQueueRequest(
        MessageViewImpl messageView) {
        final apache.rocketmq.v2.Resource topicResource =
            apache.rocketmq.v2.Resource.newBuilder().setName(messageView.getTopic()).build();
        return ForwardMessageToDeadLetterQueueRequest.newBuilder().setGroup(getProtobufGroup()).setTopic(topicResource)
            .setReceiptHandle(messageView.getReceiptHandle())
            .setMessageId(messageView.getMessageId().toString())
            .setDeliveryAttempt(messageView.getDeliveryAttempt())
            .setMaxDeliveryAttempts(getRetryPolicy().getMaxAttempts()).build();
    }

    public RpcFuture<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse>
    forwardMessageToDeadLetterQueue(final MessageViewImpl messageView) {
        // Intercept before forwarding message to DLQ.
        final List<GeneralMessage> generalMessages = Collections.singletonList(new GeneralMessageImpl(messageView));
        MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.FORWARD_TO_DLQ);
        doBefore(context, generalMessages);

        final Endpoints endpoints = messageView.getEndpoints();
        RpcFuture<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse> future;
        final ForwardMessageToDeadLetterQueueRequest request =
            wrapForwardMessageToDeadLetterQueueRequest(messageView);
        future = this.getClientManager().forwardMessageToDeadLetterQueue(endpoints, request,
            clientConfiguration.getRequestTimeout());
        Futures.addCallback(future, new FutureCallback<ForwardMessageToDeadLetterQueueResponse>() {
            @Override
            public void onSuccess(ForwardMessageToDeadLetterQueueResponse response) {
                // Intercept after forwarding message to DLQ.
                MessageHookPointsStatus status = Code.OK.equals(response.getStatus().getCode()) ?
                    MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
                final MessageInterceptorContext context0 = new MessageInterceptorContextImpl(context, status);
                doAfter(context0, generalMessages);
            }

            @Override
            public void onFailure(Throwable t) {
                // Intercept after forwarding message to DLQ.
                final MessageInterceptorContext context0 = new MessageInterceptorContextImpl(context,
                    MessageHookPointsStatus.ERROR);
                doAfter(context0, generalMessages);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public void doStats() {
        final long receptionTimes = this.receptionTimes.getAndSet(0);
        final long receivedMessagesQuantity = this.receivedMessagesQuantity.getAndSet(0);

        final long consumptionOkQuantity = this.consumptionOkQuantity.getAndSet(0);
        final long consumptionErrorQuantity = this.consumptionErrorQuantity.getAndSet(0);

        log.info("clientId={}, consumerGroup={}, receptionTimes={}, receivedMessagesQuantity={}, "
                + "consumptionOkQuantity={}, consumptionErrorQuantity={}", clientId, consumerGroup, receptionTimes,
            receivedMessagesQuantity, consumptionOkQuantity, consumptionErrorQuantity);
        processQueueTable.values().forEach(ProcessQueue::doStats);
    }

    public RetryPolicy getRetryPolicy() {
        return pushSubscriptionSettings.getRetryPolicy();
    }

    public ThreadPoolExecutor getConsumptionExecutor() {
        return consumptionExecutor;
    }
}
