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

package org.apache.rocketmq.client.impl.consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.ConsumeMessageType;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.ConsumerData;
import apache.rocketmq.v1.DeadLetterPolicy;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.GenericPollingRequest;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.ResponseCommon;
import apache.rocketmq.v1.SubscriptionEntry;
import apache.rocketmq.v1.VerifyMessageConsumptionRequest;
import apache.rocketmq.v1.VerifyMessageConsumptionResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Metadata;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.filter.ExpressionType;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.listener.MessageListenerType;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageHookPointStatus;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageImplAccessor;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.ExecutorServices;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings(value = {"UnstableApiUsage", "NullableProblems"})
public class PushConsumerImpl extends ConsumerImpl {
    private static final Logger log = LoggerFactory.getLogger(PushConsumerImpl.class);

    /**
     * For {@link MessageModel#CLUSTERING} only, indicates the times of message reception.
     */
    private final AtomicLong receptionTimes;
    /**
     * For {@link MessageModel#CLUSTERING} only, indicates the quantity of received messages.
     */
    private final AtomicLong receivedMessagesQuantity;

    /**
     * For {@link MessageModel#BROADCASTING} only, indicates the times of pull message.
     */
    private final AtomicLong pullTimes;
    /**
     * For {@link MessageModel#BROADCASTING} only, indicates the quantity of pulled messages.
     */
    private final AtomicLong pulledMessagesQuantity;

    /**
     * Indicates the times of successful message consumption.
     */
    private final AtomicLong consumptionOkQuantity;
    /**
     * Indicates the times of failed message consumption.
     */
    private final AtomicLong consumptionErrorQuantity;

    /**
     * Limit cached messages quantity in all {@link ProcessQueue}, higher priority than
     * {@link #maxCachedMessagesQuantityThresholdPerQueue} if it is set. Less than zero
     * indicates that it is not set.
     */
    private int maxTotalCachedMessagesQuantityThreshold = -1;

    /**
     * Limit cached messages quantity in each {@link ProcessQueue}, only make sense if
     * {@link #maxTotalCachedMessagesBytesThreshold} is not set.
     */
    private int maxCachedMessagesQuantityThresholdPerQueue = 1024;

    /**
     * Limit cached message memory bytes in all {@link ProcessQueue}, higher priority than
     * {@link #maxCachedMessagesQuantityThresholdPerQueue} if it is set. Less than zero
     * indicates that it is not set.
     */
    private int maxTotalCachedMessagesBytesThreshold = -1;

    /**
     * Limit cached messages memory bytes in each {@link ProcessQueue}, only make sense if
     * {@link #maxTotalCachedMessagesBytesThreshold} is not set.
     */
    private int maxCachedMessagesBytesThresholdPerQueue = 4 * 1024 * 1024;

    /**
     * If a FIFO message was failed to consume, it would be suspended for a while to prepare for next delivery until
     * delivery attempt times is run out.
     */
    private long fifoConsumptionSuspendTimeMillis = 1000L;

    /**
     * Maximum batch size for each message consumption. It does not make sense for FIFO message.
     */
    private int consumeMessageBatchMaxSize = 1;

    /**
     * Consumption thread amount, which determines the consumption rate to some degree.
     */
    private int consumptionThreadsAmount = 32;

    /**
     * for {@link MessageModel#CLUSTERING} only. It shows the Maximum delivery attempts for each message.
     */
    private int maxDeliveryAttempts = 1 + 16;

    /**
     * Message model for current consumption group.
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * Indicates the first consumption's position of consumer.
     *
     * <p>In cluster consumption model, it is <strong>UNDEFINED</strong> if consumer use different timestamp in the
     * same group.
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * Indicates first consumption's time point of consumer by timestamp. Note that setting this option does not take
     * effect while {@link #consumeFromWhere} is not {@link ConsumeFromWhere#CONSUME_FROM_TIMESTAMP}.
     *
     * <p>In cluster consumption model, timestamp here indicates the position of all consumer's first consumption.
     * Which is <strong>UNDEFINED</strong> if consumers use different timestamp in same group.
     *
     * <p>In broadcasting consumption model, timestamp here are individual for each consumer, even though they belong
     * to the same group.
     */
    private long consumeFromTimeMillis = System.currentTimeMillis();

    /**
     * Timeout of consumption shows failure of consumption, message would be delivered once again until run out of
     * delivery attempt times.
     */
    private long consumptionTimeoutMillis = 15 * 60 * 1000L;

    /**
     * Indicates the max time that server should hold the request if message pulled/received for queue from
     * server is not satisfied with the {@link #maxAwaitBatchSizePerQueue}.
     */
    private long maxAwaitTimeMillisPerQueue = 0;

    /**
     * Indicates the max batch quantity of messages that server returned for each queue.
     */
    private int maxAwaitBatchSizePerQueue = 32;

    private OffsetStore offsetStore = null;

    private MessageListener messageListener;

    private volatile ConsumeService consumeService;

    private final ThreadPoolExecutor consumptionExecutor;

    private final ConcurrentMap<String /* topic */, FilterExpression> filterExpressionTable;

    private final ConcurrentMap<String /* topic */, TopicAssignments> cachedTopicAssignmentTable;

    private final ConcurrentMap<String /* topic */, RateLimiter> rateLimiterTable;

    private final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;

    private volatile ScheduledFuture<?> scanAssignmentsFuture;

    public PushConsumerImpl(String group) throws ClientException {
        super(group);
        this.filterExpressionTable = new ConcurrentHashMap<String, FilterExpression>();
        this.cachedTopicAssignmentTable = new ConcurrentHashMap<String, TopicAssignments>();

        this.messageListener = null;
        this.consumeService = null;

        this.rateLimiterTable = new ConcurrentHashMap<String, RateLimiter>();

        this.processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>();

        this.receptionTimes = new AtomicLong(0);
        this.receivedMessagesQuantity = new AtomicLong(0);

        this.pullTimes = new AtomicLong(0);
        this.pulledMessagesQuantity = new AtomicLong(0);

        this.consumptionOkQuantity = new AtomicLong(0);
        this.consumptionErrorQuantity = new AtomicLong(0);

        this.consumptionExecutor = new ThreadPoolExecutor(
                consumptionThreadsAmount,
                consumptionThreadsAmount,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("MessageConsumption"));
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = checkNotNull(offsetStore, "offsetStore");
    }

    private void generateConsumeService() throws ClientException {
        switch (messageListener.getListenerType()) {
            case CONCURRENTLY:
                this.consumeService = new ConsumeConcurrentlyService(messageListener, this, consumptionExecutor,
                                                                     this.getScheduler(), processQueueTable,
                                                                     consumeMessageBatchMaxSize);
                break;
            case ORDERLY:
                this.consumeService = new ConsumeOrderlyService(messageListener, this, consumptionExecutor,
                                                                this.getScheduler(), processQueueTable);
                break;
            default:
                throw new ClientException(ErrorCode.NO_LISTENER_REGISTERED);
        }
    }

    int cachedMessagesQuantityThresholdPerQueue() {
        if (maxTotalCachedMessagesQuantityThreshold <= 0) {
            return maxCachedMessagesQuantityThresholdPerQueue;
        }
        final int size = processQueueTable.size();
        // all process queues have been removed, no need to cache message.
        if (size <= 0) {
            return 0;
        }
        return Math.max(1, maxTotalCachedMessagesQuantityThreshold / size);
    }

    int cachedMessagesBytesThresholdPerQueue() {
        if (maxTotalCachedMessagesBytesThreshold <= 0) {
            return maxCachedMessagesBytesThresholdPerQueue;
        }
        final int size = processQueueTable.size();
        // all process queues have been removed, no need to cache message.
        if (size <= 0) {
            return 0;
        }
        return Math.max(1, maxTotalCachedMessagesBytesThreshold / size);
    }

    public Optional<Long> readOffset(MessageQueue mq) {
        return offsetStore.readOffset(mq);
    }

    @Override
    public void setUp() throws ClientException {
        log.info("Begin to start the rocketmq push consumer.");
        if (null == messageListener) {
            throw new ClientException(ErrorCode.NO_LISTENER_REGISTERED);
        }
        super.setUp();
        this.generateConsumeService();
        consumeService.startAsync().awaitRunning();

        final ScheduledExecutorService scheduler = clientManager.getScheduler();
        scanAssignmentsFuture = scheduler.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            scanAssignments();
                        } catch (Throwable t) {
                            log.error("Exception raised while scanning the load assignments.", t);
                        }
                    }
                },
                1,
                5,
                TimeUnit.SECONDS);
        log.info("The rocketmq push consumer starts successfully.");
    }

    @Override
    public void tearDown() throws InterruptedException {
        log.info("Begin to shutdown the rocketmq push consumer, clientId={}", id);
        if (null != scanAssignmentsFuture) {
            scanAssignmentsFuture.cancel(false);
        }
        super.tearDown();
        if (null != consumeService) {
            consumeService.stopAsync().awaitTerminated();
        }
        consumptionExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(consumptionExecutor)) {
            log.error("[Bug] Failed to shutdown the consumption executor, clientId={}", id);
        }
        log.info("Shutdown the rocketmq push consumer successfully, clientId={}", id);
    }

    public void start() {
        clientService.startAsync().awaitRunning();
    }

    public void shutdown() {
        clientService.stopAsync().awaitTerminated();
    }

    @Override
    public void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData) {
    }

    RateLimiter rateLimiter(String topic) {
        return rateLimiterTable.get(topic);
    }

    public void rateLimit(String topic, double permitsPerSecond) {
        final RateLimiter rateLimiter = RateLimiter.create(permitsPerSecond);
        rateLimiterTable.put(topic, rateLimiter);
    }

    private QueryAssignmentRequest wrapQueryAssignmentRequest(String topic, Endpoints endpoints) {
        Resource topicResource = Resource.newBuilder().setResourceNamespace(namespace).setName(topic).build();
        return QueryAssignmentRequest.newBuilder().setTopic(topicResource).setEndpoints(endpoints.toPbEndpoints())
                                     .setGroup(getPbGroup()).setClientId(id).build();
    }

    @VisibleForTesting
    public void scanAssignments() {
        try {
            log.debug("Start to scan assignments periodically");
            for (Map.Entry<String, FilterExpression> entry : filterExpressionTable.entrySet()) {
                final String topic = entry.getKey();
                final FilterExpression filterExpression = entry.getValue();
                final TopicAssignments local = cachedTopicAssignmentTable.get(topic);
                final ListenableFuture<TopicAssignments> future = queryAssignment(topic);
                Futures.addCallback(future, new FutureCallback<TopicAssignments>() {
                    @Override
                    public void onSuccess(TopicAssignments remote) {
                        if (remote.getAssignmentList().isEmpty()) {
                            if (null == local || local.getAssignmentList().isEmpty()) {
                                log.info("Acquired empty assignments from remote, would scan later, topic={}", topic);
                                return;
                            }
                            log.info("Attention!!! acquired empty assignments from remote, but local assignments is "
                                     + "not empty, topic={}", topic);
                        }

                        if (!remote.equals(local)) {
                            log.info("Assignments of topic={} has changed, {} -> {}", topic, local, remote);
                            synchronizeProcessQueue(topic, remote, filterExpression);
                            cachedTopicAssignmentTable.put(topic, remote);
                            return;
                        }
                        // process queue may be dropped, need to be synchronized anyway.
                        synchronizeProcessQueue(topic, remote, filterExpression);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.error("Exception raised while scanning the assignments, topic={}", topic, t);
                    }
                });
            }
        } catch (Throwable t) {
            log.error("Exception raised while scanning the assignments for all topics.", t);
        }
    }

    @Override
    public void doStats() {
        // for cluster consumption mode.
        final long receiveTimes = this.receptionTimes.getAndSet(0);
        final long receivedMessagesQuantity = this.receivedMessagesQuantity.getAndSet(0);

        // for broadcasting consumption mode.
        final long pullTimes = this.pullTimes.getAndSet(0);
        final long pulledMessagesQuantity = this.pulledMessagesQuantity.getAndSet(0);

        final long consumptionOkQuantity = this.consumptionOkQuantity.getAndSet(0);
        final long consumptionErrorQuantity = this.consumptionErrorQuantity.getAndSet(0);

        log.info("clientId={}, namespace={}, group={}, receiveTimes={}, receivedMessagesQuantity={}, pullTimes={}, "
                 + "pulledMessagesQuantity={}, consumptionOkQuantity={}, consumptionErrorQuantity={}", id,
                 namespace, group, receiveTimes, receivedMessagesQuantity, pullTimes, pulledMessagesQuantity,
                 consumptionOkQuantity, consumptionErrorQuantity);

        for (ProcessQueue pq : processQueueTable.values()) {
            pq.doStats();
        }
    }

    /**
     * Drop {@link ProcessQueue} by {@link MessageQueue}, {@link ProcessQueue} must be removed before it is dropped.
     *
     * @param mq message queue.
     */
    void dropProcessQueue(MessageQueue mq) {
        final ProcessQueue pq = processQueueTable.remove(mq);
        if (null != pq) {
            pq.drop();
        }
    }

    /**
     * Get {@link ProcessQueue} by {@link MessageQueue} and {@link FilterExpression}. ensure the returned
     * {@link ProcessQueue} has been added to the {@link #processQueueTable} and not dropped. <strong>Never
     * </strong> return null.
     *
     * @param mq               message queue.
     * @param filterExpression filter expression of topic.
     * @return {@link ProcessQueue} by {@link MessageQueue}. <strong>Never</strong> return null.
     */
    private ProcessQueue getProcessQueue(MessageQueue mq, final FilterExpression filterExpression) {
        final ProcessQueueImpl processQueue = new ProcessQueueImpl(this, mq, filterExpression);
        final ProcessQueue previous = processQueueTable.putIfAbsent(mq, processQueue);
        if (null != previous) {
            return previous;
        }
        return processQueue;
    }

    private void synchronizeProcessQueue(
            String topic, TopicAssignments topicAssignments, FilterExpression filterExpression) {
        Set<MessageQueue> latestMqs = new HashSet<MessageQueue>();

        final List<Assignment> assignments = topicAssignments.getAssignmentList();
        for (Assignment assignment : assignments) {
            latestMqs.add(assignment.getMessageQueue());
        }

        Set<MessageQueue> activeMqs = new HashSet<MessageQueue>();

        for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
            final MessageQueue mq = entry.getKey();
            final ProcessQueue pq = entry.getValue();
            if (!topic.equals(mq.getTopic())) {
                continue;
            }

            if (!latestMqs.contains(mq)) {
                log.info("Drop message queue according to the latest assignments, mq={}", mq);
                dropProcessQueue(mq);
                continue;
            }

            if (pq.expired()) {
                log.warn("Drop message queue because it is expired, mq={}", mq);
                dropProcessQueue(mq);
                continue;
            }
            activeMqs.add(mq);
        }

        for (MessageQueue mq : latestMqs) {
            if (!activeMqs.contains(mq)) {
                final ProcessQueue pq = getProcessQueue(mq, filterExpression);
                log.info("Start to fetch message from remote, mq={}", mq);
                pq.fetchMessageImmediately();
            }
        }
    }

    public void subscribe(final String topic, String expression, ExpressionType expressionType) {
        final FilterExpression filterExpression = new FilterExpression(expression, expressionType);
        filterExpressionTable.put(topic, filterExpression);
    }

    public void unsubscribe(final String topic) {
        filterExpressionTable.remove(topic);
    }

    public void registerMessageListener(MessageListenerConcurrently messageListenerConcurrently) {
        this.messageListener = checkNotNull(messageListenerConcurrently, "messageListenerConcurrently");
    }

    public void registerMessageListener(MessageListenerOrderly messageListenerOrderly) {
        this.messageListener = checkNotNull(messageListenerOrderly, "messageListenerOrderly");
    }

    private ListenableFuture<Endpoints> pickRouteEndpointsToQueryAssignments(String topic) {
        final ListenableFuture<TopicRouteData> future = getRouteData(topic);
        return Futures.transformAsync(future, new AsyncFunction<TopicRouteData, Endpoints>() {
            @Override
            public ListenableFuture<Endpoints> apply(TopicRouteData topicRouteData) throws Exception {
                final SettableFuture<Endpoints> future0 = SettableFuture.create();
                final Endpoints endpoints = topicRouteData.pickEndpointsToQueryAssignments();
                future0.set(endpoints);
                return future0;
            }
        });
    }

    private ListenableFuture<TopicAssignments> queryAssignment(final String topic) {
        // for broadcasting mode, return full topic route.
        if (MessageModel.BROADCASTING.equals(messageModel)) {
            final ListenableFuture<TopicRouteData> future = getRouteData(topic);
            return Futures.transform(future, new Function<TopicRouteData, TopicAssignments>() {
                @Override
                public TopicAssignments apply(TopicRouteData topicRouteData) {
                    return new TopicAssignments(topicRouteData);
                }
            });
        }
        // for clustering mode.
        final ListenableFuture<Endpoints> future = pickRouteEndpointsToQueryAssignments(topic);
        final ListenableFuture<QueryAssignmentResponse> responseFuture =
                Futures.transformAsync(future, new AsyncFunction<Endpoints, QueryAssignmentResponse>() {
                    @Override
                    public ListenableFuture<QueryAssignmentResponse> apply(Endpoints endpoints) throws Exception {
                        final Metadata metadata = sign();
                        final QueryAssignmentRequest request = wrapQueryAssignmentRequest(topic, endpoints);
                        return clientManager.queryAssignment(endpoints, metadata, request, ioTimeoutMillis,
                                                             TimeUnit.MILLISECONDS);
                    }
                });
        return Futures.transformAsync(responseFuture, new AsyncFunction<QueryAssignmentResponse, TopicAssignments>() {
            @Override
            public ListenableFuture<TopicAssignments> apply(QueryAssignmentResponse response) throws Exception {
                SettableFuture<TopicAssignments> future0 = SettableFuture.create();
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to query assignment, topic={}, code={}, status message=[{}]", topic, code,
                              status.getMessage());
                    throw new ClientException(ErrorCode.NO_ASSIGNMENT);
                }
                final TopicAssignments topicAssignments = new TopicAssignments(response.getAssignmentsList());
                future0.set(topicAssignments);
                return future0;
            }
        });
    }

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        List<SubscriptionEntry> subscriptionEntries = new ArrayList<SubscriptionEntry>();
        for (Map.Entry<String, FilterExpression> entry : filterExpressionTable.entrySet()) {
            final String topic = entry.getKey();
            final FilterExpression filterExpression = entry.getValue();

            Resource topicResource = Resource.newBuilder().setResourceNamespace(namespace).setName(topic).build();
            final apache.rocketmq.v1.FilterExpression.Builder builder =
                    apache.rocketmq.v1.FilterExpression.newBuilder().setExpression(filterExpression.getExpression());
            switch (filterExpression.getExpressionType()) {
                case TAG:
                    builder.setType(FilterType.TAG);
                    break;
                case SQL92:
                default:
                    builder.setType(FilterType.SQL);
            }
            final apache.rocketmq.v1.FilterExpression expression = builder.build();
            SubscriptionEntry subscriptionEntry =
                    SubscriptionEntry.newBuilder().setTopic(topicResource).setExpression(expression).build();
            subscriptionEntries.add(subscriptionEntry);
        }

        final DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.newBuilder()
                                                                  .setMaxDeliveryAttempts(maxDeliveryAttempts)
                                                                  .build();

        final ConsumerData.Builder builder = ConsumerData.newBuilder()
                                                         .setGroup(getPbGroup())
                                                         .addAllSubscriptions(subscriptionEntries)
                                                         .setDeadLetterPolicy(deadLetterPolicy)
                                                         .setConsumeType(ConsumeMessageType.POP);

        switch (messageModel) {
            case BROADCASTING:
                builder.setConsumeModel(ConsumeModel.BROADCASTING);
                break;
            case CLUSTERING:
            default:
                builder.setConsumeModel(ConsumeModel.CLUSTERING);
        }

        switch (consumeFromWhere) {
            case CONSUME_FROM_FIRST_OFFSET:
                builder.setConsumePolicy(ConsumePolicy.PLAYBACK);
                break;
            case CONSUME_FROM_TIMESTAMP:
                builder.setConsumePolicy(ConsumePolicy.TARGET_TIMESTAMP);
                break;
            case CONSUME_FROM_MAX_OFFSET:
                builder.setConsumePolicy(ConsumePolicy.DISCARD);
                break;
            default:
                builder.setConsumePolicy(ConsumePolicy.RESUME);
        }
        final ConsumerData consumerData = builder.build();

        return HeartbeatRequest.newBuilder()
                               .setClientId(id)
                               .setConsumerData(consumerData)
                               .setFifoFlag(messageListener.getListenerType().equals(MessageListenerType.ORDERLY))
                               .build();
    }

    @Override
    public ListenableFuture<VerifyMessageConsumptionResponse> verifyConsumption(final VerifyMessageConsumptionRequest
                                                                                        request) {
        try {
            final ListenableFuture<ConsumeStatus> future = verifyConsumption0(request);
            return Futures.transform(future, new Function<ConsumeStatus, VerifyMessageConsumptionResponse>() {
                @Override
                public VerifyMessageConsumptionResponse apply(ConsumeStatus consumeStatus) {
                    final Status.Builder builder = Status.newBuilder();
                    Status status;
                    switch (consumeStatus) {
                        case OK:
                            status = builder.setCode(Code.OK_VALUE).build();
                            break;
                        case ERROR:
                        default:
                            status = builder.setCode(Code.ABORTED_VALUE).build();
                            break;
                    }
                    ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
                    return VerifyMessageConsumptionResponse.newBuilder().setCommon(common).setMid(request.getMid())
                                                           .build();
                }
            });
        } catch (Throwable t) {
            log.error("[Bug] Exception raised while verifying message consumption, messageId={}",
                      request.getMessage().getSystemAttribute().getMessageId());
            SettableFuture<VerifyMessageConsumptionResponse> future0 = SettableFuture.create();
            future0.setException(t);
            return future0;
        }
    }

    public ListenableFuture<ConsumeStatus> verifyConsumption0(VerifyMessageConsumptionRequest request) {
        final Message message = request.getMessage();
        MessageImpl messageImpl = MessageImplAccessor.wrapMessageImpl(message);
        final MessageExt messageExt = new MessageExt(messageImpl);
        return consumeService.consume(messageExt);
    }

    private AckMessageRequest wrapAckMessageRequest(MessageExt messageExt) {
        final Resource topicResource =
                Resource.newBuilder().setResourceNamespace(namespace).setName(messageExt.getTopic()).build();
        return AckMessageRequest.newBuilder().setGroup(getPbGroup())
                                .setTopic(topicResource)
                                .setMessageId(messageExt.getMsgId()).setClientId(id)
                                .setReceiptHandle(messageExt.getReceiptHandle()).build();
    }

    public ListenableFuture<AckMessageResponse> ackMessage(final MessageExt messageExt) {
        return ackMessage(messageExt, 1);
    }

    public ListenableFuture<AckMessageResponse> ackMessage(final MessageExt messageExt, int attempt) {
        // intercept before nack
        final MessageInterceptorContext preContext = MessageInterceptorContext.builder().setTopic(messageExt.getTopic())
                                                                              .setAttempt(attempt).build();
        intercept(MessageHookPoint.PRE_ACK_MESSAGE, messageExt, preContext);
        final Stopwatch stopwatch = Stopwatch.createStarted();

        final Endpoints endpoints = messageExt.getAckEndpoints();
        ListenableFuture<AckMessageResponse> future;
        try {
            final AckMessageRequest request = wrapAckMessageRequest(messageExt);
            final Metadata metadata = sign();
            future = clientManager.ackMessage(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            final SettableFuture<AckMessageResponse> future0 = SettableFuture.create();
            future0.setException(t);
            future = future0;
        }
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                // intercept after ack.
                final Code code = Code.forNumber(response.getCommon().getStatus().getCode());
                MessageHookPointStatus hookPointStatus = Code.OK.equals(code) ? MessageHookPointStatus.OK
                                                                              : MessageHookPointStatus.ERROR;
                final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                final MessageInterceptorContext postContext =
                        preContext.toBuilder().setStatus(hookPointStatus).setDuration(duration).build();
                intercept(MessageHookPoint.POST_ACK_MESSAGE, messageExt, postContext);
            }

            @Override
            public void onFailure(Throwable t) {
                // intercept after ack.
                final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                final MessageInterceptorContext postContext =
                        preContext.toBuilder().setStatus(MessageHookPointStatus.ERROR).setThrowable(t)
                                  .setDuration(duration).build();
                intercept(MessageHookPoint.POST_ACK_MESSAGE, messageExt, postContext);
            }
        });
        return future;
    }

    private NackMessageRequest wrapNackMessageRequest(MessageExt messageExt) {
        final Resource topicResource =
                Resource.newBuilder().setResourceNamespace(namespace).setName(messageExt.getTopic()).build();
        return NackMessageRequest.newBuilder()
                                 .setGroup(getPbGroup())
                                 .setTopic(topicResource)
                                 .setClientId(id)
                                 .setReceiptHandle(messageExt.getReceiptHandle())
                                 .setMessageId(messageExt.getMsgId())
                                 .setDeliveryAttempt(messageExt.getDeliveryAttempt())
                                 .setMaxDeliveryAttempts(maxDeliveryAttempts)
                                 .build();
    }

    public ListenableFuture<NackMessageResponse> nackMessage(final MessageExt messageExt) {
        // intercept before nack.
        final MessageInterceptorContext preContext = MessageInterceptorContext.builder().setTopic(messageExt.getTopic())
                                                                              .build();
        intercept(MessageHookPoint.PRE_NACK_MESSAGE, messageExt, preContext);
        final Stopwatch stopwatch = Stopwatch.createStarted();

        final String messageId = messageExt.getMsgId();
        final Endpoints endpoints = messageExt.getAckEndpoints();
        ListenableFuture<NackMessageResponse> future;
        try {
            final NackMessageRequest request = wrapNackMessageRequest(messageExt);
            final Metadata metadata = sign();
            future = clientManager.nackMessage(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            final SettableFuture<NackMessageResponse> future0 = SettableFuture.create();
            future0.setException(t);
            future = future0;
        }
        Futures.addCallback(future, new FutureCallback<NackMessageResponse>() {
            @Override
            public void onSuccess(NackMessageResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());

                // intercept after nack.
                MessageHookPointStatus hookPointStatus = Code.OK.equals(code) ? MessageHookPointStatus.OK
                                                                              : MessageHookPointStatus.ERROR;
                final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                final MessageInterceptorContext postContext =
                        preContext.toBuilder().setStatus(hookPointStatus).setDuration(duration).build();
                intercept(MessageHookPoint.POST_NACK_MESSAGE, messageExt, postContext);

                if (Code.OK.equals(code)) {
                    return;
                }
                log.error("Failed to nack, messageId={}, endpoints={}, code={}, status message=[{}]", messageId,
                          endpoints, code, status.getMessage());
            }

            @Override
            public void onFailure(Throwable t) {
                // intercept after nack
                final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                final MessageInterceptorContext postContext =
                        preContext.toBuilder().setStatus(MessageHookPointStatus.ERROR).setDuration(duration).build();
                intercept(MessageHookPoint.POST_NACK_MESSAGE, messageExt, postContext);

                log.error("Exception raised while nack, messageId={}, endpoints={}", messageId, endpoints, t);
            }
        });
        return future;
    }

    private ForwardMessageToDeadLetterQueueRequest wrapForwardMessageToDeadLetterQueueRequest(MessageExt messageExt) {
        final Resource topicResource =
                Resource.newBuilder().setResourceNamespace(namespace).setName(messageExt.getTopic()).build();
        return ForwardMessageToDeadLetterQueueRequest.newBuilder().setGroup(getPbGroup()).setTopic(topicResource)
                                                     .setClientId(id)
                                                     .setReceiptHandle(messageExt.getReceiptHandle())
                                                     .setMessageId(messageExt.getMsgId())
                                                     .setDeliveryAttempt(messageExt.getDeliveryAttempt())
                                                     .setMaxDeliveryAttempts(maxDeliveryAttempts).build();
    }

    public ListenableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(
            final MessageExt messageExt, int attempt) {
        // intercept before forward message to dlq
        final MessageInterceptorContext preContext = MessageInterceptorContext.builder().setTopic(messageExt.getTopic())
                                                                              .setAttempt(attempt).build();
        intercept(MessageHookPoint.PRE_FORWARD_MESSAGE_TO_DLQ, messageExt, preContext);
        final Stopwatch stopwatch = Stopwatch.createStarted();

        final Endpoints endpoints = messageExt.getAckEndpoints();
        ListenableFuture<ForwardMessageToDeadLetterQueueResponse> future;
        try {
            final ForwardMessageToDeadLetterQueueRequest request =
                    wrapForwardMessageToDeadLetterQueueRequest(messageExt);
            final Metadata metadata = sign();
            future = clientManager.forwardMessageToDeadLetterQueue(endpoints, metadata, request, ioTimeoutMillis,
                                                                   TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            final SettableFuture<ForwardMessageToDeadLetterQueueResponse> future0 = SettableFuture.create();
            future0.setException(t);
            future = future0;
        }
        Futures.addCallback(future, new FutureCallback<ForwardMessageToDeadLetterQueueResponse>() {
            @Override
            public void onSuccess(ForwardMessageToDeadLetterQueueResponse response) {
                // intercept after forward message to dlq.
                final Code code = Code.forNumber(response.getCommon().getStatus().getCode());
                MessageHookPointStatus hookPointStatus = Code.OK.equals(code) ? MessageHookPointStatus.OK
                                                                              : MessageHookPointStatus.ERROR;
                final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                final MessageInterceptorContext postContext =
                        preContext.toBuilder().setStatus(hookPointStatus).setDuration(duration).build();
                intercept(MessageHookPoint.POST_FORWARD_MESSAGE_TO_DLQ, messageExt, postContext);
            }

            @Override
            public void onFailure(Throwable t) {
                // intercept after forward message to dlq.
                final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                final MessageInterceptorContext postContext =
                        preContext.toBuilder().setStatus(MessageHookPointStatus.ERROR).setThrowable(t)
                                  .setDuration(duration).setThrowable(t).build();
                intercept(MessageHookPoint.POST_FORWARD_MESSAGE_TO_DLQ, messageExt, postContext);
            }
        });
        return future;
    }

    @Override
    public GenericPollingRequest wrapGenericPollingRequest() {
        final GenericPollingRequest.Builder builder =
                GenericPollingRequest.newBuilder().setClientId(id).setProducerGroup(getPbGroup());
        for (String topic : filterExpressionTable.keySet()) {
            Resource topicResource = Resource.newBuilder().setResourceNamespace(namespace).setName(topic).build();
            builder.addTopics(topicResource);
        }
        return builder.build();
    }

    public void setConsumptionThreadsAmount(int threadsAmount) {
        checkArgument(threadsAmount > 0, "threadsAmount must be positive");
        this.consumptionThreadsAmount = threadsAmount;
        consumptionExecutor.setCorePoolSize(threadsAmount);
        consumptionExecutor.setMaximumPoolSize(threadsAmount);
    }

    public AtomicLong getReceptionTimes() {
        return this.receptionTimes;
    }

    public AtomicLong getReceivedMessagesQuantity() {
        return this.receivedMessagesQuantity;
    }

    public AtomicLong getPullTimes() {
        return this.pullTimes;
    }

    public AtomicLong getPulledMessagesQuantity() {
        return this.pulledMessagesQuantity;
    }

    public AtomicLong getConsumptionOkQuantity() {
        return this.consumptionOkQuantity;
    }

    public AtomicLong getConsumptionErrorQuantity() {
        return this.consumptionErrorQuantity;
    }

    public int getMaxTotalCachedMessagesQuantityThreshold() {
        return this.maxTotalCachedMessagesQuantityThreshold;
    }

    public int getMaxCachedMessagesQuantityThresholdPerQueue() {
        return this.maxCachedMessagesQuantityThresholdPerQueue;
    }

    public int getMaxTotalCachedMessagesBytesThreshold() {
        return this.maxTotalCachedMessagesBytesThreshold;
    }

    public int getMaxCachedMessagesBytesThresholdPerQueue() {
        return this.maxCachedMessagesBytesThresholdPerQueue;
    }

    public long getFifoConsumptionSuspendTimeMillis() {
        return this.fifoConsumptionSuspendTimeMillis;
    }

    public int getConsumeMessageBatchMaxSize() {
        return this.consumeMessageBatchMaxSize;
    }

    public int getConsumptionThreadsAmount() {
        return this.consumptionThreadsAmount;
    }

    public int getMaxDeliveryAttempts() {
        return this.maxDeliveryAttempts;
    }

    public MessageModel getMessageModel() {
        return this.messageModel;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return this.consumeFromWhere;
    }

    public long getConsumeFromTimeMillis() {
        return this.consumeFromTimeMillis;
    }

    public long getConsumptionTimeoutMillis() {
        return this.consumptionTimeoutMillis;
    }

    public long getMaxAwaitTimeMillisPerQueue() {
        return this.maxAwaitTimeMillisPerQueue;
    }

    public int getMaxAwaitBatchSizePerQueue() {
        return this.maxAwaitBatchSizePerQueue;
    }

    public OffsetStore getOffsetStore() {
        return this.offsetStore;
    }

    public MessageListener getMessageListener() {
        return this.messageListener;
    }

    public ConsumeService getConsumeService() {
        return this.consumeService;
    }

    public ThreadPoolExecutor getConsumptionExecutor() {
        return this.consumptionExecutor;
    }

    public void setMaxTotalCachedMessagesQuantityThreshold(int maxTotalCachedMessagesQuantityThreshold) {
        this.maxTotalCachedMessagesQuantityThreshold = maxTotalCachedMessagesQuantityThreshold;
    }

    public void setMaxCachedMessagesQuantityThresholdPerQueue(int maxCachedMessagesQuantityThresholdPerQueue) {
        this.maxCachedMessagesQuantityThresholdPerQueue = maxCachedMessagesQuantityThresholdPerQueue;
    }

    public void setMaxTotalCachedMessagesBytesThreshold(int maxTotalCachedMessagesBytesThreshold) {
        this.maxTotalCachedMessagesBytesThreshold = maxTotalCachedMessagesBytesThreshold;
    }

    public void setMaxCachedMessagesBytesThresholdPerQueue(int maxCachedMessagesBytesThresholdPerQueue) {
        this.maxCachedMessagesBytesThresholdPerQueue = maxCachedMessagesBytesThresholdPerQueue;
    }

    public void setFifoConsumptionSuspendTimeMillis(long fifoConsumptionSuspendTimeMillis) {
        this.fifoConsumptionSuspendTimeMillis = fifoConsumptionSuspendTimeMillis;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }

    public void setMaxDeliveryAttempts(int maxDeliveryAttempts) {
        this.maxDeliveryAttempts = maxDeliveryAttempts;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = checkNotNull(messageModel, "messageModel");
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = checkNotNull(consumeFromWhere, "consumeFromWhere");
    }

    public void setConsumeFromTimeMillis(long consumeFromTimeMillis) {
        this.consumeFromTimeMillis = consumeFromTimeMillis;
    }

    public void setConsumptionTimeoutMillis(long consumptionTimeoutMillis) {
        this.consumptionTimeoutMillis = consumptionTimeoutMillis;
    }

    public void setMaxAwaitTimeMillisPerQueue(long maxAwaitTimeMillisPerQueue) {
        this.maxAwaitTimeMillisPerQueue = maxAwaitTimeMillisPerQueue;
    }

    public void setMaxAwaitBatchSizePerQueue(int maxAwaitBatchSizePerQueue) {
        this.maxAwaitBatchSizePerQueue = maxAwaitBatchSizePerQueue;
    }
}
