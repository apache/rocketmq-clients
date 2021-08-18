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

import static com.google.common.base.Preconditions.checkNotNull;

import apache.rocketmq.v1.ClientResourceBundle;
import apache.rocketmq.v1.ConsumeMessageType;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.ConsumerGroup;
import apache.rocketmq.v1.DeadLetterPolicy;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.ResponseCommon;
import apache.rocketmq.v1.SubscriptionEntry;
import apache.rocketmq.v1.VerifyMessageConsumptionRequest;
import apache.rocketmq.v1.VerifyMessageConsumptionResponse;
import com.google.common.base.Function;
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
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.ClientImpl;
import org.apache.rocketmq.client.impl.ServiceState;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.Broker;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.Permission;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMQPushConsumerImpl extends ClientImpl {
    private static final Logger log = LoggerFactory.getLogger(DefaultMQPushConsumerImpl.class);

    /**
     * For {@link MessageModel#CLUSTERING} only, reflects the times of message reception.
     */
    private final AtomicLong receptionTimes;
    /**
     * For {@link MessageModel#CLUSTERING} only, reflects the quantity of received messages.
     */
    private final AtomicLong receivedMessagesQuantity;

    /**
     * For {@link MessageModel#BROADCASTING} only, reflects the times of pull message.
     */
    private final AtomicLong pullTimes;
    /**
     * For {@link MessageModel#BROADCASTING} only, reflects the quantity of pulled messages.
     */
    private final AtomicLong pulledMessagesQuantity;

    /**
     * Record times of successful message consumption.
     */
    private final AtomicLong consumptionOkQuantity;
    /**
     * Record times of failed message consumption.
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
     * If a FIFO message was failed to consume, it would be suspend for a while to prepare for next delivery until
     * delivery attempt times is run out.
     */
    private long fifoConsumptionSuspendTimeMillis = 1000L;

    /**
     * Maximum batch size for each message consumption. It does not make sense for FIFO message.
     */
    private int consumeMessageBatchMaxSize = 1;

    /**
     * Consumption thread amount, which determines the consumption speed to some degree.
     */
    private int consumptionThreadsAmount = 32;

    /**
     * for {@link MessageModel#CLUSTERING} only. It shows the Maximum delivery attempts for each message.
     */
    private int maxDeliveryAttempts = 17;

    /**
     * Message model for current consumption group.
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * Indicates the first consumption's position of consumer.
     *
     * <p>In cluster consumption model, it is UNDEFINED if consumer use different timestamp in the same group.
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.END;

    /**
     * Indicates first consumption's time point of consumer by timestamp. Note that setting this option does not take
     * effect while {@link #consumeFromWhere} is not {@link ConsumeFromWhere#TIMESTAMP}.
     *
     * <p>In cluster consumption model, timestamp here indicates the position of all consumer's first consumption.
     * Which is UNDEFINED if consumers use different timestamp in same group.
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

    private final ConcurrentMap<String /* topic */, TopicAssignment> cachedTopicAssignmentTable;

    private final ConcurrentMap<String /* topic */, RateLimiter> rateLimiterTable;

    private final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;

    private volatile ScheduledFuture<?> scanAssignmentsFuture;

    public DefaultMQPushConsumerImpl(String group) {
        super(group);
        this.filterExpressionTable = new ConcurrentHashMap<String, FilterExpression>();
        this.cachedTopicAssignmentTable = new ConcurrentHashMap<String, TopicAssignment>();

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
        checkNotNull(offsetStore);
        this.offsetStore = offsetStore;
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

    public boolean hasCustomOffsetStore() {
        return null != offsetStore;
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

    public long readOffset(MessageQueue mq) {
        return offsetStore.readOffset(mq);
    }

    @Override
    public void start() throws ClientException {
        synchronized (this) {
            log.info("Begin to start the rocketmq push consumer");
            if (null == messageListener) {
                throw new ClientException(ErrorCode.NO_LISTENER_REGISTERED);
            }
            super.start();
            this.generateConsumeService();
            consumeService.start();

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
            if (ServiceState.STARTED == getState()) {
                log.info("The rocketmq push consumer starts successfully.");
            }
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        synchronized (this) {
            log.info("Begin to shutdown the rocketmq push consumer.");

            if (ServiceState.STARTED == getState()) {
                if (null != scanAssignmentsFuture) {
                    scanAssignmentsFuture.cancel(false);
                }
                super.shutdown();
                if (null != consumeService) {
                    consumeService.shutdown();
                }
                consumptionExecutor.shutdown();
                if (!consumptionExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                    log.error("[Bug] Failed to shutdown the consumption executor.");
                }
                log.info("Shutdown the rocketmq push consumer successfully.");
            }
        }
    }

    RateLimiter rateLimiter(String topic) {
        return rateLimiterTable.get(topic);
    }

    public void throttle(String topic, double permitsPerSecond) {
        final RateLimiter rateLimiter = RateLimiter.create(permitsPerSecond);
        rateLimiterTable.put(topic, rateLimiter);
    }

    private QueryAssignmentRequest wrapQueryAssignmentRequest(String topic, Endpoints endpoints) {
        Resource topicResource = Resource.newBuilder().setArn(arn).setName(topic).build();
        return QueryAssignmentRequest.newBuilder()
                                     .setTopic(topicResource)
                                     .setEndpoints(endpoints.toEndpoints())
                                     .setGroup(getGroupResource())
                                     .setClientId(clientId)
                                     .build();
    }

    public void scanAssignments() {
        try {
            log.debug("Start to scan assignments periodically");
            for (Map.Entry<String, FilterExpression> entry : filterExpressionTable.entrySet()) {
                final String topic = entry.getKey();
                final FilterExpression filterExpression = entry.getValue();
                final TopicAssignment local = cachedTopicAssignmentTable.get(topic);
                final ListenableFuture<TopicAssignment> future = queryAssignment(topic);
                Futures.addCallback(future, new FutureCallback<TopicAssignment>() {
                    @Override
                    public void onSuccess(TopicAssignment remote) {
                        if (remote.getAssignmentList().isEmpty()) {
                            log.info("Acquired empty assignments from remote, topic={}", topic);
                            if (null == local || local.getAssignmentList().isEmpty()) {
                                log.info("No available assignments now, would scan later, topic={}", topic);
                                return;
                            }
                            log.info("Attention!!! acquired empty assignments from remote, topic={}", topic);
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

        log.info("ConsumerGroup={}, receiveTimes={}, receivedMessagesQuantity={}, pullTimes={}, "
                 + "pulledMessagesQuantity={}, consumptionOkQuantity={}, consumptionErrorQuantity={}", group,
                 receiveTimes, receivedMessagesQuantity, pullTimes, pulledMessagesQuantity, consumptionOkQuantity,
                 consumptionErrorQuantity);
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
            String topic, TopicAssignment topicAssignment, FilterExpression filterExpression) {
        Set<MessageQueue> latestMqs = new HashSet<MessageQueue>();

        final List<Assignment> assignments = topicAssignment.getAssignmentList();
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

            if (null == pq) {
                log.error("[Bug] Process queue is null, mq={}", mq);
                continue;
            }

            if (!latestMqs.contains(mq)) {
                log.info("Stop to receive message queue according to the latest assignments, mq={}", mq);
                processQueueTable.remove(mq);
                dropProcessQueue(mq);
                continue;
            }

            if (pq.expired()) {
                log.warn("Process queue is expired, mq={}", mq);
                processQueueTable.remove(mq);
                dropProcessQueue(mq);
                continue;
            }
            activeMqs.add(mq);
        }

        for (MessageQueue mq : latestMqs) {
            if (!activeMqs.contains(mq)) {
                final ProcessQueue pq = getProcessQueue(mq, filterExpression);
                // for clustering mode.
                if (MessageModel.CLUSTERING.equals(messageModel)) {
                    log.info("Start to receive message from mq according to the latest assignments, mq={}", mq);
                    pq.receiveMessageImmediately();
                    continue;
                }
                // for broadcasting mode.
                log.info("Start to pull message from mq according to the latest assignments, mq={}", mq);
                pq.pullMessageImmediately();
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
        checkNotNull(messageListenerConcurrently);
        this.messageListener = messageListenerConcurrently;
    }

    public void registerMessageListener(MessageListenerOrderly messageListenerOrderly) {
        checkNotNull(messageListenerOrderly);
        this.messageListener = messageListenerOrderly;
    }

    private ListenableFuture<Endpoints> pickRouteEndpoints(String topic) {
        final ListenableFuture<TopicRouteData> future = getRouteFor(topic);
        return Futures.transformAsync(future, new AsyncFunction<TopicRouteData, Endpoints>() {
            @Override
            public ListenableFuture<Endpoints> apply(TopicRouteData topicRouteData) throws Exception {
                final SettableFuture<Endpoints> future0 = SettableFuture.create();
                final List<Partition> partitions = topicRouteData.getPartitions();
                for (int i = 0; i < partitions.size(); i++) {
                    final Partition partition =
                            partitions.get(TopicAssignment.getNextPartitionIndex() % partitions.size());
                    final Broker broker = partition.getBroker();
                    if (MixAll.MASTER_BROKER_ID != broker.getId()) {
                        continue;
                    }
                    if (Permission.NONE == partition.getPermission()) {
                        continue;
                    }
                    future0.set(broker.getEndpoints());
                    return future0;
                }
                throw new ServerException(ErrorCode.NO_PERMISSION);
            }
        });
    }

    private ListenableFuture<TopicAssignment> queryAssignment(final String topic) {
        // for broadcasting mode, return full topic route.
        if (MessageModel.BROADCASTING == messageModel) {
            final ListenableFuture<TopicRouteData> future = getRouteFor(topic);
            return Futures.transform(future, new Function<TopicRouteData, TopicAssignment>() {
                @Override
                public TopicAssignment apply(TopicRouteData topicRouteData) {
                    return new TopicAssignment(topicRouteData);
                }
            });
        }
        // for clustering mode.
        final ListenableFuture<Endpoints> future = pickRouteEndpoints(topic);
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
        return Futures.transformAsync(responseFuture, new AsyncFunction<QueryAssignmentResponse, TopicAssignment>() {
            @Override
            public ListenableFuture<TopicAssignment> apply(QueryAssignmentResponse response) throws Exception {
                SettableFuture<TopicAssignment> future0 = SettableFuture.create();
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to query assignment, topic={}, code={}, message={}", topic, code,
                              status.getMessage());
                    throw new ClientException(ErrorCode.NO_ASSIGNMENT);
                }
                final TopicAssignment topicAssignment = new TopicAssignment(response.getAssignmentsList());
                future0.set(topicAssignment);
                return future0;
            }
        });
    }

    @Override
    public HeartbeatEntry prepareHeartbeatData() {
        List<SubscriptionEntry> subscriptionEntries = new ArrayList<SubscriptionEntry>();
        for (Map.Entry<String, FilterExpression> entry : filterExpressionTable.entrySet()) {
            final String topic = entry.getKey();
            final FilterExpression filterExpression = entry.getValue();

            Resource topicResource = Resource.newBuilder().setArn(arn).setName(topic).build();
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

        final ConsumerGroup.Builder builder = ConsumerGroup.newBuilder()
                                                           .setGroup(getGroupResource())
                                                           .addAllSubscriptions(subscriptionEntries)
                                                           .setDeadLetterPolicy(deadLetterPolicy)
                                                           .setConsumeType(ConsumeMessageType.POP);

        switch (messageModel) {
            case CLUSTERING:
                builder.setConsumeModel(ConsumeModel.CLUSTERING);
                break;
            case BROADCASTING:
                builder.setConsumeModel(ConsumeModel.BROADCASTING);
                break;
            default:
                builder.setConsumeModel(ConsumeModel.UNRECOGNIZED);
        }

        switch (consumeFromWhere) {
            case BEGINNING:
                builder.setConsumePolicy(ConsumePolicy.PLAYBACK);
                break;
            case TIMESTAMP:
                builder.setConsumePolicy(ConsumePolicy.TARGET_TIMESTAMP);
                break;
            case END:
            default:
                builder.setConsumePolicy(ConsumePolicy.RESUME);
        }
        final ConsumerGroup consumerGroup = builder.build();

        return HeartbeatEntry.newBuilder()
                             .setClientId(clientId)
                             .setConsumerGroup(consumerGroup)
                             .setNeedRebalance(messageListener.getListenerType().equals(MessageListenerType.ORDERLY))
                             .build();
    }

    @Override
    public ListenableFuture<VerifyMessageConsumptionResponse> verifyConsumption(VerifyMessageConsumptionRequest
                                                                                        request) {
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
                return VerifyMessageConsumptionResponse.newBuilder().setCommon(common).build();
            }
        });
    }

    public ListenableFuture<ConsumeStatus> verifyConsumption0(VerifyMessageConsumptionRequest request) {
        final Partition partition = new Partition(request.getPartition());
        final Message message = request.getMessage();
        MessageImpl messageImpl;
        final SettableFuture<ConsumeStatus> future = SettableFuture.create();
        try {
            messageImpl = ClientImpl.wrapMessageImpl(message);
        } catch (Throwable t) {
            log.error("Message verify consumption is corrupted, partition={}, messageId={}", partition,
                      message.getSystemAttribute().getMessageId());
            future.setException(t);
            return future;
        }
        final MessageExt messageExt = new MessageExt(messageImpl);
        return consumeService.consume(messageExt);
    }

    @Override
    public ClientResourceBundle wrapClientResourceBundle() {
        final ClientResourceBundle.Builder builder =
                ClientResourceBundle.newBuilder().setClientId(clientId).setProducerGroup(getGroupResource());
        for (String topic : filterExpressionTable.keySet()) {
            Resource topicResource = Resource.newBuilder().setArn(arn).setName(topic).build();
            builder.addTopics(topicResource);
        }
        return builder.build();
    }

    public void setConsumptionThreadsAmount(int threadsAmount) {
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
        this.messageModel = messageModel;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
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

    public void setScanAssignmentsFuture(ScheduledFuture<?> scanAssignmentsFuture) {
        this.scanAssignmentsFuture = scanAssignmentsFuture;
    }

    ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return this.processQueueTable;
    }
}
