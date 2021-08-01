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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ConsumeFromWhere;
import org.apache.rocketmq.client.constant.Permission;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.ClientBaseImpl;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.route.Broker;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.ThreadFactoryImpl;


@Slf4j
@Getter
@Setter
public class DefaultMQPushConsumerImpl extends ClientBaseImpl {

    // for cluster consumption mode.
    public final AtomicLong receiveTimes;
    public final AtomicLong receivedMessagesSize;

    // for broadcasting consumption mode.
    public final AtomicLong pullTimes;
    public final AtomicLong pulledMessagesSize;

    public final AtomicLong consumptionOkCount;
    public final AtomicLong consumptionErrorCount;

    @Setter(AccessLevel.NONE)
    private MessageListener messageListener;

    @Setter(AccessLevel.NONE)
    private volatile ConsumeService consumeService;

    @Setter(AccessLevel.NONE)
    private final ThreadPoolExecutor consumptionExecutor;

    @Getter(AccessLevel.NONE)
    private final ConcurrentMap<String /* topic */, FilterExpression> filterExpressionTable;

    @Getter(AccessLevel.NONE)
    private final ConcurrentMap<String /* topic */, TopicAssignment> cachedTopicAssignmentTable;

    @Getter(AccessLevel.NONE)
    private final ConcurrentMap<String/* topic */, RateLimiter> rateLimiterTable;

    @Getter(AccessLevel.PACKAGE)
    private final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;

    @Getter(AccessLevel.NONE)
    private volatile ScheduledFuture<?> scanAssignmentsFuture;

    private long fifoConsumptionSuspendTimeMillis = 1000L;

    private int consumeMessageBatchMaxSize = 1;

    private long maxBatchConsumeWaitTimeMillis = 0;

    private int consumptionThreadMin = 20;

    private int consumptionThreadMax = 64;

    private int maxDeliveryAttempts = 17;

    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * Indicates the first consumption's position of consumer.
     *
     * <p>In cluster consumption model, it is UNDEFINED if consumer use different timestamp in the same group.
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.END;

    /**
     * Indicates first consumption's time point of consumer by timestamp. Note that setting this option does not take
     * effect while {@link ConsumeFromWhere} is not {@link ConsumeFromWhere#TIMESTAMP}.
     *
     * <p>In cluster consumption model, timestamp here indicates the position of all consumer's first consumption.
     * Which is UNDEFINED if consumers use different timestamp in same group.
     *
     * <p>In broadcasting consumption model, timestamp here are individual for each consumer, even though they belong
     * to the same group.
     */
    private long consumeFromTimeMillis = System.currentTimeMillis();

    private long consumptionTimeoutMillis = 15 * 60 * 1000L;

    private OffsetStore offsetStore = null;

    public DefaultMQPushConsumerImpl(String group) {
        super(group);
        this.filterExpressionTable = new ConcurrentHashMap<String, FilterExpression>();
        this.cachedTopicAssignmentTable = new ConcurrentHashMap<String, TopicAssignment>();

        this.messageListener = null;
        this.consumeService = null;

        this.rateLimiterTable = new ConcurrentHashMap<String, RateLimiter>();

        this.processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>();

        this.receiveTimes = new AtomicLong(0);
        this.receivedMessagesSize = new AtomicLong(0);

        this.pullTimes = new AtomicLong(0);
        this.pulledMessagesSize = new AtomicLong(0);

        this.consumptionOkCount = new AtomicLong(0);
        this.consumptionErrorCount = new AtomicLong(0);

        this.consumptionExecutor = new ThreadPoolExecutor(
                consumptionThreadMin,
                consumptionThreadMax,
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
            final ScheduledExecutorService scheduler = clientInstance.getScheduler();
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
    public void shutdown() {
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
                log.info("Shutdown the rocketmq push consumer successfully.");
            }
        }
    }

    List<ProcessQueue> processQueueList() {
        return new ArrayList<ProcessQueue>(processQueueTable.values());
    }

    ProcessQueue getProcessQueue(MessageQueue mq) {
        return processQueueTable.get(mq);
    }

    RateLimiter rateLimiter(String topic) {
        return rateLimiterTable.get(topic);
    }

    public void throttle(String topic, int permitsPerSecond) {
        final RateLimiter rateLimiter = RateLimiter.create(permitsPerSecond);
        rateLimiterTable.put(topic, rateLimiter);
    }

    private QueryAssignmentRequest wrapQueryAssignmentRequest(String topic) {
        Resource topicResource = Resource.newBuilder().setArn(arn).setName(topic).build();
        return QueryAssignmentRequest.newBuilder()
                                     .setTopic(topicResource)
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
                        // TODO: remote assignments should never be empty.
                        if (remote.getAssignmentList().isEmpty()) {
                            log.warn("Acquired empty assignments from remote, topic={}", topic);
                            if (null == local || local.getAssignmentList().isEmpty()) {
                                log.warn("No available assignments now, would scan later, topic={}", topic);
                                return;
                            }
                            log.warn("Acquired empty assignments from remote, reuse the existing one, topic={}", topic);
                            return;
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
        final long receiveTimes = this.receiveTimes.getAndSet(0);
        final long receivedMessagesSize = this.receivedMessagesSize.getAndSet(0);

        // for broadcasting consumption mode.
        final long pullTimes = this.pullTimes.getAndSet(0);
        final long pulledMessagesSize = this.pulledMessagesSize.getAndSet(0);

        final long consumptionOkCount = this.consumptionOkCount.getAndSet(0);
        final long consumptionErrorCount = this.consumptionErrorCount.getAndSet(0);

        log.info("ConsumerGroup={}, receiveTimes={}, receivedMessagesSize={}, pullTimes={}, pulledMessagesSize={}, "
                + "consumptionOkCount={}, consumptionErrorCount={}", group, receiveTimes, receivedMessagesSize,
                pullTimes, pulledMessagesSize, consumptionOkCount, consumptionErrorCount);
    }

    void dropProcessQueue(MessageQueue mq) {
        final ProcessQueue pq = processQueueTable.remove(mq);
        if (null != pq) {
            pq.drop();
        }
    }

    private ProcessQueue getProcessQueue(MessageQueue mq, final FilterExpression filterExpression) {
        if (null == processQueueTable.get(mq)) {
            processQueueTable.putIfAbsent(mq, new ProcessQueue(this, mq, filterExpression));
        }
        return processQueueTable.get(mq);
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

    public void subscribe(final String topic, final String subscribeExpression)
            throws ClientException {
        FilterExpression filterExpression = new FilterExpression(subscribeExpression);
        if (!filterExpression.verifyExpression()) {
            throw new ClientException("Subscribe expression is illegal");
        }
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
        final QueryAssignmentRequest request = wrapQueryAssignmentRequest(topic);
        final ListenableFuture<Endpoints> future = pickRouteEndpoints(topic);
        final ListenableFuture<QueryAssignmentResponse> responseFuture =
                Futures.transformAsync(future, new AsyncFunction<Endpoints, QueryAssignmentResponse>() {
                    @Override
                    public ListenableFuture<QueryAssignmentResponse> apply(Endpoints endpoints) throws Exception {
                        final Metadata metadata = sign();
                        return clientInstance.queryAssignment(endpoints, metadata, request, ioTimeoutMillis,
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
        Resource groupResource = Resource.newBuilder().setArn(arn).setName(group).build();

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

        DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.newBuilder()
                                                            .setMaxDeliveryAttempts(maxDeliveryAttempts)
                                                            .build();

        final ConsumerGroup.Builder builder = ConsumerGroup.newBuilder()
                                                           .setGroup(groupResource)
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
                             .build();
    }

    // TODO: polish code
    private Resource getGroupResource() {
        return Resource.newBuilder().setArn(arn).setName(group).build();
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
            messageImpl = ClientBaseImpl.wrapMessageImpl(message);
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
        Resource groupResource = Resource.newBuilder().setArn(arn).setName(group).build();
        final ClientResourceBundle.Builder builder =
                ClientResourceBundle.newBuilder().setClientId(clientId).setProducerGroup(groupResource);
        for (String topic : filterExpressionTable.keySet()) {
            Resource topicResource = Resource.newBuilder().setArn(arn).setName(topic).build();
            builder.addTopics(topicResource);
        }
        return builder.build();
    }

    long getIoTimeoutMillis() {
        return ioTimeoutMillis;
    }
}
