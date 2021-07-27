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
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ConsumeFromWhere;
import org.apache.rocketmq.client.constant.Permission;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.ConsumeContext;
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
public class DefaultMQPushConsumerImpl extends ClientBaseImpl {

    public final AtomicLong receiveTimes;
    public final AtomicLong receivedCount;
    public final AtomicLong consumptionOkCount;
    public final AtomicLong consumptionErrorCount;

    private final ConcurrentMap<String /* topic */, FilterExpression> filterExpressionTable;
    private final ConcurrentMap<String /* topic */, TopicAssignment> cachedTopicAssignmentTable;

    private MessageListener messageListener;
    private final ConcurrentMap<String/* topic */, RateLimiter> rateLimiterTable;

    private final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;
    private volatile ScheduledFuture<?> scanAssignmentsFuture;

    @Setter
    @Getter
    private int consumeMessageBatchMaxSize = 1;

    @Setter
    @Getter
    private long maxBatchConsumeWaitTimeMillis = 0;

    @Getter
    @Setter
    private int consumeThreadMin = 20;

    @Getter
    @Setter
    private int consumeThreadMax = 64;

    @Getter
    @Setter
    // Only for order message.
    private long suspendCurrentQueueTimeMillis = 1000;

    @Getter
    private volatile ConsumeService consumeService;

    @Setter
    @Getter
    private int maxReconsumeTimes = 16;

    @Getter
    @Setter
    private MessageModel messageModel = MessageModel.CLUSTERING;

    @Getter
    @Setter
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    @Getter
    private final ThreadPoolExecutor consumeExecutor;

    public DefaultMQPushConsumerImpl(String group) {
        super(group);
        this.filterExpressionTable = new ConcurrentHashMap<String, FilterExpression>();
        this.cachedTopicAssignmentTable = new ConcurrentHashMap<String, TopicAssignment>();

        this.messageListener = null;
        this.consumeService = null;

        this.rateLimiterTable = new ConcurrentHashMap<String, RateLimiter>();

        this.processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>();

        this.receiveTimes = new AtomicLong(0);
        this.receivedCount = new AtomicLong(0);
        this.consumptionOkCount = new AtomicLong(0);
        this.consumptionErrorCount = new AtomicLong(0);

        this.consumeExecutor = new ThreadPoolExecutor(
                consumeThreadMin,
                consumeThreadMax,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("ConsumeThread"));
    }

    private void generateConsumeService() throws ClientException {
        switch (messageListener.getListenerType()) {
            case CONCURRENTLY:
                this.consumeService = new ConsumeConcurrentlyService(this, messageListener);
                break;
            case ORDERLY:
                this.consumeService = new ConsumeOrderlyService(this, messageListener);
                break;
            default:
                throw new ClientException(ErrorCode.NO_LISTENER_REGISTERED);
        }
    }

    @Override
    public void start() throws ClientException {
        synchronized (this) {
            log.info("Begin to start the rocketmq push consumer");
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
                consumeExecutor.shutdown();
                log.info("Shutdown the rocketmq push consumer successfully.");
            }
        }
    }

    List<ProcessQueue> processQueueList() {
        return new ArrayList<ProcessQueue>(processQueueTable.values());
    }

    int messagesCachedSize() {
        int size = 0;
        for (ProcessQueue pq : processQueueTable.values()) {
            size += pq.messagesCacheSize();
        }
        return size;
    }

    ProcessQueue processQueue(MessageQueue mq) {
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
                                     .setTopic(topicResource).setGroup(getGroupResource())
                                     .setClientId(clientId)
                                     .build();
    }

    public void scanAssignments() {
        try {
            log.debug("Start to scan assignments periodically");
            for (Map.Entry<String, FilterExpression> entry : filterExpressionTable.entrySet()) {
                final String topic = entry.getKey();
                final FilterExpression filterExpression = entry.getValue();

                final TopicAssignment topicAssignment = cachedTopicAssignmentTable.get(topic);

                final ListenableFuture<TopicAssignment> future = queryAssignment(topic);
                Futures.addCallback(future, new FutureCallback<TopicAssignment>() {
                    @Override
                    public void onSuccess(TopicAssignment remoteTopicAssignment) {
                        // remoteTopicAssignmentInfo should never be null.
                        if (remoteTopicAssignment.getAssignmentList().isEmpty()) {
                            log.warn("Acquired empty assignment list from remote, topic={}", topic);
                            if (null == topicAssignment || topicAssignment.getAssignmentList().isEmpty()) {
                                log.warn("No available assignments now, would scan later, topic={}", topic);
                                return;
                            }
                            log.warn("Acquired empty assignment list from remote, reuse the existing one, topic={}",
                                     topic);
                            return;
                        }

                        if (!remoteTopicAssignment.equals(topicAssignment)) {
                            log.info("Assignment of topic={} has changed, {} -> {}", topic, topicAssignment,
                                     remoteTopicAssignment);
                            syncProcessQueueByTopic(topic, remoteTopicAssignment, filterExpression);
                            cachedTopicAssignmentTable.put(topic, remoteTopicAssignment);
                        }
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
        final long receiveTimes = this.receiveTimes.getAndSet(0);
        final long receivedCount = this.receivedCount.getAndSet(0);
        final long consumptionOkCount = this.consumptionOkCount.getAndSet(0);
        final long consumptionErrorCount = this.consumptionErrorCount.getAndSet(0);
        log.info(
                "ConsumerGroup={}, receiveTimes={}, receivedCount={}, consumptionOkCount={}, "
                + "consumptionErrorCount={}", group, receiveTimes, receivedCount, consumptionOkCount,
                consumptionErrorCount);
    }

    private void syncProcessQueueByTopic(
            String topic, TopicAssignment topicAssignment, FilterExpression filterExpression) {
        Set<MessageQueue> latestMessageQueueSet = new HashSet<MessageQueue>();

        final List<Assignment> assignmentList = topicAssignment.getAssignmentList();
        for (Assignment assignment : assignmentList) {
            latestMessageQueueSet.add(assignment.getMessageQueue());
        }

        Set<MessageQueue> activeMessageQueueSet = new HashSet<MessageQueue>();

        for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
            final MessageQueue messageQueue = entry.getKey();
            final ProcessQueue processQueue = entry.getValue();
            if (!topic.equals(messageQueue.getTopic())) {
                continue;
            }

            if (null == processQueue) {
                log.error("[Bug] Process queue is null, mq={}", messageQueue);
                continue;
            }

            if (!latestMessageQueueSet.contains(messageQueue)) {
                log.info("Stop to receive message queue according to the latest assignments, mq={}", messageQueue);
                processQueueTable.remove(messageQueue);
                processQueue.setDropped(true);
                continue;
            }

            if (processQueue.idle()) {
                log.warn("Process queue is expired, mq={}", messageQueue);
                processQueueTable.remove(messageQueue);
                processQueue.setDropped(true);
                continue;
            }
            activeMessageQueueSet.add(messageQueue);
        }

        for (MessageQueue messageQueue : latestMessageQueueSet) {
            if (!activeMessageQueueSet.contains(messageQueue)) {
                log.info("Start to receive message from message queue according to the latest assignments, mq={}",
                         messageQueue);
                final ProcessQueue processQueue = processQueue(messageQueue, filterExpression);
                processQueue.receiveMessageImmediately();
            }
        }
    }

    private ProcessQueue processQueue(
            MessageQueue messageQueue, final FilterExpression filterExpression) {
        if (null == processQueueTable.get(messageQueue)) {
            processQueueTable.putIfAbsent(
                    messageQueue, new ProcessQueue(this, messageQueue, filterExpression));
        }
        return processQueueTable.get(messageQueue);
    }

    public void subscribe(final String topic, final String subscribeExpression)
            throws ClientException {
        FilterExpression filterExpression = new FilterExpression(subscribeExpression);
        if (!filterExpression.verifyExpression()) {
            throw new ClientException("SubscribeExpression is illegal");
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

        DeadLetterPolicy deadLetterPolicy =
                DeadLetterPolicy.newBuilder()
                                .setMaxDeliveryAttempts(maxReconsumeTimes)
                                .build();

        final ConsumerGroup.Builder builder =
                ConsumerGroup.newBuilder()
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
            case CONSUME_FROM_FIRST_OFFSET:
                builder.setConsumePolicy(ConsumePolicy.PLAYBACK);
                break;
            case CONSUME_FROM_TIMESTAMP:
                builder.setConsumePolicy(ConsumePolicy.TARGET_TIMESTAMP);
                break;
            case CONSUME_FROM_LAST_OFFSET:
            default:
                builder.setConsumePolicy(ConsumePolicy.RESUME);
        }
        final ConsumerGroup consumerGroup = builder.build();
        return HeartbeatEntry.newBuilder()
                             .setClientId(clientId)
                             .setConsumerGroup(consumerGroup)
                             .build();
    }

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
        try {
            consumeExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        final ArrayList<MessageExt> messageList = new ArrayList<MessageExt>();
                        messageList.add(messageExt);
                        final MessageQueue messageQueue = new MessageQueue(partition);
                        final ConsumeContext context = new ConsumeContext();
                        // Listener here may not registered.
                        final ConsumeStatus status = messageListener.consume(messageList, context);
                        future.set(status);
                    } catch (Throwable t) {
                        log.error("Exception raised while verification of message consumption, topic={}, "
                                  + "messageId={}", messageExt.getTopic(), messageExt.getMsgId());
                        future.setException(t);
                    }
                }
            });
        } catch (Throwable t) {
            log.error("Failed to submit task for verification of message consumption, topic={}, messageId={}",
                      messageExt.getTopic(), messageExt.getMsgId());
            future.setException(t);
        }
        return future;
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
