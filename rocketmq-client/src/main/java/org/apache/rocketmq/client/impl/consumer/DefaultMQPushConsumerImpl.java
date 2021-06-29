package org.apache.rocketmq.client.impl.consumer;

import apache.rocketmq.v1.ConsumeMessageType;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.ConsumerGroup;
import apache.rocketmq.v1.DeadLetterPolicy;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SubscriptionEntry;
import io.opentelemetry.api.trace.Tracer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ConsumeFromWhere;
import org.apache.rocketmq.client.constant.Permission;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.remoting.RpcTarget;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;


@Slf4j
public class DefaultMQPushConsumerImpl implements ConsumerObserver {

    public AtomicLong popTimes;
    public AtomicLong popMsgCount;
    public AtomicLong consumeSuccessMsgCount;
    public AtomicLong consumeFailureMsgCount;

    @Getter
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    private final ConcurrentMap<String /* topic */, FilterExpression> filterExpressionTable;
    private final ConcurrentMap<String /* topic */, TopicAssignment> cachedTopicAssignmentTable;

    private MessageListenerConcurrently messageListenerConcurrently;
    private MessageListenerOrderly messageListenerOrderly;

    private final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;

    @Getter
    private final ClientInstance clientInstance;
    @Getter
    private ConsumeService consumeService;
    private final AtomicReference<ServiceState> state;

    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer) {

        this.defaultMQPushConsumer = defaultMQPushConsumer;

        this.filterExpressionTable = new ConcurrentHashMap<String, FilterExpression>();
        this.cachedTopicAssignmentTable = new ConcurrentHashMap<String, TopicAssignment>();

        this.messageListenerConcurrently = null;
        this.messageListenerOrderly = null;

        this.processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>();

        this.consumeService = null;

        this.clientInstance = new ClientInstance(defaultMQPushConsumer);
        this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);

        this.popTimes = new AtomicLong(0);
        this.popMsgCount = new AtomicLong(0);
        this.consumeSuccessMsgCount = new AtomicLong(0);
        this.consumeFailureMsgCount = new AtomicLong(0);
    }

    private ConsumeService generateConsumeService() throws MQClientException {
        if (null != messageListenerConcurrently) {
            return new ConsumeConcurrentlyService(this, messageListenerConcurrently);
        }
        if (null != messageListenerOrderly) {
            return new ConsumeOrderlyService(this, messageListenerOrderly);
        }
        throw new MQClientException("No message listener registered.");
    }

    public void start() throws MQClientException {
        final String consumerGroup = defaultMQPushConsumer.getConsumerGroup();

        if (!state.compareAndSet(ServiceState.CREATED, ServiceState.STARTING)) {
            throw new MQClientException(
                    "The push consumer has attempted to be started before, consumerGroup=" + consumerGroup);
        }

        consumeService = this.generateConsumeService();
        consumeService.start();

        final boolean registerResult = clientInstance.registerConsumerObserver(consumerGroup, this);
        if (!registerResult) {
            throw new MQClientException(
                    "The consumer group has been created already, please specify another one, consumerGroup="
                    + consumerGroup);
        }

        log.debug("Registered consumer observer, consumerGroup={}", consumerGroup);

        clientInstance.start();

        clientInstance.getScheduler().scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            scanLoadAssignments();
                        } catch (Throwable t) {
                            log.error("Exception occurs while scanning load assignment of push consumer, "
                                      + "consumerGroup={}", defaultMQPushConsumer.getConsumerGroup(), t);
                        }
                    }
                }, 1000,
                5 * 1000,
                TimeUnit.MILLISECONDS);

        state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
        log.info("Start DefaultMQPushConsumerImpl successfully.");
    }

    public void shutdown() throws MQClientException {
        state.compareAndSet(ServiceState.STARTING, ServiceState.STOPPING);
        state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING);
        final ServiceState serviceState = state.get();
        if (ServiceState.STOPPING == serviceState) {
            clientInstance.unregisterConsumerObserver(defaultMQPushConsumer.getConsumerGroup());
            clientInstance.shutdown();

            if (null != consumeService) {
                consumeService.shutdown();
            }
            if (state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED)) {
                log.info("Shutdown DefaultMQPushConsumerImpl successfully.");
                return;
            }
        }
        throw new MQClientException("Failed to shutdown consumer, state=" + state.get());
    }

    private QueryAssignmentRequest wrapQueryAssignmentRequest(String topic) {
        Resource topicResource = Resource.newBuilder().setArn(this.getArn()).setName(topic).build();
        return QueryAssignmentRequest.newBuilder()
                                     .setTopic(topicResource).setGroup(getGroupResource())
                                     .setClientId(defaultMQPushConsumer.getClientId())
                                     .build();
    }

    public void scanLoadAssignments() {
        try {
            final ServiceState serviceState = state.get();
            if (ServiceState.STARTED != serviceState && ServiceState.STARTING != serviceState) {
                log.warn(
                        "Unexpected consumer state while scanning load assignments, state={}", serviceState);
                return;
            }
            log.debug("Start to scan load assignments periodically");
            for (Map.Entry<String, FilterExpression> entry : filterExpressionTable.entrySet()) {

                final String topic = entry.getKey();
                final FilterExpression filterExpression = entry.getValue();

                try {
                    final TopicAssignment localTopicAssignment =
                            cachedTopicAssignmentTable.get(topic);
                    final TopicAssignment remoteTopicAssignment = queryLoadAssignment(topic);

                    // remoteTopicAssignmentInfo should never be null.
                    if (remoteTopicAssignment.getAssignmentList().isEmpty()) {
                        log.warn("Acquired empty assignment list from remote, topic={}", topic);
                        if (null == localTopicAssignment
                            || localTopicAssignment.getAssignmentList().isEmpty()) {
                            log.warn("No available assignments now, would scan later, topic={}", topic);
                            continue;
                        }
                        log.warn(
                                "Acquired empty assignment list from remote, reuse the existing one, topic={}",
                                topic);
                        continue;
                    }

                    if (!remoteTopicAssignment.equals(localTopicAssignment)) {
                        log.info(
                                "Load assignment of {} has changed, {} -> {}",
                                topic,
                                localTopicAssignment,
                                remoteTopicAssignment);

                        syncProcessQueueByTopic(topic, remoteTopicAssignment, filterExpression);
                        cachedTopicAssignmentTable.put(topic, remoteTopicAssignment);
                    }
                } catch (Throwable t) {
                    log.error(
                            "Unexpected error occurs while scanning the load assignments for topic={}", topic, t);
                }
            }
        } catch (Throwable t) {
            log.error("Exception occurs while scanning the load assignments for all topics.", t);
        }
    }

    @Override
    public void logStats() {
        final long popTimes = this.popTimes.getAndSet(0);
        final long poppedMsgCount = popMsgCount.getAndSet(0);
        final long consumeSuccessMsgCount = this.consumeSuccessMsgCount.getAndSet(0);
        final long consumeFailureMsgCount = this.consumeFailureMsgCount.getAndSet(0);
        log.info(
                "ConsumerGroup={}, PopTimes={}, PoppedMsgCount={}, ConsumeSuccessMsgCount={}, "
                + "ConsumeFailureMsgCount={}", defaultMQPushConsumer.getConsumerGroup(),
                popTimes,
                poppedMsgCount,
                consumeSuccessMsgCount,
                consumeFailureMsgCount);
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
                log.warn("BUG!!! processQueue is null unexpectedly, mq={}", messageQueue);
                continue;
            }

            if (!latestMessageQueueSet.contains(messageQueue)) {
                log.info(
                        "Stop to pop message queue according to the latest load assignments, mq={}", messageQueue);
                processQueueTable.remove(messageQueue);
                processQueue.setDropped(true);
                continue;
            }

            if (processQueue.isPopExpired()) {
                log.warn("ProcessQueue is expired to pop, mq={}", messageQueue);
                processQueueTable.remove(messageQueue);
                processQueue.setDropped(true);
                continue;
            }
            activeMessageQueueSet.add(messageQueue);
        }

        for (MessageQueue messageQueue : latestMessageQueueSet) {
            if (!activeMessageQueueSet.contains(messageQueue)) {
                log.info(
                        "Start to pop message queue according to the latest load assignments, mq={}",
                        messageQueue);
                final ProcessQueue processQueue = getProcessQueue(messageQueue, filterExpression);
                processQueue.popMessage();
            }
        }
    }

    private ProcessQueue getProcessQueue(
            MessageQueue messageQueue, final FilterExpression filterExpression) {
        if (null == processQueueTable.get(messageQueue)) {
            processQueueTable.putIfAbsent(
                    messageQueue, new ProcessQueue(this, messageQueue, filterExpression));
        }
        return processQueueTable.get(messageQueue);
    }

    public void subscribe(final String topic, final String subscribeExpression)
            throws MQClientException {
        FilterExpression filterExpression = new FilterExpression(subscribeExpression);
        if (!filterExpression.verifyExpression()) {
            throw new MQClientException("SubscribeExpression is illegal");
        }
        filterExpressionTable.put(topic, filterExpression);
    }

    public void unsubscribe(final String topic) {
        filterExpressionTable.remove(topic);
    }

    public boolean hasBeenStarted() {
        final ServiceState serviceState = state.get();
        return ServiceState.CREATED != serviceState;
    }

    public void registerMessageListener(MessageListenerConcurrently messageListenerConcurrently) {
        this.messageListenerConcurrently = messageListenerConcurrently;
    }

    public void registerMessageListener(MessageListenerOrderly messageListenerOrderly) {
        this.messageListenerOrderly = messageListenerOrderly;
    }

    private RpcTarget selectRpcTargetForQuery(String topic) throws MQClientException, MQServerException {
        final TopicRouteData topicRouteData = clientInstance.getTopicRouteInfo(topic);

        final List<Partition> partitions = topicRouteData.getPartitions();
        for (int i = 0; i < partitions.size(); i++) {
            final Partition partition = partitions.get(TopicRouteData.getNextPartitionIndex() % partitions.size());
            if (MixAll.MASTER_BROKER_ID != partition.getBrokerId()) {
                continue;
            }
            if (Permission.NONE == partition.getPermission()) {
                continue;
            }
            return partition.getRpcTarget();
        }
        throw new MQServerException("No target available for query.");
    }

    private TopicAssignment queryLoadAssignment(String topic)
            throws MQClientException, MQServerException {
        final RpcTarget target = selectRpcTargetForQuery(topic);

        QueryAssignmentRequest request = wrapQueryAssignmentRequest(topic);

        return clientInstance.queryLoadAssignment(target, request);
    }

    @Override
    public HeartbeatEntry prepareHeartbeatData() {
        Resource groupResource = Resource.newBuilder()
                                         .setArn(this.getArn())
                                         .setName(defaultMQPushConsumer.getConsumerGroup()).build();

        List<SubscriptionEntry> subscriptionEntries = new ArrayList<SubscriptionEntry>();
        for (Map.Entry<String, FilterExpression> entry : filterExpressionTable.entrySet()) {
            final String topic = entry.getKey();
            final FilterExpression filterExpression = entry.getValue();

            Resource topicResource = Resource.newBuilder().setArn(this.getArn()).setName(topic).build();
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
                                .setMaxDeliveryAttempts(defaultMQPushConsumer.getMaxReconsumeTimes())
                                .build();

        final ConsumerGroup.Builder builder =
                ConsumerGroup.newBuilder()
                             .setGroup(groupResource)
                             .addAllSubscriptions(subscriptionEntries)
                             .setDeadLetterPolicy(deadLetterPolicy)
                             .setConsumeType(ConsumeMessageType.POP);

        switch (defaultMQPushConsumer.getMessageModel()) {
            case CLUSTERING:
                builder.setConsumeModel(ConsumeModel.CLUSTERING);
                break;
            case BROADCASTING:
                builder.setConsumeModel(ConsumeModel.BROADCASTING);
                break;
            default:
                builder.setConsumeModel(ConsumeModel.UNRECOGNIZED);
        }

        final ConsumeFromWhere consumeFromWhere = defaultMQPushConsumer.getConsumeFromWhere();
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
                             .setClientId(defaultMQPushConsumer.getClientId())
                             .setConsumerGroup(consumerGroup)
                             .build();
    }

    private String getProducerGroup() {
        return defaultMQPushConsumer.getConsumerGroup();
    }

    private String getArn() {
        return defaultMQPushConsumer.getArn();
    }

    private Resource getGroupResource() {
        return Resource.newBuilder().setArn(getArn()).setName(getProducerGroup()).build();
    }

    public Tracer getTracer() {
        return clientInstance.getTracer();
    }
}
