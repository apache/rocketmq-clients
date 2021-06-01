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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ConsumeFromWhere;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.impl.ClientInstanceManager;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.remoting.RpcTarget;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;


@Slf4j
public class DefaultMQPushConsumerImpl implements ConsumerObserver {

    public AtomicLong popTimes;
    public AtomicLong popMsgCount;
    public AtomicLong consumeSuccessNum;
    public AtomicLong consumeFailureNum;

    @Getter
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    private final ConcurrentMap<String /* topic */, FilterExpression> filterExpressionTable;
    private final ConcurrentMap<String /* topic */, TopicAssignmentInfo> cachedTopicAssignmentTable;

    private MessageListenerConcurrently messageListenerConcurrently;
    private MessageListenerOrderly messageListenerOrderly;

    private final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;

    @Getter
    private ClientInstance clientInstance;
    @Getter
    private ConsumeService consumeService;
    private final AtomicReference<ServiceState> state;

    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer) {

        this.defaultMQPushConsumer = defaultMQPushConsumer;

        this.filterExpressionTable = new ConcurrentHashMap<String, FilterExpression>();
        this.cachedTopicAssignmentTable = new ConcurrentHashMap<String, TopicAssignmentInfo>();

        this.messageListenerConcurrently = null;
        this.messageListenerOrderly = null;

        this.processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>();

        this.consumeService = null;
        this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);

        this.popTimes = new AtomicLong(0);
        this.popMsgCount = new AtomicLong(0);
        this.consumeSuccessNum = new AtomicLong(0);
        this.consumeFailureNum = new AtomicLong(0);
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
        final String consumerGroup = defaultMQPushConsumer.getGroupName();

        if (!state.compareAndSet(ServiceState.CREATED, ServiceState.STARTING)) {
            throw new MQClientException(
                    "The producer has attempted to be started before, consumerGroup=" + consumerGroup);
        }

        consumeService = this.generateConsumeService();
        consumeService.start();

        clientInstance = ClientInstanceManager.getInstance().getClientInstance(defaultMQPushConsumer);
        final boolean registerResult = clientInstance.registerConsumerObserver(consumerGroup, this);
        if (!registerResult) {
            throw new MQClientException(
                    "The consumer group has been created already, please specify another one, consumerGroup="
                    + consumerGroup);
        }

        log.debug("Registered consumer observer, consumerGroup={}", consumerGroup);

        clientInstance.start();
        state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
    }

    public void shutdown() throws MQClientException {
        state.compareAndSet(ServiceState.STARTING, ServiceState.STOPPING);
        state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING);
        final ServiceState serviceState = state.get();
        if (ServiceState.STOPPING == serviceState) {
            if (null != clientInstance) {
                clientInstance.unregisterConsumerObserver(defaultMQPushConsumer.getConsumerGroup());
                clientInstance.shutdown();
            }

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
                                     .setConsumeModel(ConsumeModel.CLUSTERING).build();
    }

    @Override
    public void scanLoadAssignments() {
        try {
            final ServiceState serviceState = state.get();
            if (ServiceState.STARTED != serviceState && ServiceState.STARTING != serviceState) {
                log.warn(
                        "Unexpected consumer state while scanning load assignments, state={}", serviceState);
                return;
            }
            log.debug("Start to scan load assignments periodically");
            for (String topic : filterExpressionTable.keySet()) {
                try {
                    final FilterExpression filterExpression = filterExpressionTable.get(topic);

                    final TopicAssignmentInfo localTopicAssignmentInfo =
                            cachedTopicAssignmentTable.get(topic);
                    final TopicAssignmentInfo remoteTopicAssignmentInfo = queryLoadAssignment(topic);

                    // remoteTopicAssignmentInfo should never be null.
                    if (remoteTopicAssignmentInfo.getAssignmentList().isEmpty()) {
                        log.warn("Acquired empty assignment list from remote, topic={}", topic);
                        if (null == localTopicAssignmentInfo
                            || localTopicAssignmentInfo.getAssignmentList().isEmpty()) {
                            log.warn("No available assignments now, would scan later, topic={}", topic);
                            continue;
                        }
                        log.warn(
                                "Acquired empty assignment list from remote, reuse the existing one, topic={}",
                                topic);
                        continue;
                    }

                    if (!remoteTopicAssignmentInfo.equals(localTopicAssignmentInfo)) {
                        log.info(
                                "Load assignment of {} has changed, {} -> {}",
                                topic,
                                localTopicAssignmentInfo,
                                remoteTopicAssignmentInfo);

                        syncProcessQueueByTopic(topic, remoteTopicAssignmentInfo, filterExpression);
                        cachedTopicAssignmentTable.put(topic, remoteTopicAssignmentInfo);
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
        final long popNum = popMsgCount.getAndSet(0);
        final long consumeSuccessNum = this.consumeSuccessNum.getAndSet(0);
        final long consumeFailureNum = this.consumeFailureNum.getAndSet(0);
        log.info(
                "ConsumerGroup={}, popTimes={}, PopNum={}, SuccessNum={}, FailureNum={}",
                defaultMQPushConsumer.getConsumerGroup(),
                popTimes,
                popNum,
                consumeSuccessNum,
                consumeFailureNum);
    }

    private void syncProcessQueueByTopic(
            String topic, TopicAssignmentInfo topicAssignmentInfo, FilterExpression filterExpression) {
        Set<MessageQueue> newMessageQueueSet = new HashSet<MessageQueue>();

        final List<Assignment> assignmentList = topicAssignmentInfo.getAssignmentList();
        for (Assignment assignment : assignmentList) {
            newMessageQueueSet.add(assignment.getMessageQueue());
        }

        Set<MessageQueue> activeMessageQueueSet = new HashSet<MessageQueue>();

        for (MessageQueue messageQueue : processQueueTable.keySet()) {
            final ProcessQueue processQueue = processQueueTable.get(messageQueue);
            if (!topic.equals(messageQueue.getTopic())) {
                continue;
            }

            if (null == processQueue) {
                log.warn("BUG!!! processQueue is null unexpectedly, mq={}", messageQueue);
                continue;
            }

            if (!newMessageQueueSet.contains(messageQueue)) {
                log.info(
                        "Stop to pop message queue according to the latest load assignments, message queue={}",
                        messageQueue);
                processQueueTable.remove(messageQueue);
                processQueue.setDropped(true);
                continue;
            }

            if (processQueue.isPopExpired()) {
                log.warn("ProcessQueue is expired to pop, mq={}", messageQueue);
                processQueue.setDropped(true);
                continue;
            }
            activeMessageQueueSet.add(messageQueue);
        }

        for (MessageQueue messageQueue : newMessageQueueSet) {
            if (!activeMessageQueueSet.contains(messageQueue)) {
                log.info(
                        "Start to pop message queue according to the latest load assignments, mq={}",
                        messageQueue);
                popMessagePromptly(messageQueue, filterExpression);
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

    private void popMessagePromptly(MessageQueue messageQueue, FilterExpression filterExpression) {
        final ProcessQueue processQueue = getProcessQueue(messageQueue, filterExpression);
        processQueue.popMessage();
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
        if (partitions.isEmpty()) {
            throw new MQServerException("No partition available.");
        }
        final Partition partition = partitions.get(TopicAssignmentInfo.getNextPartitionIndex() % partitions.size());
        return partition.getRpcTarget();
    }

    private TopicAssignmentInfo queryLoadAssignment(String topic)
            throws MQClientException, MQServerException {
        final RpcTarget target = selectRpcTargetForQuery(topic);

        QueryAssignmentRequest request = wrapQueryAssignmentRequest(topic);

        return clientInstance.queryLoadAssignment(target, request);
    }

    @Override
    public HeartbeatEntry prepareHeartbeatData() {
        Resource groupResource =
                Resource.newBuilder()
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

        // TODO: fix max delivery attempts here.
        DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.newBuilder().setMaxDeliveryAttempts(0).build();

        final ConsumerGroup.Builder builder =
                ConsumerGroup.newBuilder()
                             .setGroup(groupResource)
                             .addAllSubscriptions(subscriptionEntries)
                             .setConsumeModel(ConsumeModel.CLUSTERING)
                             .setDeadLetterPolicy(deadLetterPolicy)
                             .setConsumeType(ConsumeMessageType.POP);

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
}
