package org.apache.rocketmq.client.impl.consumer;

import static com.google.common.base.Preconditions.checkNotNull;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.ConsumeMessageType;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.ConsumerGroup;
import apache.rocketmq.v1.DeadLetterPolicy;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SubscriptionEntry;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.ClientBaseImpl;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.remoting.RpcTarget;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;


@Slf4j
public class DefaultMQPushConsumerImpl extends ClientBaseImpl {

    public AtomicLong popTimes;
    public AtomicLong poppedMsgCount;
    public AtomicLong consumeSuccessMsgCount;
    public AtomicLong consumeFailureMsgCount;

    private final ConcurrentMap<String /* topic */, FilterExpression> filterExpressionTable;
    private final ConcurrentMap<String /* topic */, TopicAssignment> cachedTopicAssignmentTable;

    private MessageListenerConcurrently messageListenerConcurrently;
    private MessageListenerOrderly messageListenerOrderly;

    private final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;
    private volatile ScheduledFuture<?> scanLoadAssignmentsFuture;

    @Setter
    @Getter
    private int consumeMessageBatchMaxSize = 1;

    @Setter
    @Getter
    private long maxBatchConsumeWaitTimeMillis = 1000;

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

    @Getter
    @Setter
    private int maxReconsumeTimes = 16;

    @Getter
    @Setter
    private MessageModel messageModel = MessageModel.CLUSTERING;

    @Getter
    @Setter
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    public DefaultMQPushConsumerImpl(String group) {
        super(group);
        this.filterExpressionTable = new ConcurrentHashMap<String, FilterExpression>();
        this.cachedTopicAssignmentTable = new ConcurrentHashMap<String, TopicAssignment>();

        this.messageListenerConcurrently = null;
        this.messageListenerOrderly = null;

        this.processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>();

        this.consumeService = null;

        this.popTimes = new AtomicLong(0);
        this.poppedMsgCount = new AtomicLong(0);
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
        throw new MQClientException(ErrorCode.NO_LISTENER_REGISTERED);
    }

    @Override
    public void start() throws MQClientException {
        synchronized (this) {
            log.info("Begin to start the rocketmq push consumer");
            super.start();

            consumeService = this.generateConsumeService();
            consumeService.start();
            final ScheduledExecutorService scheduler = clientInstance.getScheduler();
            scanLoadAssignmentsFuture = scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                scanAssignments();
                            } catch (Throwable t) {
                                log.error("Unexpected error while scanning the load assignments.", t);
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
            super.shutdown();

            if (ServiceState.STOPPED == getState()) {
                if (null != scanLoadAssignmentsFuture) {
                    scanLoadAssignmentsFuture.cancel(false);
                }
                super.shutdown();
                if (null != consumeService) {
                    consumeService.shutdown();
                }
                log.info("Shutdown the rocketmq push consumer successfully.");
            }
        }
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
                            log.info("Assignment of {} has changed, {} -> {}", topic, topicAssignment,
                                     remoteTopicAssignment);
                            syncProcessQueueByTopic(topic, remoteTopicAssignment, filterExpression);
                            cachedTopicAssignmentTable.put(topic, remoteTopicAssignment);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.error("Unexpected error occurs while scanning the assignments for topic={}", topic, t);
                    }
                });
            }
        } catch (Throwable t) {
            log.error("Exception occurs while scanning the assignments for all topics.", t);
        }
    }

    @Override
    public void logStats() {
        final long popTimes = this.popTimes.getAndSet(0);
        final long poppedMsgCount = this.poppedMsgCount.getAndSet(0);
        final long consumeSuccessMsgCount = this.consumeSuccessMsgCount.getAndSet(0);
        final long consumeFailureMsgCount = this.consumeFailureMsgCount.getAndSet(0);
        log.info(
                "ConsumerGroup={}, PopTimes={}, PoppedMsgCount={}, ConsumeSuccessMsgCount={}, "
                + "ConsumeFailureMsgCount={}", group,
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
                log.warn("BUG!!! processQueue is null unexpectedly, mq={}", messageQueue.simpleName());
                continue;
            }

            if (!latestMessageQueueSet.contains(messageQueue)) {
                log.info(
                        "Stop to pop message queue according to the latest load assignments, mq={}",
                        messageQueue.simpleName());
                processQueueTable.remove(messageQueue);
                processQueue.setDropped(true);
                continue;
            }

            if (processQueue.isPopExpired()) {
                log.warn("ProcessQueue is expired to pop, mq={}", messageQueue.simpleName());
                processQueueTable.remove(messageQueue);
                processQueue.setDropped(true);
                continue;
            }
            activeMessageQueueSet.add(messageQueue);
        }

        for (MessageQueue messageQueue : latestMessageQueueSet) {
            if (!activeMessageQueueSet.contains(messageQueue)) {
                log.info("Start to pop message queue according to the latest assignments, mq={}",
                         messageQueue.simpleName());
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

    public void registerMessageListener(MessageListenerConcurrently messageListenerConcurrently) {
        checkNotNull(messageListenerConcurrently);
        this.messageListenerConcurrently = messageListenerConcurrently;
    }

    public void registerMessageListener(MessageListenerOrderly messageListenerOrderly) {
        checkNotNull(messageListenerOrderly);
        this.messageListenerOrderly = messageListenerOrderly;
    }

    private ListenableFuture<RpcTarget> selectTargetForQuery(String topic) {
        final ListenableFuture<TopicRouteData> future = getRouteFor(topic);
        return Futures.transformAsync(future, new AsyncFunction<TopicRouteData, RpcTarget>() {
            @Override
            public ListenableFuture<RpcTarget> apply(TopicRouteData topicRouteData) throws Exception {
                final SettableFuture<RpcTarget> future0 = SettableFuture.create();
                final List<Partition> partitions = topicRouteData.getPartitions();
                for (int i = 0; i < partitions.size(); i++) {
                    final Partition partition =
                            partitions.get(TopicAssignment.getNextPartitionIndex() % partitions.size());
                    if (MixAll.MASTER_BROKER_ID != partition.getBrokerId()) {
                        continue;
                    }
                    if (Permission.NONE == partition.getPermission()) {
                        continue;
                    }
                    future0.set(partition.getTarget());
                    return future0;
                }
                throw new MQServerException(ErrorCode.NO_PERMISSION);
            }
        });
    }

    private ListenableFuture<TopicAssignment> queryAssignment(final String topic) {

        final QueryAssignmentRequest request = wrapQueryAssignmentRequest(topic);
        final ListenableFuture<RpcTarget> future = selectTargetForQuery(topic);
        final ListenableFuture<QueryAssignmentResponse> responseFuture =
                Futures.transformAsync(future, new AsyncFunction<RpcTarget, QueryAssignmentResponse>() {
                    @Override
                    public ListenableFuture<QueryAssignmentResponse> apply(RpcTarget target) throws Exception {
                        final Metadata metadata = sign();
                        return clientInstance.queryAssignment(target, metadata, request, ioTimeoutMillis,
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
                    throw new MQClientException(ErrorCode.NO_ASSIGNMENT);
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

    public void nackMessage(MessageExt messageExt) throws MQClientException {
        final NackMessageRequest request = wrapNackMessageRequest(messageExt);
        final RpcTarget target = messageExt.getAckTarget();
        final Metadata metadata = sign();
        final ListenableFuture<NackMessageResponse> future =
                clientInstance.nackMessage(target, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        final String messageId = request.getMessageId();
        final Endpoints endpoints = target.getEndpoints();
        Futures.addCallback(future, new FutureCallback<NackMessageResponse>() {
            @Override
            public void onSuccess(NackMessageResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to nack message, messageId={}, endpoints={}, code={}, message={}", messageId,
                              endpoints, code, status.getMessage());
                    return;
                }
                log.trace("Nack message successfully, messageId={}, endpoints={}, code={}, message={}", messageId,
                          endpoints, code, status.getMessage());
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Unexpected error while nack message, messageId={}, endpoints={}", messageId, endpoints, t);
            }
        });
    }

    private NackMessageRequest wrapNackMessageRequest(MessageExt messageExt) {
        // Group
        final Resource groupResource = Resource.newBuilder()
                                               .setArn(arn)
                                               .setName(group)
                                               .build();
        // Topic
        final Resource topicResource = Resource.newBuilder()
                                               .setArn(arn)
                                               .setName(messageExt.getTopic())
                                               .build();

        final NackMessageRequest.Builder builder =
                NackMessageRequest.newBuilder()
                                  .setGroup(groupResource)
                                  .setTopic(topicResource)
                                  .setClientId(this.getClientId())
                                  .setReceiptHandle(messageExt.getReceiptHandle())
                                  .setMessageId(messageExt.getMsgId())
                                  .setReconsumeTimes(messageExt.getReconsumeTimes() + 1)
                                  .setMaxReconsumeTimes(this.getMaxReconsumeTimes());

        switch (getMessageModel()) {
            case CLUSTERING:
                builder.setConsumeModel(ConsumeModel.CLUSTERING);
                break;
            case BROADCASTING:
                builder.setConsumeModel(ConsumeModel.BROADCASTING);
                break;
            default:
                builder.setConsumeModel(ConsumeModel.UNRECOGNIZED);
        }

        return builder.build();
    }

    private AckMessageRequest wrapAckMessageRequest(MessageExt messageExt) {
        // Group
        final Resource groupResource = Resource.newBuilder()
                                               .setArn(arn)
                                               .setName(this.getGroup())
                                               .build();
        // Topic
        final Resource topicResource = Resource.newBuilder()
                                               .setArn(arn)
                                               .setName(messageExt.getTopic())
                                               .build();

        final AckMessageRequest.Builder builder = AckMessageRequest.newBuilder()
                                                                   .setGroup(groupResource)
                                                                   .setTopic(topicResource)
                                                                   .setMessageId(messageExt.getMsgId())
                                                                   .setClientId(this.getClientId())
                                                                   .setReceiptHandle(messageExt.getReceiptHandle());

        return builder.build();
    }

    public void ackMessage(final MessageExt messageExt) throws MQClientException {
        final AckMessageRequest request = wrapAckMessageRequest(messageExt);
        final RpcTarget target = messageExt.getAckTarget();
        final Metadata metadata = sign();
        final ListenableFuture<AckMessageResponse> future =
                clientInstance.ackMessage(target, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        final String messageId = request.getMessageId();
        final Endpoints endpoints = target.getEndpoints();
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to ack message, messageId={}, endpoints={}, code={}, message={}", messageId,
                              endpoints, code, status.getMessage());
                    return;
                }
                log.trace("Ack message successfully, messageId={}, endpoints={}, code={}, message={}", messageId,
                          endpoints, code, status.getMessage());
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Unexpected error while ack message, messageId={}, endpoints={}", messageId, endpoints, t);
            }
        });
    }
}
