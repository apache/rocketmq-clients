package org.apache.rocketmq.client.impl.consumer;

import static com.google.common.base.Preconditions.checkNotNull;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.Resource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.util.Durations;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.filter.ExpressionType;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.remoting.RpcTarget;

@Slf4j
public class ProcessQueue {
    public static final long LONG_POLLING_TIMEOUT_MILLIS = MixAll.DEFAULT_LONG_POLLING_TIMEOUT_MILLIS;
    public static final long MAX_CACHED_MESSAGES_COUNT_PER_MESSAGE_QUEUE =
            MixAll.DEFAULT_MAX_CACHED_MESSAGES_COUNT_PER_MESSAGE_QUEUE;
    public static final long MAX_CACHED_MESSAGES_SIZE_PER_MESSAGE_QUEUE =
            MixAll.DEFAULT_MAX_CACHED_MESSAGES_SIZE_PER_MESSAGE_QUEUE;
    public static final long MAX_POP_MESSAGE_INTERVAL_MILLIS =
            MixAll.DEFAULT_MAX_POP_MESSAGE_INTERVAL_MILLIS;

    private static final long POP_TIME_DELAY_TIME_MILLIS_WHEN_FLOW_CONTROL = 3000L;

    @Setter
    @Getter
    private volatile boolean dropped;
    @Getter
    private final MessageQueue messageQueue;
    private final FilterExpression filterExpression;

    @Getter
    private final DefaultMQPushConsumerImpl consumerImpl;

    private final List<MessageExt> cachedMessages;
    private final ReentrantReadWriteLock cachedMessagesLock;
    private final AtomicLong cachedMsgSize;

    private volatile long lastPopTimestamp;
    private volatile long lastThrottledTimestamp;

    private final AtomicLong termId;

    public ProcessQueue(
            DefaultMQPushConsumerImpl consumerImpl,
            MessageQueue messageQueue,
            FilterExpression filterExpression) {
        this.consumerImpl = consumerImpl;
        this.messageQueue = messageQueue;
        this.filterExpression = filterExpression;
        this.dropped = false;

        this.cachedMessages = new ArrayList<MessageExt>();
        this.cachedMessagesLock = new ReentrantReadWriteLock();

        this.cachedMsgSize = new AtomicLong(0L);

        this.lastPopTimestamp = System.currentTimeMillis();
        this.lastThrottledTimestamp = System.currentTimeMillis();

        this.termId = new AtomicLong(1);
    }

    public boolean isPopExpired() {
        final long popDuration = System.currentTimeMillis() - lastPopTimestamp;
        if (popDuration < MAX_POP_MESSAGE_INTERVAL_MILLIS) {
            return false;
        }

        final long throttledDuration = System.currentTimeMillis() - lastThrottledTimestamp;
        if (throttledDuration < MAX_POP_MESSAGE_INTERVAL_MILLIS) {
            return false;
        }

        log.warn(
                "ProcessQueue is expired, duration from last pop={}ms, duration from last throttle={}ms, " +
                "lastPopTimestamp={}, lastThrottledTimestamp={}, currentTimestamp={}",
                popDuration,
                throttledDuration,
                lastPopTimestamp,
                lastThrottledTimestamp,
                System.currentTimeMillis());
        return true;
    }

    @VisibleForTesting
    public void cacheMessages(List<MessageExt> messageList) {
        cachedMessagesLock.writeLock().lock();
        try {
            for (MessageExt message : messageList) {
                cachedMessages.add(message);
                cachedMsgSize.addAndGet(null == message.getBody() ? 0 : message.getBody().length);
            }
        } finally {
            cachedMessagesLock.writeLock().unlock();
        }
    }

    public List<MessageExt> getCachedMessages() {
        cachedMessagesLock.readLock().lock();
        try {
            return new ArrayList<MessageExt>(cachedMessages);
        } finally {
            cachedMessagesLock.readLock().unlock();
        }
    }

    public void removeCachedMessages(List<MessageExt> messageExtList) {
        cachedMessagesLock.writeLock().lock();
        try {
            for (MessageExt messageExt : messageExtList) {
                final boolean removed = cachedMessages.remove(messageExt);
                if (removed) {
                    cachedMsgSize.addAndGet(null == messageExt.getBody() ? 0 : -messageExt.getBody().length);
                }
            }
        } finally {
            cachedMessagesLock.writeLock().unlock();
        }
    }

    private void handlePopResult(PopResult popResult) {
        final PopStatus popStatus = popResult.getPopStatus();
        final List<MessageExt> msgFoundList = popResult.getMsgFoundList();

        switch (popStatus) {
            case FOUND:
                cacheMessages(msgFoundList);

                consumerImpl.popTimes.getAndIncrement();
                consumerImpl.popMsgCount.getAndAdd(msgFoundList.size());

                try {
                    // TODO: considering whether exception would be thrown here?
                    consumerImpl.getConsumeService().dispatch(this);
                } catch (Throwable t) {
                    log.error("Unexpected error while dispatching message popped, mq={}", messageQueue.simpleName(), t);
                }
                // fall through on purpose.
            case NO_NEW_MSG:
            case POLLING_FULL:
            case POLLING_NOT_FOUND:
            case SERVICE_UNSTABLE:
                log.debug(
                        "Pop message from target={} with status={}, mq={}, message count={}",
                        popResult.getTarget(),
                        popStatus,
                        messageQueue.simpleName(),
                        msgFoundList.size());
                prepareNextPop();
                break;
            default:
                log.warn(
                        "Pop message from target={} with unknown status={}, mq={}, message count={}",
                        popResult.getTarget(),
                        popStatus,
                        messageQueue.simpleName(),
                        msgFoundList.size());
                prepareNextPop();
        }
    }

    @VisibleForTesting
    public void prepareNextPop() {
        if (this.isDropped()) {
            log.debug("Process queue has been dropped, mq={}.", messageQueue.simpleName());
            return;
        }
        if (this.isPopThrottled()) {
            log.warn(
                    "Process queue flow control is triggered, would pop message later, mq={}.",
                    messageQueue.simpleName());

            lastThrottledTimestamp = System.currentTimeMillis();

            popMessageLater();
            return;
        }
        popMessage();
    }

    public void popMessageLater() {
        final ScheduledExecutorService scheduler = consumerImpl.getClientInstance().getScheduler();
        try {
            scheduler.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            popMessage();
                        }
                    },
                    POP_TIME_DELAY_TIME_MILLIS_WHEN_FLOW_CONTROL,
                    TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            // Should never reach here.
            log.error("Unexpected error, failed to schedule pop message request", t);
            popMessageLater();
        }
    }

    private boolean isPopThrottled() {
        final long actualCachedMsgCount = this.getCachedMsgCount();
        if (MAX_CACHED_MESSAGES_COUNT_PER_MESSAGE_QUEUE <= actualCachedMsgCount) {
            log.warn(
                    "Process queue cached message count exceeds the threshold, max count={}, cached count={}, mq={}",
                    MAX_CACHED_MESSAGES_COUNT_PER_MESSAGE_QUEUE,
                    actualCachedMsgCount,
                    messageQueue.simpleName());
            return true;
        }
        final long actualCachedMsgSize = cachedMsgSize.get();
        if (MAX_CACHED_MESSAGES_SIZE_PER_MESSAGE_QUEUE <= actualCachedMsgSize) {
            log.warn(
                    "Process queue cached message size exceeds the threshold, max size={}, cached size={}, mq={}",
                    MAX_CACHED_MESSAGES_SIZE_PER_MESSAGE_QUEUE,
                    actualCachedMsgSize,
                    messageQueue.simpleName());
            return true;
        }
        return false;
    }

    public void ackMessage(MessageExt messageExt) throws MQClientException {
        final AckMessageRequest request = wrapAckMessageRequest(messageExt);
        final ClientInstance clientInstance = consumerImpl.getClientInstance();
        final RpcTarget target = messageExt.getAckRpcTarget();
        if (consumerImpl.getDefaultMQPushConsumer().isAckMessageAsync()) {
            clientInstance.ackMessageAsync(target, request);
            return;
        }
        clientInstance.ackMessage(target, request);
    }

    public void negativeAckMessage(MessageExt messageExt) throws MQClientException {
        final NackMessageRequest request = wrapNackMessageRequest(messageExt);
        final ClientInstance clientInstance = consumerImpl.getClientInstance();
        final RpcTarget target = messageExt.getAckRpcTarget();
        if (consumerImpl.getDefaultMQPushConsumer().isNackMessageAsync()) {
            clientInstance.nackMessageAsync(target, request);
            return;
        }
        clientInstance.nackMessage(target, request);
    }

    public void popMessage() {
        try {
            final ClientInstance clientInstance = consumerImpl.getClientInstance();
            final RpcTarget target = messageQueue.getPartition().getRpcTarget();
            final ReceiveMessageRequest request = wrapPopMessageRequest();

            lastPopTimestamp = System.currentTimeMillis();

            final ListenableFuture<PopResult> future =
                    clientInstance.receiveMessageAsync(
                            target, request, LONG_POLLING_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            Futures.addCallback(
                    future,
                    new FutureCallback<PopResult>() {
                        @Override
                        public void onSuccess(@Nullable PopResult popResult) {
                            try {
                                checkNotNull(popResult);
                                ProcessQueue.this.handlePopResult(popResult);
                            } catch (Throwable t) {
                                // Should never reach here.
                                log.error(
                                        "BUG!!! unexpected exception occurs while handle pop result, would pop "
                                        + "message later, mq={}", messageQueue, t);
                                popMessageLater();
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            log.error(
                                    "Exception occurs while popping message, would pop message later, mq={}",
                                    messageQueue, t);
                            popMessageLater();
                        }
                    },
                    MoreExecutors.directExecutor());
        } catch (Throwable t) {
            log.error(
                    "Exception raised while popping message, would pop message later, mq={}.",
                    messageQueue,
                    t);
            popMessageLater();
        }
    }

    private AckMessageRequest wrapAckMessageRequest(MessageExt messageExt) {
        // Group
        final Resource groupResource = Resource.newBuilder()
                                               .setArn(this.getArn())
                                               .setName(this.getGroup())
                                               .build();
        // Topic
        final Resource topicResource = Resource.newBuilder()
                                               .setArn(this.getArn())
                                               .setName(messageExt.getTopic())
                                               .build();

        final AckMessageRequest.Builder builder = AckMessageRequest.newBuilder()
                                                                   .setGroup(groupResource)
                                                                   .setTopic(topicResource)
                                                                   .setMessageId(messageExt.getMsgId())
                                                                   .setClientId(this.getClientId())
                                                                   .setReceiptHandle(messageExt.getReceiptHandle());

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

    private NackMessageRequest wrapNackMessageRequest(MessageExt messageExt) {
        // Group
        final Resource groupResource = Resource.newBuilder()
                                               .setArn(this.getArn())
                                               .setName(this.getGroup())
                                               .build();
        // Topic
        final Resource topicResource = Resource.newBuilder()
                                               .setArn(this.getArn())
                                               .setName(messageExt.getTopic())
                                               .build();

        final NackMessageRequest.Builder builder = NackMessageRequest.newBuilder()
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

    private ReceiveMessageRequest wrapPopMessageRequest() {
        final Resource groupResource =
                Resource.newBuilder()
                        .setArn(this.getArn())
                        .setName(this.getGroup())
                        .build();

        final Resource topicResource =
                Resource.newBuilder()
                        .setArn(this.getArn())
                        .setName(messageQueue.getTopic()).build();

        final Partition partition =
                Partition.newBuilder().setTopic(topicResource).setId(messageQueue.getQueueId()).build();

        final ReceiveMessageRequest.Builder builder =
                ReceiveMessageRequest.newBuilder()
                                     .setGroup(groupResource)
                                     .setClientId(this.getClientId())
                                     .setPartition(partition).setBatchSize(MixAll.DEFAULT_MAX_MESSAGE_NUMBER_PRE_BATCH)
                                     .setInvisibleDuration(Durations.fromMillis(MixAll.DEFAULT_INVISIBLE_TIME_MILLIS))
                                     .setAwaitTime(Durations.fromMillis(MixAll.DEFAULT_POLL_TIME_MILLIS));

        switch (this.getConsumeFromWhere()) {
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

        switch (this.getMessageModel()) {
            case CLUSTERING:
                builder.setConsumeModel(ConsumeModel.CLUSTERING);
                break;
            case BROADCASTING:
                builder.setConsumeModel(ConsumeModel.BROADCASTING);
                break;
            default:
                builder.setConsumeModel(ConsumeModel.UNRECOGNIZED);
        }

        final ExpressionType expressionType = filterExpression.getExpressionType();

        apache.rocketmq.v1.FilterExpression.Builder expressionBuilder =
                apache.rocketmq.v1.FilterExpression.newBuilder();
        expressionBuilder.setExpression(filterExpression.getExpression());

        switch (expressionType) {
            case SQL92:
                expressionBuilder.setType(FilterType.SQL);
                break;
            case TAG:
                expressionBuilder.setType(FilterType.TAG);
                break;
            default:
                log.error(
                        "Unknown filter expression type={}, expression string={}",
                        expressionType,
                        filterExpression.getExpression());
        }

        builder.setFilterExpression(expressionBuilder.build());
        return builder.build();
    }

    public long getCachedMsgCount() {
        cachedMessagesLock.readLock().lock();
        try {
            return cachedMessages.size();
        } finally {
            cachedMessagesLock.readLock().unlock();
        }
    }

    public long getCachedMsgSize() {
        return cachedMsgSize.get();
    }

    private String getArn() {
        return consumerImpl.getDefaultMQPushConsumer().getArn();
    }

    private String getGroup() {
        return consumerImpl.getDefaultMQPushConsumer().getConsumerGroup();
    }

    private String getClientId() {
        return consumerImpl.getDefaultMQPushConsumer().getClientId();
    }

    private int getMaxReconsumeTimes() {
        return consumerImpl.getDefaultMQPushConsumer().getMaxReconsumeTimes();
    }

    private MessageModel getMessageModel() {
        return consumerImpl.getDefaultMQPushConsumer().getMessageModel();
    }

    private ConsumeFromWhere getConsumeFromWhere() {
        return consumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
    }
}
