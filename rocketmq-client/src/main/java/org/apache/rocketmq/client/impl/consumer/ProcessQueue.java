package org.apache.rocketmq.client.impl.consumer;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.Resource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Metadata;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.ReceiveMessageResult;
import org.apache.rocketmq.client.consumer.ReceiveStatus;
import org.apache.rocketmq.client.consumer.filter.ExpressionType;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.impl.ClientBaseImpl;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.remoting.Endpoints;

@Slf4j
public class ProcessQueue {
    private static final long LONG_POLLING_TIMEOUT_MILLIS = 15 * 1000L;
    private static final long TOTAL_MAX_MESSAGES_SIZE = 1024;
    private static final long TOTAL_MAX_MESSAGES_BODY_SIZE = 4 * 1024 * 1024;
    private static final long MAX_IDLE_MILLIS = 30 * 1000L;
    private static final long RECEIVE_LATER_DELAY_MILLIS = 3 * 1000L;

    @Setter
    @Getter
    private volatile boolean dropped;
    @Getter
    private final MessageQueue mq;
    private final FilterExpression filterExpression;

    @Getter
    private final DefaultMQPushConsumerImpl consumerImpl;

    @GuardedBy("cachedMessagesLock")
    private final List<MessageExt> cachedMessages;
    private final ReentrantReadWriteLock cachedMessagesLock;

    @GuardedBy("inflightMessagesLock")
    private final List<MessageExt> inflightMessages;
    private final ReentrantReadWriteLock inflightMessagesLock;

    private final AtomicLong messagesBodySize;
    private final AtomicBoolean fifoConsumeTaskOccupied;

    private volatile long receptionTime;
    private volatile long throttledTime;

    public ProcessQueue(
            DefaultMQPushConsumerImpl consumerImpl,
            MessageQueue mq,
            FilterExpression filterExpression) {
        this.consumerImpl = consumerImpl;
        this.mq = mq;
        this.filterExpression = filterExpression;
        this.dropped = false;

        this.cachedMessages = new ArrayList<MessageExt>();
        this.cachedMessagesLock = new ReentrantReadWriteLock();

        this.inflightMessages = new ArrayList<MessageExt>();
        this.inflightMessagesLock = new ReentrantReadWriteLock();

        this.messagesBodySize = new AtomicLong(0L);
        this.fifoConsumeTaskOccupied = new AtomicBoolean(false);

        this.receptionTime = System.nanoTime();
        this.throttledTime = System.nanoTime();
    }

    public boolean fifoConsumeTaskInbound() {
        return fifoConsumeTaskOccupied.compareAndSet(false, true);
    }

    public boolean fifoConsumeTaskOutbound() {
        return fifoConsumeTaskOccupied.compareAndSet(true, false);
    }


    @VisibleForTesting
    public void cacheMessages(List<MessageExt> messageList) {
        cachedMessagesLock.writeLock().lock();
        try {
            for (MessageExt message : messageList) {
                cachedMessages.add(message);
                messagesBodySize.addAndGet(null == message.getBody() ? 0 : message.getBody().length);
            }
        } finally {
            cachedMessagesLock.writeLock().unlock();
        }
    }

    /**
     * Try to take one message from {@link ProcessQueue#cachedMessages}, and move it to
     * {@link ProcessQueue#inflightMessages}.
     *
     * @return message which has been taken, or null if no message exists.
     */
    public MessageExt tryTakeMessage() {
        final List<MessageExt> messageExtList = tryTakeMessages(1);
        if (messageExtList.isEmpty()) {
            return null;
        }
        return messageExtList.get(0);
    }

    /**
     * Try to take messages from {@link ProcessQueue#cachedMessages}, and move them to
     * {@link ProcessQueue#inflightMessages}.
     *
     * @param batchMaxSize max batch size to take messages.
     * @return messages which have been taken.
     */
    public List<MessageExt> tryTakeMessages(int batchMaxSize) {
        cachedMessagesLock.writeLock().lock();
        inflightMessagesLock.writeLock().lock();
        try {
            final int actualSize = Math.min(cachedMessages.size(), batchMaxSize);
            final List<MessageExt> subList = cachedMessages.subList(0, actualSize);
            final List<MessageExt> messageExtList = new ArrayList<MessageExt>(subList);

            inflightMessages.addAll(messageExtList);
            cachedMessages.removeAll(messageExtList);
            return messageExtList;
        } finally {
            inflightMessagesLock.writeLock().unlock();
            cachedMessagesLock.writeLock().unlock();
        }
    }

    private void eraseMessages(List<MessageExt> messageExtList) {
        inflightMessagesLock.writeLock().lock();
        try {
            for (MessageExt messageExt : messageExtList) {
                if (inflightMessages.remove(messageExt)) {
                    messagesBodySize.addAndGet(null == messageExt.getBody() ? 0 : -messageExt.getBody().length);
                }
            }
        } finally {
            inflightMessagesLock.writeLock().unlock();
        }
    }

    /**
     * Erase consumed fifo messages from {@link ProcessQueue#cachedMessages}, send message to DLQ if status is
     * {@link ConsumeStatus#ERROR}
     *
     * @param messageExtList consumed fifo message list
     * @param status         consume status, which indicated whether to send message to DLQ.
     */
    public void eraseFifoMessages(List<MessageExt> messageExtList, ConsumeStatus status) {
        eraseMessages(messageExtList);
        for (MessageExt messageExt : messageExtList) {

        }
    }

    /**
     * Erase consumed message froms {@link ProcessQueue#cachedMessages} and execute ack/nack.
     *
     * @param messageExtList consumed message list.
     * @param status         consume status, which indicates to ack/nack message.
     */
    public void eraseMessages(List<MessageExt> messageExtList, ConsumeStatus status) {
        eraseMessages(messageExtList);
        for (MessageExt message : messageExtList) {
            switch (status) {
                case OK:
                    consumerImpl.consumptionOkCount.incrementAndGet();
                    try {
                        if (MessageModel.CLUSTERING == consumerImpl.getMessageModel()) {
                            ackMessage(message);
                        }
                    } catch (Throwable t) {
                        log.warn("Failed to ACK message, mq={}, msgId={}", mq, message.getMsgId(), t);
                    }
                    break;
                case ERROR:
                default:
                    consumerImpl.consumptionErrorCount.incrementAndGet();
                    try {
                        if (MessageModel.CLUSTERING == consumerImpl.getMessageModel()) {
                            nackMessage(message);
                        }
                    } catch (Throwable t) {
                        log.warn("Failed to NACK message, mq={}, msgId={}", mq, message.getMsgId(), t);
                    }
            }
        }
    }

    private void onReceiveMessageResult(ReceiveMessageResult result) {
        final ReceiveStatus receiveStatus = result.getReceiveStatus();
        final List<MessageExt> messagesFound = result.getMessagesFound();
        final Endpoints endpoints = result.getEndpoints();

        switch (receiveStatus) {
            case OK:
                if (!messagesFound.isEmpty()) {
                    cacheMessages(messagesFound);
                    consumerImpl.receivedCount.getAndAdd(messagesFound.size());
                    consumerImpl.getConsumeService().dispatch();
                }
                receiveMessage();
                break;
            // fall through on purpose.
            case DEADLINE_EXCEEDED:
            case RESOURCE_EXHAUSTED:
            case NOT_FOUND:
            case DATA_CORRUPTED:
            case INTERNAL:
            default:
                log.debug("Receive message with status={}, mq={}, endpoints={}, messages found count={}", receiveStatus,
                          mq, endpoints, messagesFound.size());
                receiveMessage();
        }
    }

    @VisibleForTesting
    public void receiveMessage() {
        if (this.isDropped()) {
            log.debug("Process queue has been dropped, mq={}.", mq);
            return;
        }
        if (this.throttled()) {
            log.warn("Process queue is throttled, would receive message later, mq={}.", mq);
            throttledTime = System.nanoTime();
            receiveMessageLater();
            return;
        }
        receiveMessageImmediately();
    }

    public void receiveMessageLater() {
        final ScheduledExecutorService scheduler = consumerImpl.getClientInstance().getScheduler();
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    receiveMessage();
                }
            }, RECEIVE_LATER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            // should never reach here.
            log.error("[Bug] Failed to schedule receive message request", t);
            receiveMessageLater();
        }
    }

    private boolean throttled() {
        final long actualMessagesSize = this.messagesSize();
        if (TOTAL_MAX_MESSAGES_SIZE <= actualMessagesSize) {
            log.warn("Process queue total messages size exceeds the threshold, threshold={}, actual={}, mq={}",
                     TOTAL_MAX_MESSAGES_SIZE, actualMessagesSize, mq);
            return true;
        }
        final long actualMessagesBodySize = messagesBodySize.get();
        if (TOTAL_MAX_MESSAGES_BODY_SIZE <= actualMessagesBodySize) {
            log.warn("Process queue total messages body size exceeds the threshold, threshold={} bytes, "
                     + "actual={} bytes, mq={}", TOTAL_MAX_MESSAGES_BODY_SIZE, actualMessagesBodySize, mq);
            return true;
        }
        return false;
    }

    public boolean expired() {
        final long receptionIdleMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - receptionTime);
        if (receptionIdleMillis < MAX_IDLE_MILLIS) {
            return false;
        }

        final long throttleIdleMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - throttledTime);
        if (throttleIdleMillis < MAX_IDLE_MILLIS) {
            return false;
        }

        log.warn("Process queue is idle, reception idle time={}ms, throttle idle time={}ms", receptionIdleMillis,
                 throttleIdleMillis);
        return true;
    }

    public void receiveMessageImmediately() {
        try {
            final ClientInstance clientInstance = consumerImpl.getClientInstance();
            final Endpoints endpoints = mq.getPartition().getBroker().getEndpoints();
            final ReceiveMessageRequest request = wrapReceiveMessageRequest();

            receptionTime = System.nanoTime();
            final Metadata metadata = consumerImpl.sign();

            final ListenableFuture<ReceiveMessageResponse> future0 = clientInstance.receiveMessage(
                    endpoints, metadata, request, LONG_POLLING_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

            final ListenableFuture<ReceiveMessageResult> future = Futures.transform(
                    future0, new Function<ReceiveMessageResponse, ReceiveMessageResult>() {
                        @Override
                        public ReceiveMessageResult apply(ReceiveMessageResponse response) {
                            return processReceiveMessageResponse(endpoints, response);
                        }
                    });

            Futures.addCallback(future, new FutureCallback<ReceiveMessageResult>() {
                @Override
                public void onSuccess(ReceiveMessageResult receiveMessageResult) {
                    try {
                        ProcessQueue.this.onReceiveMessageResult(receiveMessageResult);
                    } catch (Throwable t) {
                        // should never reach here.
                        log.error("[Bug] Exception raised while handling receive result, would receive later, mq={}",
                                  mq, t);
                        receiveMessageLater();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Exception raised while message reception, would receive later, mq={}", mq, t);
                    receiveMessageLater();
                }
            });
            consumerImpl.receiveTimes.getAndIncrement();
        } catch (Throwable t) {
            log.error("Exception raised while message reception, would receive later, mq={}.", mq, t);
            receiveMessageLater();
        }
    }

    public void ackMessage(final MessageExt messageExt) throws ClientException {
        final AckMessageRequest request = wrapAckMessageRequest(messageExt);
        final Endpoints endpoints = messageExt.getAckEndpoints();
        final Metadata metadata = consumerImpl.sign();
        final ClientInstance clientInstance = consumerImpl.getClientInstance();
        final long ioTimeoutMillis = consumerImpl.getIoTimeoutMillis();
        final ListenableFuture<AckMessageResponse> future =
                clientInstance.ackMessage(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        final String messageId = request.getMessageId();
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
                log.error("Exception raised while ack message, messageId={}, endpoints={}", messageId, endpoints, t);
            }
        });
    }

    public void nackMessage(MessageExt messageExt) throws ClientException {
        final NackMessageRequest request = wrapNackMessageRequest(messageExt);
        final Endpoints endpoints = messageExt.getAckEndpoints();
        final Metadata metadata = consumerImpl.sign();
        final ClientInstance clientInstance = consumerImpl.getClientInstance();
        final long ioTimeoutMillis = consumerImpl.getIoTimeoutMillis();
        final ListenableFuture<NackMessageResponse> future =
                clientInstance.nackMessage(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        final String messageId = request.getMessageId();
        Futures.addCallback(future, new FutureCallback<NackMessageResponse>() {
            @Override
            public void onSuccess(NackMessageResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to nack message, messageId={}, endpoints={}, code={}, status message={}",
                              messageId, endpoints, code, status.getMessage());
                    return;
                }
                log.trace("Nack message successfully, messageId={}, endpoints={}, code={}, status message={}",
                          messageId, endpoints, code, status.getMessage());
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while nack message, messageId={}, endpoints={}", messageId, endpoints, t);
            }
        });
    }


    private ReceiveMessageRequest wrapReceiveMessageRequest() {
        final Resource groupResource =
                Resource.newBuilder()
                        .setArn(this.getArn())
                        .setName(this.getGroup())
                        .build();

        final Resource topicResource =
                Resource.newBuilder()
                        .setArn(this.getArn())
                        .setName(mq.getTopic()).build();

        final Broker broker = Broker.newBuilder().setName(mq.getBrokerName()).build();

        final Partition partition = Partition.newBuilder()
                                             .setTopic(topicResource)
                                             .setId(mq.getQueueId())
                                             .setBroker(broker).build();

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

    // TODO: handle the case that the topic does not exist.
    public ReceiveMessageResult processReceiveMessageResponse(Endpoints endpoints,
                                                              ReceiveMessageResponse response) {
        ReceiveStatus receiveStatus;

        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        switch (null != code ? code : Code.UNKNOWN) {
            case OK:
                receiveStatus = ReceiveStatus.OK;
                break;
            case RESOURCE_EXHAUSTED:
                receiveStatus = ReceiveStatus.RESOURCE_EXHAUSTED;
                log.warn("Too many request in server, server endpoints={}, status message={}", endpoints,
                         status.getMessage());
                break;
            case DEADLINE_EXCEEDED:
                receiveStatus = ReceiveStatus.DEADLINE_EXCEEDED;
                log.warn("Gateway timeout, server endpoints={}, status message={}", endpoints, status.getMessage());
                break;
            default:
                receiveStatus = ReceiveStatus.INTERNAL;
                log.warn("Receive response indicated server-side error, server endpoints={}, code={}, status "
                         + "message={}", endpoints, code, status.getMessage());
        }

        List<MessageExt> msgFoundList = new ArrayList<MessageExt>();
        if (ReceiveStatus.OK == receiveStatus) {
            final List<Message> messageList = response.getMessagesList();
            for (Message message : messageList) {
                MessageImpl messageImpl;
                try {
                    messageImpl = ClientBaseImpl.wrapMessageImpl(message);
                } catch (Throwable t) {
                    // TODO: need nack immediately.
                    continue;
                }
                messageImpl.getSystemAttribute().setAckEndpoints(endpoints);
                msgFoundList.add(new MessageExt(messageImpl));
            }
        }

        return new ReceiveMessageResult(endpoints, receiveStatus, Timestamps.toMillis(response.getDeliveryTimestamp()),
                                        Durations.toMillis(response.getInvisibleDuration()), msgFoundList);
    }

    public int messagesSize() {
        cachedMessagesLock.readLock().lock();
        inflightMessagesLock.readLock().lock();
        try {
            return cachedMessages.size() + inflightMessages.size();
        } finally {
            inflightMessagesLock.readLock().unlock();
            cachedMessagesLock.readLock().unlock();
        }
    }

    public int messagesCacheSize() {
        cachedMessagesLock.readLock().lock();
        try {
            return cachedMessages.size();
        } finally {
            cachedMessagesLock.readLock().unlock();
        }
    }

    public long getMessagesBodySize() {
        return messagesBodySize.get();
    }

    private String getArn() {
        return consumerImpl.getArn();
    }

    private String getGroup() {
        return consumerImpl.getGroup();
    }

    private String getClientId() {
        return consumerImpl.getClientId();
    }

    private int getMaxReconsumeTimes() {
        return consumerImpl.getMaxReconsumeTimes();
    }

    private MessageModel getMessageModel() {
        return consumerImpl.getMessageModel();
    }

    private ConsumeFromWhere getConsumeFromWhere() {
        return consumerImpl.getConsumeFromWhere();
    }
}
