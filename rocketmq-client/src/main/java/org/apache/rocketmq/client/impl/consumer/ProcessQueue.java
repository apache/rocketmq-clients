package org.apache.rocketmq.client.impl.consumer;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
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
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.Duration;
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
import org.apache.rocketmq.client.consumer.PullMessageQuery;
import org.apache.rocketmq.client.consumer.PullMessageResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.ReceiveMessageResult;
import org.apache.rocketmq.client.consumer.ReceiveStatus;
import org.apache.rocketmq.client.consumer.filter.ExpressionType;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
import org.apache.rocketmq.client.impl.ClientBaseImpl;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.message.MessageAccessor;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;
import org.apache.rocketmq.client.remoting.Endpoints;

@Slf4j
public class ProcessQueue {
    private static final long RECEIVE_LONG_POLLING_TIMEOUT_MILLIS = 15 * 1000L;
    private static final long RECEIVE_AWAIT_TIME_MILLIS = 10 * 1000L;
    private static final int RECEIVE_MAX_BATCH_SIZE = 32;
    private static final long RECEIVE_LATER_DELAY_MILLIS = 3 * 1000L;

    private static final long PULL_LONG_POLLING_TIMEOUT_MILLIS = 15 * 1000L;
    private static final long PULL_AWAIT_TIME_MILLIS = 10 * 1000L;
    private static final int PULL_MAX_BATCH_SIZE = 32;
    private static final long PULL_LATER_DELAY_MILLIS = 3 * 1000L;

    private static final long TOTAL_MAX_MESSAGES_SIZE = 1024;
    private static final long TOTAL_MAX_MESSAGES_BODY_SIZE = 4 * 1024 * 1024;

    private static final long MAX_IDLE_MILLIS = 30 * 1000L;
    private static final long ACK_FIFO_MESSAGE_DELAY_MILLIS = 100L;
    private static final long REDIRECT_FIFO_MESSAGE_TO_DLQ_DELAY_MILLIS = 100L;

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
    private final AtomicBoolean fifoConsumptionOccupied;

    private volatile long activityNanoTime;
    private volatile long throttleNanoTime;

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
        this.fifoConsumptionOccupied = new AtomicBoolean(false);

        this.activityNanoTime = System.nanoTime();
        this.throttleNanoTime = System.nanoTime();
    }

    public boolean fifoConsumptionInbound() {
        return fifoConsumptionOccupied.compareAndSet(false, true);
    }

    public void fifoConsumptionOutbound() {
        fifoConsumptionOccupied.compareAndSet(true, false);
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

    /**
     * Erase message from in-flight message list, and re-calculate the total messages body size.
     *
     * @param messageExt message to erase.
     */
    private void eraseMessage(final MessageExt messageExt) {
        inflightMessagesLock.writeLock().lock();
        try {
            if (inflightMessages.remove(messageExt)) {
                messagesBodySize.addAndGet(null == messageExt.getBody() ? 0 : -messageExt.getBody().length);
            }
        } finally {
            inflightMessagesLock.writeLock().unlock();
        }
    }

    /**
     * Erase consumed fifo messages from {@link ProcessQueue#cachedMessages}, send message to DLQ if status is
     * {@link ConsumeStatus#ERROR}
     *
     * @param messageExt consumed fifo message
     * @param status     consume status, which indicated whether to send message to DLQ.
     */
    public void eraseFifoMessage(final MessageExt messageExt, ConsumeStatus status) {
        statsMessageConsumptionStatus(status);

        final boolean needReConsumption;
        ListenableFuture<?> future;
        final MessageModel messageModel = consumerImpl.getMessageModel();
        if (MessageModel.BROADCASTING.equals(messageModel)) {
            needReConsumption = false;

            SettableFuture<Void> future0 = SettableFuture.create();
            future0.set(null);

            future = future0;
        } else if (ConsumeStatus.OK == status) {
            needReConsumption = false;
            future = ackFifoMessage(messageExt);
        } else if (consumerImpl.getMaxDeliveryAttempts() >= messageExt.getDeliveryAttempt()) {
            needReConsumption = false;
            future = forwardToDeadLetterQueue(messageExt, 1);
        } else {
            // deliver attempts do not exceed the threshold.
            needReConsumption = true;

            final MessageImpl messageImpl = MessageAccessor.getMessageImpl(messageExt);
            final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();

            // increment the delivery attempt and prepare to deliver message once again.
            systemAttribute.setDeliveryAttempt(1 + systemAttribute.getDeliveryAttempt());

            // try to deliver message once again.
            final long fifoConsumptionSuspendTimeMillis = consumerImpl.getFifoConsumptionSuspendTimeMillis();
            future = consumerImpl.getConsumeService().consume(messageExt, fifoConsumptionSuspendTimeMillis,
                                                              TimeUnit.MILLISECONDS);
        }

        future.addListener(new Runnable() {
            @Override
            public void run() {
                eraseMessage(messageExt);
                if (!needReConsumption) {
                    fifoConsumptionOutbound();
                }
            }
        }, MoreExecutors.directExecutor());
    }

    /**
     * Erase message list from in-flight message list, and re-calculate the total messages body size.
     *
     * @param messageExtList message list to erase.
     */
    private void eraseMessages(final List<MessageExt> messageExtList) {
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
     * Erase consumed message froms {@link ProcessQueue#cachedMessages} and execute ack/nack.
     *
     * @param messageExtList consumed message list.
     * @param status         consume status, which indicates to ack/nack message.
     */
    public void eraseMessages(List<MessageExt> messageExtList, ConsumeStatus status) {
        statsMessageConsumptionStatus(messageExtList.size(), status);
        eraseMessages(messageExtList);
        final MessageModel messageModel = consumerImpl.getMessageModel();
        if (MessageModel.BROADCASTING.equals(messageModel)) {
            // TODO
            return;
        } else if (ConsumeStatus.OK == status) {
            for (MessageExt messageExt : messageExtList) {
                ackMessage(messageExt);
            }
        } else {
            for (MessageExt messageExt : messageExtList) {
                nackMessage(messageExt);
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
                    consumerImpl.receivedMessagesSize.getAndAdd(messagesFound.size());
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

    private void onPullMessageResult(PullMessageResult result) {
        final PullStatus pullStatus = result.getPullStatus();
        final List<MessageExt> messagesFound = result.getMessagesFound();

        switch (pullStatus) {
            case OK:
                if (messagesFound.isEmpty()) {
                    cacheMessages(messagesFound);
                    consumerImpl.pulledMessagesSize.getAndAdd(messagesFound.size());
                    consumerImpl.getConsumeService().dispatch();
                }
                pullMessage();
                break;
            // fall through on purpose.
            case DEADLINE_EXCEEDED:
            case RESOURCE_EXHAUSTED:
            case NOT_FOUND:
            case OUT_OF_RANGE:
            case INTERNAL:
            default:
                log.debug("Pull message with status={}, mq={}, messages found status={}", pullStatus, mq,
                          messagesFound.size());
                pullMessage();
        }
    }

    @VisibleForTesting
    public void receiveMessage() {
        if (this.isDropped()) {
            log.debug("Process queue has been dropped, no longer receive message, mq={}.", mq);
            return;
        }
        if (this.throttled()) {
            log.warn("Process queue is throttled, would receive message later, mq={}.", mq);
            throttleNanoTime = System.nanoTime();
            receiveMessageLater();
            return;
        }
        receiveMessageImmediately();
    }

    public void receiveMessageLater() {
        final ScheduledExecutorService scheduler = consumerImpl.getScheduler();
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
        final long idleMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - activityNanoTime);
        if (idleMillis < MAX_IDLE_MILLIS) {
            return false;
        }

        final long throttleIdleMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - throttleNanoTime);
        if (throttleIdleMillis < MAX_IDLE_MILLIS) {
            return false;
        }

        log.warn("Process queue is idle, reception idle time={}ms, throttle idle time={}ms", idleMillis,
                 throttleIdleMillis);
        return true;
    }

    public void pullMessageImmediately() {
        try {
            final Endpoints endpoints = mq.getPartition().getBroker().getEndpoints();
            PullMessageQuery pullMessageQuery = new PullMessageQuery(mq, filterExpression, PULL_MAX_BATCH_SIZE,
                                                                     PULL_MAX_BATCH_SIZE, PULL_AWAIT_TIME_MILLIS,
                                                                     PULL_LONG_POLLING_TIMEOUT_MILLIS);
            final ListenableFuture<PullMessageResult> future = consumerImpl.pull(pullMessageQuery);
            Futures.addCallback(future, new FutureCallback<PullMessageResult>() {
                @Override
                public void onSuccess(PullMessageResult pullMessageResult) {
                    try {
                        ProcessQueue.this.onPullMessageResult(pullMessageResult);
                    } catch (Throwable t) {
                        // should never reach here.
                        log.error("[Bug] Exception raised while handling pull result, would pull later, mq={}, "
                                  + "endpoints={}", mq, endpoints, t);
                        pullMessageLater();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Exception raised while pull message, would pull later, mq={}, endpoints={}", mq,
                              endpoints, t);
                    pullMessageLater();
                }
            });
            consumerImpl.pullTimes.getAndIncrement();
        } catch (Throwable t) {
            log.error("Exception raised while pull message, would pull message later, mq={}", mq, t);
            pullMessageLater();
        }
    }

    public void pullMessageLater() {
        final ScheduledExecutorService scheduler = consumerImpl.getScheduler();
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    pullMessage();
                }
            }, PULL_LATER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            // should never reach here.
            log.error("[Bug] Failed to schedule pull message request", t);
            pullMessageLater();
        }
    }

    public void pullMessage() {
        if (this.isDropped()) {
            log.debug("Process queue has been dropped, no longer pull message, mq={}.", mq);
            return;
        }
        if (this.throttled()) {
            log.warn("Process queue is throttled, would pull message later, mq={}", mq);
            throttleNanoTime = System.nanoTime();
            pullMessageLater();
            return;
        }
        pullMessageImmediately();
    }

    public void receiveMessageImmediately() {
        try {
            final ClientInstance clientInstance = consumerImpl.getClientInstance();
            final Endpoints endpoints = mq.getPartition().getBroker().getEndpoints();
            final ReceiveMessageRequest request = wrapReceiveMessageRequest();

            activityNanoTime = System.nanoTime();
            final Metadata metadata = consumerImpl.sign();

            final ListenableFuture<ReceiveMessageResponse> future0 = clientInstance.receiveMessage(
                    endpoints, metadata, request, RECEIVE_LONG_POLLING_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

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
                        log.error("[Bug] Exception raised while handling receive result, would receive later, mq={}, "
                                  + "endpoints={}", mq, endpoints, t);
                        receiveMessageLater();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Exception raised while message reception, would receive later, mq={}, endpoints={}",
                              mq, endpoints, t);
                    receiveMessageLater();
                }
            });
            consumerImpl.receiveTimes.getAndIncrement();
        } catch (Throwable t) {
            log.error("Exception raised while message reception, would receive later, mq={}.", mq, t);
            receiveMessageLater();
        }
    }

    private ListenableFuture<Void> ackFifoMessage(final MessageExt messageExt) {
        return ackFifoMessage(messageExt, 1);
    }

    private ListenableFuture<Void> ackFifoMessage(final MessageExt messageExt, final int attempt) {
        final SettableFuture<Void> future0 = SettableFuture.create();

        final ListenableFuture<AckMessageResponse> future = ackMessage(messageExt, attempt);
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                final Code code = Code.forNumber(response.getCommon().getStatus().getCode());
                if (Code.OK != code) {
                    ackFifoMessageLater(messageExt, 1 + attempt);
                    return;
                }
                future0.set(null);
            }

            @Override
            public void onFailure(Throwable t) {
                ackFifoMessageLater(messageExt, 1 + attempt);
            }
        });
        return future0;
    }

    private void ackFifoMessageLater(final MessageExt messageExt, final int attempt) {
        if (dropped) {
            log.info("Process queue was dropped, give up to ack message. mq={}, messageId={}", mq,
                     messageExt.getMsgId());
            return;
        }
        final ScheduledExecutorService scheduler = consumerImpl.getScheduler();
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    ackFifoMessage(messageExt, attempt);
                }
            }, ACK_FIFO_MESSAGE_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            // should never reach here.
            log.error("[Bug] Failed to schedule ack message request.");
            ackFifoMessageLater(messageExt, 1 + attempt);
        }
    }

    private ListenableFuture<Void> forwardToDeadLetterQueue(final MessageExt messageExt, final int attempt) {
        final SettableFuture<Void> future0 = SettableFuture.create();

        final ListenableFuture<ForwardMessageToDeadLetterQueueResponse> future = forwardToDeadLetterQueue(messageExt);
        final Endpoints endpoints = messageExt.getAckEndpoints();
        final String messageId = messageExt.getMsgId();
        Futures.addCallback(future, new FutureCallback<ForwardMessageToDeadLetterQueueResponse>() {
            @Override
            public void onSuccess(ForwardMessageToDeadLetterQueueResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to redirect message to DLQ, attempt={}, messageId={}, endpoints={}, code={}, "
                              + "status message={}", attempt, messageId, endpoints, code, status.getMessage());
                    redirectToDeadLetterQueueLater(messageExt, 1 + attempt);
                    return;
                }
                future0.set(null);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while message redirection to DLQ, attempt={}, messageId={}, endpoints={}",
                          attempt, messageId, endpoints, t);
                redirectToDeadLetterQueueLater(messageExt, 1 + attempt);
            }
        });
        return future0;
    }

    private void redirectToDeadLetterQueueLater(final MessageExt messageExt, final int attempt) {
        if (dropped) {
            log.info("Process queue was dropped, give up to redirect message to DLQ, mq={}, messageId={}", mq,
                     messageExt.getMsgId());
            return;
        }
        final ScheduledExecutorService scheduler = consumerImpl.getScheduler();
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    forwardToDeadLetterQueue(messageExt, attempt);
                }
            }, REDIRECT_FIFO_MESSAGE_TO_DLQ_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            // should never reach here.
            log.error("[Bug] Failed to schedule DLQ message request.");
            redirectToDeadLetterQueueLater(messageExt, 1 + attempt);
        }
    }

    private ListenableFuture<ForwardMessageToDeadLetterQueueResponse> forwardToDeadLetterQueue(
            final MessageExt messageExt) {
        ListenableFuture<ForwardMessageToDeadLetterQueueResponse> future;
        final Endpoints endpoints = messageExt.getAckEndpoints();
        try {
            final ForwardMessageToDeadLetterQueueRequest request =
                    wrapForwardMessageToDeadLetterQueueRequest(messageExt);
            final Metadata metadata = consumerImpl.sign();
            final ClientInstance clientInstance = consumerImpl.getClientInstance();
            final long ioTimeoutMillis = consumerImpl.getIoTimeoutMillis();
            future = clientInstance.forwardMessageToDeadLetterQueue(endpoints, metadata, request, ioTimeoutMillis,
                                                                 TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            final SettableFuture<ForwardMessageToDeadLetterQueueResponse> future0 = SettableFuture.create();
            future0.setException(t);
            future = future0;
        }
        return future;
    }

    public void ackMessage(MessageExt messageExt) {
        ackMessage(messageExt, 1);
    }

    public ListenableFuture<AckMessageResponse> ackMessage(final MessageExt messageExt, final int attempt) {
        ListenableFuture<AckMessageResponse> future;
        final String messageId = messageExt.getMsgId();
        final Endpoints endpoints = messageExt.getAckEndpoints();
        try {
            final AckMessageRequest request = wrapAckMessageRequest(messageExt);
            final Metadata metadata = consumerImpl.sign();
            final ClientInstance clientInstance = consumerImpl.getClientInstance();
            final long ioTimeoutMillis = consumerImpl.getIoTimeoutMillis();
            future = clientInstance.ackMessage(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            log.error("Failed to ACK, attempt={}, messageId={}, endpoints={}", attempt, messageId, endpoints, t);
            final SettableFuture<AckMessageResponse> future0 = SettableFuture.create();
            future0.setException(t);
            future = future0;
        }
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to ACK, attempt={}, messageId={}, endpoints={}, code={}, status message={}",
                              attempt, messageId, endpoints, code, status.getMessage());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while ACK, attempt={}, messageId={}, endpoints={}", attempt, messageId,
                          endpoints, t);
            }
        });
        return future;
    }

    public void nackMessage(final MessageExt messageExt) {
        final String messageId = messageExt.getMsgId();
        final Endpoints endpoints = messageExt.getAckEndpoints();
        ListenableFuture<NackMessageResponse> future;
        try {
            final NackMessageRequest request = wrapNackMessageRequest(messageExt);
            final Metadata metadata = consumerImpl.sign();
            final ClientInstance clientInstance = consumerImpl.getClientInstance();
            final long ioTimeoutMillis = consumerImpl.getIoTimeoutMillis();
            future = clientInstance.nackMessage(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            log.error("Failed to NACK, messageId={}, endpoints={}", messageId, endpoints, t);
            return;
        }
        Futures.addCallback(future, new FutureCallback<NackMessageResponse>() {
            @Override
            public void onSuccess(NackMessageResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to NACK, messageId={}, endpoints={}, code={}, status message={}", messageId,
                              endpoints, code, status.getMessage());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while NACK, messageId={}, endpoints={}", messageId, endpoints, t);
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

        final Duration duration = Durations.fromMillis(consumerImpl.getConsumptionTimeoutMillis());
        final ReceiveMessageRequest.Builder builder =
                ReceiveMessageRequest.newBuilder()
                                     .setGroup(groupResource)
                                     .setClientId(this.getClientId())
                                     .setPartition(partition).setBatchSize(RECEIVE_MAX_BATCH_SIZE)
                                     .setInvisibleDuration(duration)
                                     .setAwaitTime(Durations.fromMillis(RECEIVE_AWAIT_TIME_MILLIS));

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

    private ForwardMessageToDeadLetterQueueRequest wrapForwardMessageToDeadLetterQueueRequest(MessageExt messageExt) {
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

        return ForwardMessageToDeadLetterQueueRequest.newBuilder()
                                                  .setGroup(groupResource)
                                                  .setTopic(topicResource)
                                                  .setClientId(this.getClientId())
                                                  .setReceiptHandle(messageExt.getReceiptHandle())
                                                  .setMessageId(messageExt.getMsgId())
                                                  .setDeliveryAttempt(messageExt.getDeliveryAttempt())
                                                  .setMaxDeliveryAttempts(consumerImpl.getMaxDeliveryAttempts())
                                                  .build();
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

        return AckMessageRequest.newBuilder()
                                .setGroup(groupResource)
                                .setTopic(topicResource)
                                .setMessageId(messageExt.getMsgId())
                                .setClientId(this.getClientId())
                                .setReceiptHandle(messageExt.getReceiptHandle())
                                .build();
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
                                  .setDeliveryAttempt(messageExt.getDeliveryAttempt())
                                  .setMaxDeliveryAttempts(this.getMaxReconsumeTimes());

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


    private void statsMessageConsumptionStatus(ConsumeStatus status) {
        statsMessageConsumptionStatus(1, status);
    }

    private void statsMessageConsumptionStatus(int messageSize, ConsumeStatus status) {
        if (ConsumeStatus.OK.equals(status)) {
            consumerImpl.consumptionOkCount.addAndGet(messageSize);
            return;
        }
        consumerImpl.consumptionErrorCount.addAndGet(messageSize);
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
