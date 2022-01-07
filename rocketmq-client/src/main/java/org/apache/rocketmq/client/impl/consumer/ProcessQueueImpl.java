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

import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.Resource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.client.consumer.ConsumeStatus;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.consumer.PullMessageResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.ReceiveMessageResult;
import org.apache.rocketmq.client.consumer.ReceiveStatus;
import org.apache.rocketmq.client.consumer.filter.ExpressionType;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.MessageListenerType;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageHookPointStatus;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageImplAccessor;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.utility.SimpleFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The specified implementation of {@link ProcessQueue}
 *
 * <ul>
 *   <li>While message was fetched from remote, it would be cached in {@link #pendingMessages} firstly. if message was
 *       taken, it would be moved from {@link #pendingMessages} to {@link #inflightMessages} immediately.
 *   <li>While message was consumed, it would be removed from {@link #inflightMessages}, which means message is removed
 *       from cache completely.
 *   <li>If message is not erased/consumed in time, which may cause large memory footprint, {@link #isCacheFull()}
 *       here could help to identify this case, <i>once this happens, the process of fetching message would be
 *       suspended until this situation is eased.</i>
 * </ul>
 */
@SuppressWarnings(value = {"UnstableApiUsage", "NullableProblems"})
public class ProcessQueueImpl implements ProcessQueue {
    public static final long RECEIVE_LONG_POLLING_TIMEOUT_MILLIS = 30 * 1000L;
    public static final long RECEIVE_LATER_DELAY_MILLIS = 1000L;

    public static final long PULL_LONG_POLLING_TIMEOUT_MILLIS = 30 * 1000L;
    public static final long PULL_LATER_DELAY_MILLIS = 1000L;

    public static final long MAX_IDLE_MILLIS = 2 * Math.max(RECEIVE_LONG_POLLING_TIMEOUT_MILLIS,
                                                            PULL_LONG_POLLING_TIMEOUT_MILLIS);

    public static final long ACK_FIFO_MESSAGE_DELAY_MILLIS = 100L;
    public static final long FORWARD_FIFO_MESSAGE_TO_DLQ_DELAY_MILLIS = 100L;

    private static final Logger log = LoggerFactory.getLogger(ProcessQueueImpl.class);

    /**
     * Dropped means {@link ProcessQueue} is deprecated, which means no message would be fetched from remote anymore.
     */
    private volatile boolean dropped;

    private final MessageQueue mq;

    /**
     * Expression to filter message.
     */
    private final FilterExpression filterExpression;

    private final PushConsumerImpl consumerImpl;

    /**
     * Messages which is pending means have been cached, but are not taken by consumer dispatcher yet.
     */
    @GuardedBy("pendingMessagesLock")
    private final List<MessageExt> pendingMessages;
    private final ReadWriteLock pendingMessagesLock;

    /**
     * Message which is in-flight means have been dispatched, but the consumption process is not accomplished.
     */
    @GuardedBy("inflightMessagesLock")
    private final List<MessageExt> inflightMessages;
    private final ReadWriteLock inflightMessagesLock;

    /**
     * Only make sense in {@link ConsumeModel#CLUSTERING} and {@link OffsetStore} is customized.
     */
    private final NextOffsetRecord nextOffsetRecord;

    /**
     * Total memory bytes of cached messages, include all message(s) in {@link #pendingMessages} and
     * {@link #inflightMessages}.
     */
    private final AtomicLong cachedMessagesBytes;
    private final AtomicBoolean fifoConsumptionOccupied;

    private volatile long activityNanoTime = System.nanoTime();
    private volatile long cacheFullNanoTime = Long.MIN_VALUE;

    public ProcessQueueImpl(PushConsumerImpl consumerImpl, MessageQueue mq, FilterExpression filterExpression) {
        this.consumerImpl = consumerImpl;

        this.mq = mq;
        this.filterExpression = filterExpression;

        this.dropped = false;

        this.pendingMessages = new ArrayList<MessageExt>();
        this.pendingMessagesLock = new ReentrantReadWriteLock();

        this.inflightMessages = new ArrayList<MessageExt>();
        this.inflightMessagesLock = new ReentrantReadWriteLock();

        this.cachedMessagesBytes = new AtomicLong(0L);
        this.fifoConsumptionOccupied = new AtomicBoolean(false);

        this.nextOffsetRecord = new NextOffsetRecord();
    }

    @Override
    public void drop() {
        this.dropped = true;
    }

    private boolean fifoConsumptionInbound() {
        return fifoConsumptionOccupied.compareAndSet(false, true);
    }

    private void fifoConsumptionOutbound() {
        fifoConsumptionOccupied.compareAndSet(true, false);
    }

    @VisibleForTesting
    public void cacheMessages(List<MessageExt> messageExtList) {
        List<Long> offsetList = new ArrayList<Long>();
        final MessageListenerType listenerType = consumerImpl.getMessageListener().getListenerType();
        final MessageModel messageModel = consumerImpl.getMessageModel();
        final String namespace = consumerImpl.getNamespace();
        pendingMessagesLock.writeLock().lock();
        try {
            for (MessageExt messageExt : messageExtList) {
                if (MessageImplAccessor.getMessageImpl(messageExt).isCorrupted()) {
                    // ignore message in broadcasting mode.
                    if (MessageModel.BROADCASTING.equals(messageModel)) {
                        log.error("Message is corrupted, ignore it in broadcasting mode, namespace={}, mq={}, "
                                  + "messageId={}, clientId={}", namespace, mq, messageExt.getMsgId(),
                                  consumerImpl.getId());
                        continue;
                    }
                    // nack message for concurrent consumption.
                    if (MessageListenerType.CONCURRENTLY.equals(listenerType)) {
                        log.error("Message is corrupted, nack it for concurrently consumption, namespace={}, mq={}, "
                                  + "messageId={}, clientId={}", namespace, mq, messageExt.getMsgId(),
                                  consumerImpl.getId());
                        consumerImpl.nackMessage(messageExt);
                    }
                    // forward to DLQ for fifo consumption.
                    if (MessageListenerType.ORDERLY.equals(listenerType)) {
                        log.error("Message is corrupted, forward it to DLQ for fifo consumption, namespace={}, mq={}, "
                                  + "messageId={}, clientId={}", namespace, mq, messageExt.getMsgId(),
                                  consumerImpl.getId());
                        forwardToDeadLetterQueue(messageExt);
                    }
                    continue;
                }
                pendingMessages.add(messageExt);
                cachedMessagesBytes.addAndGet(messageExt.getBody().length);
                offsetList.add(messageExt.getQueueOffset());
            }
        } finally {
            pendingMessagesLock.writeLock().unlock();
        }

        if (!consumerImpl.isOffsetRecorded()) {
            return;
        }
        nextOffsetRecord.add(offsetList);
    }

    @Override
    public List<MessageExt> tryTakeMessages(int batchMaxSize) {
        pendingMessagesLock.writeLock().lock();
        inflightMessagesLock.writeLock().lock();
        try {
            List<MessageExt> messageExtList = new ArrayList<MessageExt>();
            final String topic = mq.getTopic();
            final RateLimiter rateLimiter = consumerImpl.rateLimiter(topic);
            // no rate limiter for current topic.
            if (null == rateLimiter) {
                final int actualSize = Math.min(pendingMessages.size(), batchMaxSize);
                final List<MessageExt> subList = new ArrayList<MessageExt>(pendingMessages.subList(0, actualSize));
                messageExtList.addAll(subList);

                inflightMessages.addAll(subList);
                pendingMessages.removeAll(subList);
                return messageExtList;
            }
            // has rate limiter for current topic.
            while (pendingMessages.size() > 0 && messageExtList.size() < batchMaxSize && rateLimiter.tryAcquire()) {
                final MessageExt messageExt = pendingMessages.iterator().next();
                messageExtList.add(messageExt);

                inflightMessages.add(messageExt);
                pendingMessages.remove(messageExt);
            }
            return messageExtList;
        } finally {
            inflightMessagesLock.writeLock().unlock();
            pendingMessagesLock.writeLock().unlock();
        }
    }

    /**
     * Erase message from in-flight message list, and re-calculate the total messages body size.
     *
     * @param messageExt message to erase.
     */
    private void eraseMessage(final MessageExt messageExt) {
        final List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(messageExt);
        eraseMessages(messageExtList);
    }

    /**
     * Try to take fifo message from {@link ProcessQueueImpl#pendingMessages}, and move them
     * to {@link ProcessQueueImpl#inflightMessages}. each fifo message taken from MUST be erased by
     * {@link ProcessQueueImpl#eraseFifoMessage(MessageExt, ConsumeStatus)}
     *
     * @return message which has been taken, or {@link Optional#absent()} if no message.
     */
    @Override
    public Optional<MessageExt> tryTakeFifoMessage() {
        pendingMessagesLock.writeLock().lock();
        inflightMessagesLock.writeLock().lock();
        try {
            // no new message arrived.
            if (pendingMessages.isEmpty()) {
                return Optional.absent();
            }
            // failed to lock.
            if (!fifoConsumptionInbound()) {
                log.debug("Fifo consumption task are not finished, namespace={}, mq={}, clientId={}",
                          consumerImpl.getNamespace(), mq, consumerImpl.getId());
                return Optional.absent();
            }
            final String topic = mq.getTopic();
            final RateLimiter rateLimiter = consumerImpl.rateLimiter(topic);
            // no rate limiter for current topic.
            if (null == rateLimiter) {
                final MessageExt first = pendingMessages.iterator().next();
                pendingMessages.remove(first);
                inflightMessages.add(first);
                return Optional.of(first);
            }
            // unlock because of the failure of acquire the token,
            if (!rateLimiter.tryAcquire()) {
                fifoConsumptionOutbound();
                return Optional.absent();
            }
            final MessageExt first = pendingMessages.iterator().next();
            pendingMessages.remove(first);
            inflightMessages.add(first);
            return Optional.of(first);
        } finally {
            inflightMessagesLock.writeLock().unlock();
            pendingMessagesLock.writeLock().unlock();
        }
    }

    @Override
    public void doStats() {
        pendingMessagesLock.readLock().lock();
        inflightMessagesLock.readLock().lock();
        try {
            log.info("clientId={}, namespace={}, mq={}, pendingMessageQuantity={}, inflightMessageQuantity={}, "
                     + "cachedMessagesBytes={}", consumerImpl.getId(), consumerImpl.getNamespace(), mq,
                     pendingMessages.size(), inflightMessages.size(), cachedMessagesBytes.get());
        } finally {
            inflightMessagesLock.readLock().unlock();
            pendingMessagesLock.readLock().unlock();
        }
    }

    @Override
    public void eraseFifoMessage(final MessageExt messageExt, final ConsumeStatus status) {
        statsConsumptionStatus(status);

        final MessageModel messageModel = consumerImpl.getMessageModel();
        if (MessageModel.BROADCASTING.equals(messageModel)) {
            // for broadcasting mode, no need to ack fifo message or forward it to DLQ.
            eraseMessage(messageExt);
            fifoConsumptionOutbound();
            return;
        }
        final int maxAttempts = consumerImpl.getMaxDeliveryAttempts();
        final int attempt = messageExt.getDeliveryAttempt();
        final ConsumeService consumeService = consumerImpl.getConsumeService();
        final String namespace = consumerImpl.getNamespace();
        // failed to consume fifo message but deliver attempt are not exhausted.
        // need to redeliver the fifo message.
        if (ConsumeStatus.ERROR.equals(status) && attempt < maxAttempts) {
            final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(messageExt);
            final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
            // increment the delivery attempt and prepare to deliver message once again.
            systemAttribute.setDeliveryAttempt(1 + attempt);
            // try to deliver message once again.
            final long fifoConsumptionSuspendTimeMillis = consumerImpl.getFifoConsumptionSuspendTimeMillis();
            log.debug("Prepare to redeliver the fifo message because of consumption failure, maxAttempt={}, "
                      + "attempt={}, namespace={}, mq={}, messageId={}, suspendTime={}ms, clientId={}", maxAttempts,
                      messageExt.getDeliveryAttempt(), namespace, mq, messageExt.getMsgId(),
                      fifoConsumptionSuspendTimeMillis, consumerImpl.getId());
            ListenableFuture<ConsumeStatus> future = consumeService
                    .consume(messageExt, fifoConsumptionSuspendTimeMillis, TimeUnit.MILLISECONDS);
            Futures.addCallback(future, new FutureCallback<ConsumeStatus>() {
                @Override
                public void onSuccess(ConsumeStatus consumeStatus) {
                    eraseFifoMessage(messageExt, consumeStatus);
                }

                @Override
                public void onFailure(Throwable t) {
                    // should never reach here.
                    log.error("[Bug] Exception raised while fifo message redelivery, namespace={}, mq={}, "
                              + "messageId={}, attempt={}, maxAttempts={}, clientId={}", namespace, mq,
                              messageExt.getMsgId(), messageExt.getDeliveryAttempt(), maxAttempts,
                              consumerImpl.getId(), t);
                }
            });
            return;
        }
        boolean ok = ConsumeStatus.OK.equals(status);
        if (!ok) {
            log.info("Failed to consume fifo message finally, run out of attempt times, maxAttempts={}, attempt={}, "
                     + "namespace={}, mq={}, messageId={}, clientId={}", maxAttempts, attempt, namespace, mq,
                     messageExt.getMsgId(), consumerImpl.getId());
        }
        // ack message or forward it to DLQ depends on consumption status.
        SimpleFuture future = ok ? ackFifoMessage(messageExt) : forwardToDeadLetterQueue(messageExt);
        future.addListener(new Runnable() {
            @Override
            public void run() {
                eraseMessage(messageExt);
                fifoConsumptionOutbound();
                // need to signal to dispatch message immediately because of the end of last message's life cycle.
                consumeService.signal();
            }
        }, consumerImpl.getConsumptionExecutor());
    }

    /**
     * Erase message list from in-flight message list, and re-calculate the total messages body size.
     *
     * @param messageExtList message list to erase.
     */
    private void eraseMessages(final List<MessageExt> messageExtList) {
        List<Long> offsetList = new ArrayList<Long>();
        inflightMessagesLock.writeLock().lock();
        try {
            for (MessageExt messageExt : messageExtList) {
                if (inflightMessages.remove(messageExt)) {
                    cachedMessagesBytes.addAndGet(-messageExt.getBody().length);
                }
                offsetList.add(messageExt.getQueueOffset());
            }
        } finally {
            inflightMessagesLock.writeLock().unlock();
        }

        if (!consumerImpl.isOffsetRecorded()) {
            return;
        }
        // maintain offset records
        nextOffsetRecord.remove(offsetList);
        final Optional<Long> optionalOffset = nextOffsetRecord.next();
        if (!optionalOffset.isPresent()) {
            return;
        }
        final Long offset = optionalOffset.get();
        consumerImpl.updateOffset(mq, offset);
    }

    /**
     * Erase consumed message froms {@link ProcessQueueImpl#pendingMessages} and execute ack/nack.
     *
     * @param messageExtList consumed message list.
     * @param status         consume status, which indicates to ack/nack message.
     */
    @Override
    public void eraseMessages(List<MessageExt> messageExtList, ConsumeStatus status) {
        statsConsumptionStatus(messageExtList.size(), status);
        eraseMessages(messageExtList);
        final MessageModel messageModel = consumerImpl.getMessageModel();
        // for clustering mode.
        if (MessageModel.CLUSTERING.equals(messageModel)) {
            // for success.
            if (ConsumeStatus.OK.equals(status)) {
                for (MessageExt messageExt : messageExtList) {
                    ackMessage(messageExt);
                }
                return;
            }
            // for failure.
            for (MessageExt messageExt : messageExtList) {
                final int maxAttempts = consumerImpl.getMaxDeliveryAttempts();
                final int attempt = messageExt.getDeliveryAttempt();
                if (maxAttempts <= attempt) {
                    log.error("Failed to consume message finally, run out of attempt times, maxAttempts={}, "
                              + "attempt={}, namespace={}, mq={}, messageId={}, clientId={}", maxAttempts, attempt,
                              consumerImpl.getNamespace(), mq, messageExt.getMsgId(), consumerImpl.getId());
                }
                consumerImpl.nackMessage(messageExt);
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
                    consumerImpl.getReceivedMessagesQuantity().getAndAdd(messagesFound.size());
                    consumerImpl.getConsumeService().signal();
                }
                log.debug("Receive message with OK, namespace={}, mq={}, endpoints={}, messages found count={}, "
                          + "clientId={}", consumerImpl.getNamespace(), mq, endpoints, messagesFound.size(),
                          consumerImpl.getId());
                receiveMessage();
                break;
            // fall through on purpose.
            case DEADLINE_EXCEEDED:
            case RESOURCE_EXHAUSTED:
            case NOT_FOUND:
            case DATA_CORRUPTED:
            case INTERNAL:
            default:
                log.error("Receive message with status={}, namespace={}, mq={}, endpoints={}, messages found "
                          + "count={}, clientId={}", receiveStatus, consumerImpl.getNamespace(), mq, endpoints,
                          messagesFound.size(), consumerImpl.getId());
                receiveMessageLater();
        }
    }

    private void onPullMessageResult(PullMessageResult result) {
        final PullStatus pullStatus = result.getPullStatus();
        final List<MessageExt> messagesFound = result.getMessagesFound();

        switch (pullStatus) {
            case OK:
                if (!messagesFound.isEmpty()) {
                    cacheMessages(messagesFound);
                    consumerImpl.getPulledMessagesQuantity().getAndAdd(messagesFound.size());
                    consumerImpl.getConsumeService().signal();
                }
                log.debug("Pull message with OK, namespace={}, mq={}, messages found count={}, clientId={}",
                          consumerImpl.getNamespace(), mq, messagesFound.size(), consumerImpl.getId());
                pullMessage(result.getNextBeginOffset());
                break;
            // fall through on purpose.
            case DEADLINE_EXCEEDED:
            case RESOURCE_EXHAUSTED:
            case NOT_FOUND:
            case OUT_OF_RANGE:
            case INTERNAL:
            default:
                log.error("Pull message with status={}, namespace={}, mq={}, messages found count={}, clientId={}",
                          pullStatus, consumerImpl.getNamespace(), mq, messagesFound.size(), consumerImpl.getId());
                pullMessageLater(result.getNextBeginOffset());
        }
    }

    @VisibleForTesting
    public void receiveMessage() {
        if (dropped) {
            log.debug("Process queue has been dropped, no longer receive message, namespace={}, mq={}, clientId={}",
                      consumerImpl.getNamespace(), mq, consumerImpl.getId());
            return;
        }
        if (this.isCacheFull()) {
            log.warn("Process queue cache is full, would receive message later, namespace={}, mq={}, clientId={}",
                     consumerImpl.getNamespace(), mq, consumerImpl.getId());
            receiveMessageLater();
            return;
        }
        receiveMessageImmediately();
    }

    /**
     * Receive message later by message queue.
     *
     * <p> Make sure that no exception will be thrown.
     */
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
            if (scheduler.isShutdown()) {
                return;
            }
            // should never reach here.
            log.error("[Bug] Failed to schedule receive message request, namespace={}, mq={}, clientId={}",
                      consumerImpl.getNamespace(), mq, consumerImpl.getId(), t);
            receiveMessageLater();
        }
    }

    public boolean isCacheFull() {
        final long actualMessagesQuantity = this.cachedMessagesQuantity();
        final int cachedMessageQuantityThresholdPerQueue = consumerImpl.cachedMessagesQuantityThresholdPerQueue();
        if (cachedMessageQuantityThresholdPerQueue <= actualMessagesQuantity) {
            log.warn("Process queue total cached messages quantity exceeds the threshold, threshold={}, actual={}, "
                     + "namespace={}, mq={}, clientId={}", cachedMessageQuantityThresholdPerQueue,
                     actualMessagesQuantity, consumerImpl.getNamespace(), mq, consumerImpl.getId());
            cacheFullNanoTime = System.nanoTime();
            return true;
        }
        final int cachedMessagesBytesPerQueue = consumerImpl.cachedMessagesBytesThresholdPerQueue();
        final long actualCachedMessagesBytes = this.cachedMessageBytes();
        if (cachedMessagesBytesPerQueue <= actualCachedMessagesBytes) {
            log.warn("Process queue total cached messages memory exceeds the threshold, threshold={} bytes, actual={} "
                     + "bytes, namespace={}, mq={}, clientId={}", cachedMessagesBytesPerQueue,
                     actualCachedMessagesBytes, consumerImpl.getNamespace(), mq, consumerImpl.getId());
            cacheFullNanoTime = System.nanoTime();
            return true;
        }
        return false;
    }

    @Override
    public boolean expired() {
        final long idleMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - activityNanoTime);
        if (idleMillis < MAX_IDLE_MILLIS) {
            return false;
        }

        final long cacheFullIdleMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - cacheFullNanoTime);
        if (cacheFullIdleMillis < MAX_IDLE_MILLIS) {
            return false;
        }

        log.warn("Process queue is idle, reception idle time={}ms, cache full idle time={}ms, namespace={}, mq={}, "
                 + "clientId={}", idleMillis, cacheFullIdleMillis, consumerImpl.getNamespace(), mq,
                 consumerImpl.getId());
        return true;
    }

    private ListenableFuture<Long> queryOffset() {
        QueryOffsetRequest.Builder builder = QueryOffsetRequest.newBuilder();
        switch (consumerImpl.getConsumeFromWhere()) {
            case CONSUME_FROM_TIMESTAMP:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.TIME_POINT);
                builder.setTimePoint(Timestamps.fromMillis(consumerImpl.getConsumeFromTimeMillis()));
                break;
            case CONSUME_FROM_FIRST_OFFSET:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.BEGINNING);
                break;
            default:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.END);
        }
        builder.setPartition(getPbPartition());
        final QueryOffsetRequest request = builder.build();
        final Endpoints endpoints = mq.getPartition().getBroker().getEndpoints();
        return consumerImpl.queryOffset(request, endpoints);
    }

    /**
     * Pull message immediately.
     *
     * <p> Actually, offset for message queue to pull from must be fetched before. If offset store was customized, it
     * would be read from offset store, otherwise offset would be fetched from remote according to the consumption
     * policy.
     */
    private void pullMessageImmediately() {
        if (consumerImpl.isOffsetRecorded()) {
            Optional<Long> optionalOffset;
            try {
                optionalOffset = consumerImpl.readOffset(mq);
                if (optionalOffset.isPresent()) {
                    pullMessage(optionalOffset.get());
                    return;
                }
            } catch (Throwable t) {
                log.error("Exception raised while reading offset from offset store, drop message queue, namespace={},"
                          + " mq={}, clientId={}", consumerImpl.getNamespace(), mq, consumerImpl.getId(), t);
                // drop this pq, waiting for the next assignments scan.
                consumerImpl.dropProcessQueue(mq);
                return;
            }
        }
        ListenableFuture<Long> future;
        log.info("Offset not found, try to query offset from remote, namespace={}, mq={}, clientId={}",
                 consumerImpl.getNamespace(), mq, consumerImpl.getId());
        try {
            future = queryOffset();
        } catch (Throwable t) {
            // should never reach here.
            SettableFuture<Long> future0 = SettableFuture.create();
            future0.setException(t);
            future = future0;
        }
        final Endpoints endpoints = mq.getPartition().getBroker().getEndpoints();
        Futures.addCallback(future, new FutureCallback<Long>() {
            @Override
            public void onSuccess(Long offset) {
                log.info("Query offset successfully, namespace={}, mq={}, endpoints={}, offset={}, clientId={}",
                         consumerImpl.getNamespace(), mq, endpoints, offset, consumerImpl.getId());
                if (consumerImpl.isOffsetRecorded()) {
                    consumerImpl.updateOffset(mq, offset);
                }
                pullMessage(offset);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while querying offset to pull, drop message queue, namespace={}, mq={}, "
                          + "endpoints={}, clientId={}", consumerImpl.getNamespace(), mq, endpoints,
                          consumerImpl.getId(), t);
                // drop this pq, waiting for the next assignments scan.
                consumerImpl.dropProcessQueue(mq);
            }
        });
    }

    /**
     * Pull message immediately by message queue according to the given offset.
     *
     * <p> Make sure that there is no any exception thrown.
     *
     * @param offset offset for message queue to pull from.
     */
    private void pullMessageImmediately(final long offset) {
        if (!consumerImpl.isRunning()) {
            log.info("Stop to pull message because consumer is not running, namespace={}, mq={}, clientId={}",
                     consumerImpl.getNamespace(), mq, consumerImpl.getId());
            return;
        }
        try {
            final Endpoints endpoints = mq.getPartition().getBroker().getEndpoints();
            final PullMessageRequest request = wrapPullMessageRequest(offset);
            activityNanoTime = System.nanoTime();
            // intercept before pull
            final MessageInterceptorContext preContext = MessageInterceptorContext.builder()
                                                                                  .setBatchSize(request.getBatchSize())
                                                                                  .setTopic(mq.getTopic()).build();
            consumerImpl.intercept(MessageHookPoint.PRE_PULL, preContext);
            final Stopwatch stopwatch = Stopwatch.createStarted();

            final ListenableFuture<PullMessageResult> future =
                    consumerImpl.pullMessage(request, endpoints, PULL_LONG_POLLING_TIMEOUT_MILLIS);
            Futures.addCallback(future, new FutureCallback<PullMessageResult>() {
                @Override
                public void onSuccess(PullMessageResult pullMessageResult) {
                    final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                    final List<MessageExt> messagesFound = pullMessageResult.getMessagesFound();
                    MessageHookPointStatus status = PullStatus.OK.equals(pullMessageResult.getPullStatus()) ?
                                                    MessageHookPointStatus.OK : MessageHookPointStatus.ERROR;
                    final MessageInterceptorContext postContext = preContext.toBuilder().setDuration(duration)
                                                                            .setStatus(status).build();
                    // intercept after empty pull
                    if (messagesFound.isEmpty()) {
                        consumerImpl.intercept(MessageHookPoint.POST_PULL, postContext);
                    }
                    // intercept after pull
                    for (MessageExt messageExt : messagesFound) {
                        consumerImpl.intercept(MessageHookPoint.POST_PULL, messageExt, postContext);
                    }
                    try {
                        onPullMessageResult(pullMessageResult);
                    } catch (Throwable t) {
                        // should never reach here.
                        log.error("[Bug] Exception raised while handling pull result, would pull later, namespace={} "
                                  + "mq={}, endpoints={}, clientId={}", consumerImpl.getNamespace(), mq, endpoints,
                                  consumerImpl.getId(), t);
                        pullMessageLater(offset);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    // intercept after pull
                    final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                    final MessageInterceptorContext context = preContext.toBuilder().setDuration(duration)
                                                                        .setStatus(MessageHookPointStatus.ERROR)
                                                                        .setThrowable(t).build();
                    consumerImpl.intercept(MessageHookPoint.POST_PULL, context);

                    log.error("Exception raised while pull message, would pull later, namespace={}, mq={}, "
                              + "endpoints={}, clientId={}", consumerImpl.getNamespace(), mq, endpoints,
                              consumerImpl.getId(), t);
                    pullMessageLater(offset);
                }
            });
            consumerImpl.getPullTimes().getAndIncrement();
        } catch (Throwable t) {
            log.error("Exception raised while pull message, would pull message later, namespace={}, mq={}, clientId={}",
                      consumerImpl.getNamespace(), mq, consumerImpl.getId(), t);
            pullMessageLater(offset);
        }
    }

    /**
     * Pull message later by message queue according to the given offset.
     *
     * <p> Make sure that no exception will be thrown.
     *
     * @param offset offset for message queue to pull from.
     */
    public void pullMessageLater(final long offset) {
        final ScheduledExecutorService scheduler = consumerImpl.getScheduler();
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    pullMessage(offset);
                }
            }, PULL_LATER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // should never reach here.
            log.error("[Bug] Failed to schedule pull message request, namespace={}, mq={}, clientId={}",
                      consumerImpl.getNamespace(), mq, consumerImpl.getId(), t);
            pullMessageLater(offset);
        }
    }

    /**
     * Pull message by message queue according to the given offset.
     *
     * <p> Make sure that there is no any exception thrown.
     *
     * @param offset offset for message queue to pull from.
     */
    private void pullMessage(long offset) {
        final String id = consumerImpl.getId();
        if (dropped) {
            log.info("Process queue has been dropped, no longer pull message, namespace={}, mq={}, clientId={}",
                     consumerImpl.getNamespace(), mq, id);
            return;
        }
        if (this.isCacheFull()) {
            log.warn("Process queue cache is full, would pull message later, namespace={}, mq={}, clientId={}",
                     consumerImpl.getNamespace(), mq, id);
            pullMessageLater(offset);
            return;
        }
        pullMessageImmediately(offset);
    }

    private int getMaxAwaitBatchSize() {
        final int cacheBufferSize =
                Math.max(consumerImpl.cachedMessagesQuantityThresholdPerQueue() - this.cachedMessagesQuantity(), 1);
        return Math.min(cacheBufferSize, consumerImpl.getMaxAwaitBatchSizePerQueue());
    }

    @Override
    public void fetchMessageImmediately() {
        // for clustering mode.
        if (MessageModel.CLUSTERING.equals(consumerImpl.getMessageModel())) {
            receiveMessageImmediately();
            return;
        }
        // for broadcasting mode.
        pullMessageImmediately();
    }

    ReceiveMessageRequest wrapReceiveMessageRequest() {
        final int maxAwaitBatchSize = getMaxAwaitBatchSize();
        final Duration invisibleDuration = Durations.fromMillis(consumerImpl.getConsumptionTimeoutMillis());
        final Duration maxAwaitTimeMillis = Durations.fromMillis(consumerImpl.getMaxAwaitTimeMillisPerQueue());
        final ReceiveMessageRequest.Builder builder =
                ReceiveMessageRequest.newBuilder()
                                     .setGroup(consumerImpl.getPbGroup())
                                     .setClientId(consumerImpl.getId())
                                     .setPartition(getPbPartition()).setBatchSize(maxAwaitBatchSize)
                                     .setInvisibleDuration(invisibleDuration)
                                     .setAwaitTime(maxAwaitTimeMillis);

        switch (consumerImpl.getConsumeFromWhere()) {
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
        builder.setFilterExpression(getPbFilterExpression());
        builder.setFifoFlag(MessageListenerType.ORDERLY.equals(consumerImpl.getMessageListener().getListenerType()));
        return builder.build();
    }

    PullMessageRequest wrapPullMessageRequest(long offset) {
        final int maxAwaitBatchSize = getMaxAwaitBatchSize();
        final long maxAwaitTimeMillis = consumerImpl.getMaxAwaitTimeMillisPerQueue();
        return PullMessageRequest.newBuilder().setGroup(consumerImpl.getPbGroup()).setPartition(getPbPartition())
                                 .setOffset(offset).setBatchSize(maxAwaitBatchSize)
                                 .setAwaitTime(Durations.fromMillis(maxAwaitTimeMillis))
                                 .setFilterExpression(getPbFilterExpression())
                                 .setClientId(consumerImpl.getId())
                                 .build();
    }

    private void receiveMessageImmediately() {
        if (!consumerImpl.isRunning()) {
            log.info("Stop to receive message because consumer is not running, namespace={}, mq={}, clientId={}",
                     consumerImpl.getNamespace(), mq, consumerImpl.getId());
            return;
        }
        try {
            final Endpoints endpoints = mq.getPartition().getBroker().getEndpoints();
            final ReceiveMessageRequest request = wrapReceiveMessageRequest();
            activityNanoTime = System.nanoTime();
            // intercept before receive
            final MessageInterceptorContext preContext = MessageInterceptorContext.builder()
                                                                                  .setBatchSize(request.getBatchSize())
                                                                                  .setTopic(mq.getTopic()).build();
            consumerImpl.intercept(MessageHookPoint.PRE_RECEIVE, preContext);
            final Stopwatch stopwatch = Stopwatch.createStarted();
            final ListenableFuture<ReceiveMessageResult> future =
                    consumerImpl.receiveMessage(request, endpoints, RECEIVE_LONG_POLLING_TIMEOUT_MILLIS);
            Futures.addCallback(future, new FutureCallback<ReceiveMessageResult>() {
                @Override
                public void onSuccess(ReceiveMessageResult receiveMessageResult) {
                    final long elapsed = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                    final List<MessageExt> messagesFound = receiveMessageResult.getMessagesFound();
                    MessageHookPointStatus status = ReceiveStatus.OK.equals(receiveMessageResult.getReceiveStatus()) ?
                                                    MessageHookPointStatus.OK : MessageHookPointStatus.ERROR;
                    final MessageInterceptorContext postContext = preContext.toBuilder().setDuration(elapsed)
                                                                            .setStatus(status).build();
                    // intercept after empty receive
                    if (messagesFound.isEmpty()) {
                        consumerImpl.intercept(MessageHookPoint.POST_RECEIVE, postContext);
                    }
                    // intercept after receive
                    for (MessageExt messageExt : messagesFound) {
                        consumerImpl.intercept(MessageHookPoint.POST_RECEIVE, messageExt, postContext);
                    }
                    try {
                        onReceiveMessageResult(receiveMessageResult);
                    } catch (Throwable t) {
                        // should never reach here.
                        log.error("[Bug] Exception raised while handling receive result, would receive later, "
                                  + "namespace={}, mq={}, endpoints={}, clientId={}", consumerImpl.getNamespace(), mq,
                                  endpoints, consumerImpl.getId(), t);
                        receiveMessageLater();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    // intercept after receive
                    final long elapsed = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                    final MessageInterceptorContext context = preContext.toBuilder().setDuration(elapsed)
                                                                        .setStatus(MessageHookPointStatus.ERROR)
                                                                        .setThrowable(t).build();
                    consumerImpl.intercept(MessageHookPoint.POST_RECEIVE, context);
                    log.error("Exception raised while message reception, would receive later, namespace={}, mq={}, "
                              + "endpoints={}, clientId={}", consumerImpl.getNamespace(), mq, endpoints,
                              consumerImpl.getId(), t);
                    receiveMessageLater();
                }
            });
            consumerImpl.getReceptionTimes().getAndIncrement();
        } catch (Throwable t) {
            log.error("Exception raised while message reception, would receive later, namespace={}, mq={}, clientId={}",
                      consumerImpl.getNamespace(), mq, consumerImpl.getId(), t);
            receiveMessageLater();
        }
    }

    private SimpleFuture ackFifoMessage(final MessageExt messageExt) {
        SimpleFuture future0 = new SimpleFuture();
        ackFifoMessage(messageExt, 1, future0);
        return future0;
    }

    private void ackFifoMessage(final MessageExt messageExt, final int attempt, final SimpleFuture future0) {
        final Endpoints endpoints = messageExt.getEndpoints();
        final String namespace = consumerImpl.getNamespace();
        final String clientId = consumerImpl.getId();
        final ListenableFuture<AckMessageResponse> future = consumerImpl.ackMessage(messageExt, attempt);
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (!Code.OK.equals(code)) {
                    log.error("Failed to ack fifo message, would attempt to re-ack later, clientId={}, attempt={}, "
                              + "messageId={}, namespace={}, mq={}, code={}, endpoints={}, status message=[{}]",
                              clientId, attempt, messageExt.getMsgId(), namespace, mq, code, endpoints,
                              status.getMessage());
                    ackFifoMessageLater(messageExt, 1 + attempt, future0);
                    return;
                }
                if (1 < attempt) {
                    log.info("Re-ack fifo message successfully, clientId={}, attempt={}, messageId={}, namespace={}, "
                             + "mq={}, endpoints={}", clientId, attempt, messageExt.getMsgId(), namespace, mq,
                             endpoints);
                }
                future0.markAsDone();
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while ack fifo message, clientId={}, would attempt to re-ack later, "
                          + "attempt={}, messageId={}, namespace={}, mq={}, endpoints={}", clientId, attempt,
                          messageExt.getMsgId(), namespace, mq, endpoints, t);
                ackFifoMessageLater(messageExt, 1 + attempt, future0);
            }
        });
    }

    private void ackFifoMessageLater(final MessageExt messageExt, final int attempt, final SimpleFuture future0) {
        final String msgId = messageExt.getMsgId();
        final String clientId = consumerImpl.getId();
        if (dropped) {
            log.info("Process queue was dropped, give up to ack message, namespace={}, mq={}, messageId={}, "
                     + "clientId={}", consumerImpl.getNamespace(), mq, msgId, clientId);
            return;
        }
        final ScheduledExecutorService scheduler = consumerImpl.getScheduler();
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    ackFifoMessage(messageExt, attempt, future0);
                }
            }, ACK_FIFO_MESSAGE_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // should never reach here.
            log.error("[Bug] Failed to schedule ack fifo message request, namespace={}, mq={}, msgId={}, clientId={}",
                      consumerImpl.getNamespace(), mq, msgId, clientId);
            ackFifoMessageLater(messageExt, 1 + attempt, future0);
        }
    }

    private SimpleFuture forwardToDeadLetterQueue(final MessageExt messageExt) {
        final SimpleFuture future0 = new SimpleFuture();
        forwardToDeadLetterQueue(messageExt, 1, future0);
        return future0;
    }

    private void forwardToDeadLetterQueue(final MessageExt messageExt, final int attempt, final SimpleFuture future0) {
        final ListenableFuture<ForwardMessageToDeadLetterQueueResponse> future =
                consumerImpl.forwardMessageToDeadLetterQueue(messageExt, attempt);
        final String namespace = consumerImpl.getNamespace();
        final String clientId = consumerImpl.getId();
        Futures.addCallback(future, new FutureCallback<ForwardMessageToDeadLetterQueueResponse>() {
            @Override
            public void onSuccess(ForwardMessageToDeadLetterQueueResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (!Code.OK.equals(code)) {
                    log.error("Failed to forward message to DLQ, would attempt to re-forward later, clientId={}, "
                              + "messageId={}, attempt={}, namespace={}, mq={}, code={}, status message=[{}]",
                              clientId, messageExt.getMsgId(), attempt, namespace, mq, code, status.getMessage());
                    forwardToDeadLetterQueueLater(messageExt, 1 + attempt, future0);
                    return;
                }
                if (1 < attempt) {
                    log.info("Re-forward message to DLQ successfully, clientId={}, attempt={}, messageId={}, "
                             + "namespace={}, mq={}", clientId, attempt, messageExt.getMsgId(), namespace, mq);
                }
                future0.markAsDone();
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while forward message to DLQ, would attempt to re-forward later, "
                          + "clientId={}, attempt={}, messageId={}, namespace={}, mq={}", clientId, attempt,
                          messageExt.getMsgId(), namespace, mq, t);
                forwardToDeadLetterQueueLater(messageExt, 1 + attempt, future0);
            }
        });
    }

    private void forwardToDeadLetterQueueLater(final MessageExt messageExt, final int attempt,
                                               final SimpleFuture future0) {
        final String msgId = messageExt.getMsgId();
        final String clientId = consumerImpl.getId();
        if (dropped) {
            log.info("Process queue was dropped, give up to forward message to DLQ, namespace={}, mq={}, "
                     + "messageId={}, clientId={}", consumerImpl.getNamespace(), mq, msgId, clientId);
            return;
        }
        final ScheduledExecutorService scheduler = consumerImpl.getScheduler();
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    forwardToDeadLetterQueue(messageExt, attempt, future0);
                }
            }, FORWARD_FIFO_MESSAGE_TO_DLQ_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // should never reach here.
            log.error("[Bug] Failed to schedule DLQ message request, namespace={}, mq={}, msgId={}, clientId={}",
                      consumerImpl.getNamespace(), mq, msgId, clientId);
            forwardToDeadLetterQueueLater(messageExt, 1 + attempt, future0);
        }
    }

    public void ackMessage(final MessageExt messageExt) {
        final ListenableFuture<AckMessageResponse> future = consumerImpl.ackMessage(messageExt);
        final String namespace = consumerImpl.getNamespace();
        final String clientId = consumerImpl.getId();
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (!Code.OK.equals(code)) {
                    log.error("Failed to ack message, clientId={}, messageId={}, namespace={}, mq={}, code={}, status "
                              + "message=[{}]", clientId, messageExt.getMsgId(), namespace, mq, code,
                              status.getMessage());
                    return;
                }
                log.trace("Ack message successfully, clientId={}, messageId={}, namespace={}, mq={}", clientId,
                          messageExt.getMsgId(), namespace, mq);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while ack message, clientId={}, messageId={}, namespace={}, mq={}",
                          clientId, messageExt.getMsgId(), namespace, mq, t);
            }
        });
    }

    private apache.rocketmq.v1.FilterExpression getPbFilterExpression() {
        final ExpressionType expressionType = filterExpression.getExpressionType();

        apache.rocketmq.v1.FilterExpression.Builder expressionBuilder =
                apache.rocketmq.v1.FilterExpression.newBuilder();

        final String expression = filterExpression.getExpression();
        expressionBuilder.setExpression(expression);
        switch (expressionType) {
            case SQL92:
                return expressionBuilder.setType(FilterType.SQL).build();
            case TAG:
            default:
                return expressionBuilder.setType(FilterType.TAG).build();
        }
    }

    private Partition getPbPartition() {
        final Resource protoTopic =
                Resource.newBuilder().setResourceNamespace(consumerImpl.getNamespace()).setName(mq.getTopic()).build();
        final Broker broker = Broker.newBuilder().setName(mq.getBrokerName()).build();
        return Partition.newBuilder().setTopic(protoTopic).setId(mq.getQueueId()).setBroker(broker).build();
    }

    public int cachedMessagesQuantity() {
        pendingMessagesLock.readLock().lock();
        inflightMessagesLock.readLock().lock();
        try {
            return pendingMessages.size() + inflightMessages.size();
        } finally {
            inflightMessagesLock.readLock().unlock();
            pendingMessagesLock.readLock().unlock();
        }
    }

    public int inflightMessagesQuantity() {
        inflightMessagesLock.readLock().lock();
        try {
            return inflightMessages.size();
        } finally {
            inflightMessagesLock.readLock().unlock();
        }
    }

    public long cachedMessageBytes() {
        return cachedMessagesBytes.get();
    }

    private void statsConsumptionStatus(ConsumeStatus status) {
        statsConsumptionStatus(1, status);
    }

    private void statsConsumptionStatus(int messageQuantity, ConsumeStatus status) {
        if (ConsumeStatus.OK.equals(status)) {
            consumerImpl.getConsumptionOkQuantity().addAndGet(messageQuantity);
            return;
        }
        consumerImpl.getConsumptionErrorQuantity().addAndGet(messageQuantity);
    }

    @Override
    public MessageQueue getMessageQueue() {
        return this.mq;
    }
}
