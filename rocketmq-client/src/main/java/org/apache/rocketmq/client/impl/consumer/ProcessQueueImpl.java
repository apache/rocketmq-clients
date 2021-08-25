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
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.Resource;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.TreeSet;
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
import org.apache.rocketmq.client.message.MessageAccessor;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.utility.SimpleFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessQueueImpl implements ProcessQueue {
    public static final long RECEIVE_LONG_POLLING_TIMEOUT_MILLIS = 30 * 1000L;
    public static final long RECEIVE_LATER_DELAY_MILLIS = 3 * 1000L;

    public static final long PULL_LONG_POLLING_TIMEOUT_MILLIS = 30 * 1000L;
    public static final long PULL_LATER_DELAY_MILLIS = 3 * 1000L;

    public static final long MAX_IDLE_MILLIS = 60 * 1000L;
    public static final long ACK_FIFO_MESSAGE_DELAY_MILLIS = 100L;
    public static final long REDIRECT_FIFO_MESSAGE_TO_DLQ_DELAY_MILLIS = 100L;

    private static final Logger log = LoggerFactory.getLogger(ProcessQueueImpl.class);

    private volatile boolean dropped;

    private final MessageQueue mq;
    private final FilterExpression filterExpression;

    private final PushConsumerImpl consumerImpl;

    @GuardedBy("pendingMessagesLock")
    private final List<MessageExt> pendingMessages;
    private final ReadWriteLock pendingMessagesLock;

    @GuardedBy("inflightMessagesLock")
    private final List<MessageExt> inflightMessages;
    private final ReadWriteLock inflightMessagesLock;

    @GuardedBy("offsetRecordsLock")
    private final TreeSet<OffsetRecord> offsetRecords;
    private final ReadWriteLock offsetRecordsLock;

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

        this.offsetRecords = new TreeSet<OffsetRecord>();
        this.offsetRecordsLock = new ReentrantReadWriteLock();
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

    private boolean noNeedMaintainOffsetRecords() {
        final MessageModel messageModel = consumerImpl.getMessageModel();
        final boolean hasCustomOffsetStore = consumerImpl.hasCustomOffsetStore();
        return !MessageModel.BROADCASTING.equals(messageModel) || !hasCustomOffsetStore;
    }

    @VisibleForTesting
    public void cacheMessages(List<MessageExt> messageExtList) {
        List<Long> offsetList = new ArrayList<Long>();
        final MessageListenerType listenerType = consumerImpl.getMessageListener().getListenerType();
        final MessageModel messageModel = consumerImpl.getMessageModel();
        pendingMessagesLock.writeLock().lock();
        try {
            for (MessageExt messageExt : messageExtList) {
                if (MessageAccessor.getMessageImpl(messageExt).isCorrupted()) {
                    // ignore message in broadcasting mode.
                    if (MessageModel.BROADCASTING.equals(messageModel)) {
                        log.error("Message is corrupted, ignore it in broadcasting mode, mq={}, messageId={}", mq,
                                  messageExt.getMsgId());
                        continue;
                    }
                    // nack message for concurrent consumption.
                    if (MessageListenerType.CONCURRENTLY.equals(listenerType)) {
                        log.error("Message is corrupted, nack it for concurrently consumption, mq={}, messageId={}",
                                  mq, messageExt.getMsgId());
                        consumerImpl.nackMessage(messageExt);
                    }
                    // forward to DLQ for fifo consumption.
                    if (MessageListenerType.ORDERLY.equals(listenerType)) {
                        log.error("Message is corrupted, forward it to DLQ for fifo consumption, mq={}, messageId={}",
                                  mq, messageExt.getMsgId());
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

        if (noNeedMaintainOffsetRecords()) {
            return;
        }
        // maintain offset records.
        offsetRecordsLock.writeLock().lock();
        try {
            for (Long offset : offsetList) {
                if (1 == offsetRecords.size()) {
                    final OffsetRecord offsetRecord = offsetRecords.iterator().next();
                    if (offsetRecord.isReleased()) {
                        offsetRecords.remove(offsetRecord);
                    }
                }
                offsetRecords.add(new OffsetRecord(offset));
            }
        } finally {
            offsetRecordsLock.writeLock().unlock();
        }
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
     * @return message which has been taken, or null if no message available
     */
    @Override
    public MessageExt tryTakeFifoMessage() {
        pendingMessagesLock.writeLock().lock();
        inflightMessagesLock.writeLock().lock();
        try {
            // no new message arrived.
            if (pendingMessages.isEmpty()) {
                return null;
            }
            // failed to lock.
            if (!fifoConsumptionInbound()) {
                log.debug("Fifo consumption task are not finished, mq={}", mq);
                return null;
            }
            final String topic = mq.getTopic();
            final RateLimiter rateLimiter = consumerImpl.rateLimiter(topic);
            // no rate limiter for current topic.
            if (null == rateLimiter) {
                final MessageExt first = pendingMessages.iterator().next();
                pendingMessages.remove(first);
                inflightMessages.add(first);
                return first;
            }
            // unlock because of the failure of acquire the token,
            if (!rateLimiter.tryAcquire()) {
                fifoConsumptionOutbound();
                return null;
            }
            final MessageExt first = pendingMessages.iterator().next();
            pendingMessages.remove(first);
            inflightMessages.add(first);
            return first;
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
            log.info("clientId={}, arn={}, mq={}, pendingMessageQuantity={}, inflightMessageQuantity={}, "
                     + "cachedMessagesBytes={}", consumerImpl.getClientId(), consumerImpl.getArn(), mq,
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
        // failed to consume fifo message but deliver attempt are not exhausted.
        // need to redeliver the fifo message.
        if (ConsumeStatus.ERROR.equals(status) && attempt < maxAttempts) {
            final MessageImpl messageImpl = MessageAccessor.getMessageImpl(messageExt);
            final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
            // increment the delivery attempt and prepare to deliver message once again.
            systemAttribute.setDeliveryAttempt(1 + attempt);
            // try to deliver message once again.
            final long fifoConsumptionSuspendTimeMillis = consumerImpl.getFifoConsumptionSuspendTimeMillis();
            final ConsumeService consumeService = consumerImpl.getConsumeService();
            log.debug("Prepare to redeliver the fifo message because of consumption failure, maxAttempt={}, "
                      + "attempt={}, mq={}, messageId={}, suspendTime={}ms", maxAttempts,
                      messageExt.getDeliveryAttempt(), mq, messageExt.getMsgId(), fifoConsumptionSuspendTimeMillis);
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
                    log.error("[Bug] Exception raised while fifo message redelivery, mq={}, messageId={}, attempt={}, "
                              + "maxAttempts={}", mq, messageExt.getMsgId(), messageExt.getDeliveryAttempt(),
                              maxAttempts, t);
                }
            });
            return;
        }
        boolean ok = ConsumeStatus.OK.equals(status);
        if (!ok) {
            log.info("Failed to consume fifo message finally, run out of attempt times, maxAttempts={}, attempt={}, "
                     + "mq={}, messageId={}", maxAttempts, attempt, mq, messageExt.getMsgId());
        }
        // ack message or forward it to DLQ depends on consumption status.
        SimpleFuture future = ok ? ackFifoMessage(messageExt) : forwardToDeadLetterQueue(messageExt);
        future.addListener(new Runnable() {
            @Override
            public void run() {
                eraseMessage(messageExt);
                fifoConsumptionOutbound();
            }
        }, consumerImpl.getConsumptionExecutor());
    }

    private Long getNextOffsetInRecords() {
        offsetRecordsLock.readLock().lock();
        try {
            if (offsetRecords.isEmpty()) {
                return null;
            }
            final OffsetRecord offsetRecord = offsetRecords.iterator().next();
            if (offsetRecord.isReleased()) {
                return offsetRecord.getOffset() + 1;
            }
            return offsetRecord.getOffset();
        } finally {
            offsetRecordsLock.readLock().unlock();
        }
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

        if (noNeedMaintainOffsetRecords()) {
            return;
        }
        // maintain offset records
        offsetRecordsLock.writeLock().lock();
        try {
            for (Long offset : offsetList) {
                final OffsetRecord offsetRecord = new OffsetRecord(offset);
                offsetRecords.remove(offsetRecord);
                if (offsetRecords.isEmpty()) {
                    offsetRecord.setReleased(true);
                    offsetRecords.add(offsetRecord);
                }
            }
        } finally {
            offsetRecordsLock.writeLock().unlock();
        }
        final Long nextOffset = getNextOffsetInRecords();
        if (null == nextOffset) {
            return;
        }
        consumerImpl.getOffsetStore().updateOffset(mq, nextOffset);
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
                              + "attempt={}, mq={}, messageId={}", maxAttempts, attempt, mq, messageExt.getMsgId());
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
                    consumerImpl.getConsumeService().dispatch();
                }
                log.debug("Receive message with OK, mq={}, endpoints={}, messages found count={}", mq,
                          endpoints, messagesFound.size());
                receiveMessage();
                break;
            // fall through on purpose.
            case DEADLINE_EXCEEDED:
            case RESOURCE_EXHAUSTED:
            case NOT_FOUND:
            case DATA_CORRUPTED:
            case INTERNAL:
            default:
                log.error("Receive message with status={}, mq={}, endpoints={}, messages found count={}", receiveStatus,
                          mq, endpoints, messagesFound.size());
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
                    consumerImpl.getConsumeService().dispatch();
                }
                log.debug("Pull message with OK, mq={}, messages found count={}", mq, messagesFound.size());
                pullMessage(result.getNextBeginOffset());
                break;
            // fall through on purpose.
            case DEADLINE_EXCEEDED:
            case RESOURCE_EXHAUSTED:
            case NOT_FOUND:
            case OUT_OF_RANGE:
            case INTERNAL:
            default:
                log.error("Pull message with status={}, mq={}, messages found status={}", pullStatus, mq,
                          messagesFound.size());
                pullMessageLater(result.getNextBeginOffset());
        }
    }

    @VisibleForTesting
    public void receiveMessage() {
        if (dropped) {
            log.debug("Process queue has been dropped, no longer receive message, mq={}.", mq);
            return;
        }
        if (this.isCacheFull()) {
            log.warn("Process queue cache is full, would receive message later, mq={}.", mq);
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
            if (scheduler.isShutdown()) {
                return;
            }
            // should never reach here.
            log.error("[Bug] Failed to schedule receive message request", t);
            receiveMessageLater();
        }
    }

    public boolean isCacheFull() {
        final long actualMessagesQuantity = this.cachedMessagesQuantity();
        final int cachedMessageQuantityThresholdPerQueue = consumerImpl.cachedMessagesQuantityThresholdPerQueue();
        if (cachedMessageQuantityThresholdPerQueue <= actualMessagesQuantity) {
            log.warn("Process queue total cached messages quantity exceeds the threshold, threshold={}, actual={}, "
                     + "mq={}", cachedMessageQuantityThresholdPerQueue, actualMessagesQuantity, mq);
            cacheFullNanoTime = System.nanoTime();
            return true;
        }
        final int cachedMessagesBytesPerQueue = consumerImpl.cachedMessagesBytesThresholdPerQueue();
        final long actualCachedMessagesBytes = this.cachedMessageBytes();
        if (cachedMessagesBytesPerQueue <= actualCachedMessagesBytes) {
            log.warn("Process queue total cached messages memory exceeds the threshold, threshold={} bytes, actual={} "
                     + "bytes, mq={}", cachedMessagesBytesPerQueue, actualCachedMessagesBytes, mq);
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

        log.warn("Process queue is idle, reception idle time={}ms, cache full idle time={}ms", idleMillis,
                 cacheFullIdleMillis);
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
     * <p> Actually, offset for message queue to pull from must be fetched before pull. If offset store was
     * customized, it would be read from offset store, otherwise offset would be fetched from remote according to the
     * consumption policy.
     */
    private void pullMessageImmediately() {
        if (consumerImpl.hasCustomOffsetStore()) {
            long offset;
            try {
                offset = consumerImpl.readOffset(mq);
            } catch (Throwable t) {
                log.error("Exception raised while reading offset from offset store, mq={}", mq, t);
                // drop this pq, waiting for the next assignments scan.
                consumerImpl.dropProcessQueue(mq);
                return;
            }
            if (offset >= 0) {
                pullMessage(offset);
                return;
            }
        }
        ListenableFuture<Long> future;
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
                log.info("Query offset successfully, mq={}, endpoints={}, offset={}", mq, endpoints, offset);
                pullMessage(offset);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while querying offset to pull, mq={}, endpoints={}", mq, endpoints, t);
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
        if (consumerImpl.isStopped()) {
            return;
        }
        try {
            final Endpoints endpoints = mq.getPartition().getBroker().getEndpoints();
            final PullMessageRequest request = wrapPullMessageRequest(offset);
            activityNanoTime = System.nanoTime();
            final ListenableFuture<PullMessageResult> future =
                    consumerImpl.pullMessage(request, endpoints, PULL_LONG_POLLING_TIMEOUT_MILLIS);
            Futures.addCallback(future, new FutureCallback<PullMessageResult>() {
                @Override
                public void onSuccess(PullMessageResult pullMessageResult) {
                    try {
                        ProcessQueueImpl.this.onPullMessageResult(pullMessageResult);
                    } catch (Throwable t) {
                        // should never reach here.
                        log.error("[Bug] Exception raised while handling pull result, would pull later, mq={}, "
                                  + "endpoints={}", mq, endpoints, t);
                        pullMessageLater(offset);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Exception raised while pull message, would pull later, mq={}, endpoints={}",
                              mq, endpoints, t);
                    pullMessageLater(offset);
                }
            });
            consumerImpl.getPullTimes().getAndIncrement();
        } catch (Throwable t) {
            log.error("Exception raised while pull message, would pull message later, mq={}", mq, t);
            pullMessageLater(offset);
        }
    }

    /**
     * Pull message later by message queue according to the given offset.
     *
     * <p> Make sure that there is no any exception thrown.
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
            log.error("[Bug] Failed to schedule pull message request", t);
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
        if (dropped) {
            log.info("Process queue has been dropped, no longer pull message, mq={}.", mq);
            return;
        }
        if (this.isCacheFull()) {
            log.warn("Process queue cache is full, would pull message later, mq={}", mq);
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
                                     .setClientId(consumerImpl.getClientId())
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
                                 .setClientId(consumerImpl.getClientId())
                                 .build();
    }

    private void receiveMessageImmediately() {
        if (consumerImpl.isStopped()) {
            return;
        }
        try {
            final Endpoints endpoints = mq.getPartition().getBroker().getEndpoints();
            final ReceiveMessageRequest request = wrapReceiveMessageRequest();
            activityNanoTime = System.nanoTime();
            final ListenableFuture<ReceiveMessageResult> future =
                    consumerImpl.receiveMessage(request, endpoints, RECEIVE_LONG_POLLING_TIMEOUT_MILLIS);
            Futures.addCallback(future, new FutureCallback<ReceiveMessageResult>() {
                @Override
                public void onSuccess(ReceiveMessageResult receiveMessageResult) {
                    try {
                        onReceiveMessageResult(receiveMessageResult);
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
            consumerImpl.getReceptionTimes().getAndIncrement();
        } catch (Throwable t) {
            log.error("Exception raised while message reception, would receive later, mq={}.", mq, t);
            receiveMessageLater();
        }
    }

    private SimpleFuture ackFifoMessage(final MessageExt messageExt) {
        SimpleFuture future0 = new SimpleFuture();
        ackFifoMessage(messageExt, 1, future0);
        return future0;
    }

    private void ackFifoMessage(final MessageExt messageExt, final int attempt, final SimpleFuture future0) {
        final Endpoints endpoints = messageExt.getAckEndpoints();
        final ListenableFuture<AckMessageResponse> future = consumerImpl.ackMessage(messageExt);
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to ack fifo message, would attempt to re-ack later, attempt={}, messageId={}, "
                              + "mq={}, code={}, endpoints={}, status message=[{}].", attempt, messageExt.getMsgId(),
                              mq, code, endpoints, status.getMessage());
                    ackFifoMessageLater(messageExt, 1 + attempt, future0);
                    return;
                }
                if (1 < attempt) {
                    log.info("Re-ack fifo message successfully, attempt={}, messageId={}, mq={}, endpoints={}", attempt,
                             messageExt.getMsgId(), mq, endpoints);
                }
                future0.markAsDone();
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while ack fifo message, would attempt to re-ack later, attempt={}, "
                          + "messageId={}, mq={}, endpoints={}.", attempt, messageExt.getMsgId(), mq, endpoints, t);
                ackFifoMessageLater(messageExt, 1 + attempt, future0);
            }
        });
    }

    private void ackFifoMessageLater(final MessageExt messageExt, final int attempt, final SimpleFuture future0) {
        final String msgId = messageExt.getMsgId();
        if (dropped) {
            log.info("Process queue was dropped, give up to ack message. mq={}, messageId={}", mq, msgId);
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
            log.error("[Bug] Failed to schedule ack fifo message request, mq={}, msgId={}.", mq, msgId);
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
                consumerImpl.forwardMessageToDeadLetterQueue(messageExt);
        Futures.addCallback(future, new FutureCallback<ForwardMessageToDeadLetterQueueResponse>() {
            @Override
            public void onSuccess(ForwardMessageToDeadLetterQueueResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to forward message to DLQ, would attempt to re-forward later, messageId={}, "
                              + "attempt={}, mq={}, code={}, status message=[{}]", messageExt.getMsgId(), attempt, mq,
                              code, status.getMessage());
                    forwardToDeadLetterQueueLater(messageExt, 1 + attempt, future0);
                    return;
                }
                if (1 < attempt) {
                    log.info("Re-forward message to DLQ successfully, attempt={}, messageId={}, mq={}", attempt,
                             messageExt.getMsgId(), mq);
                }
                future0.markAsDone();
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while forward message to DLQ, would attempt to re-forward later, "
                          + "attempt={}, messageId={}, mq={}.", attempt, messageExt.getMsgId(), mq, t);
                forwardToDeadLetterQueueLater(messageExt, 1 + attempt, future0);
            }
        });
    }

    private void forwardToDeadLetterQueueLater(final MessageExt messageExt, final int attempt,
                                               final SimpleFuture future0) {
        if (dropped) {
            log.info("Process queue was dropped, give up to forward message to DLQ, mq={}, messageId={}", mq,
                     messageExt.getMsgId());
            return;
        }
        final ScheduledExecutorService scheduler = consumerImpl.getScheduler();
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    forwardToDeadLetterQueue(messageExt, attempt, future0);
                }
            }, REDIRECT_FIFO_MESSAGE_TO_DLQ_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // should never reach here.
            log.error("[Bug] Failed to schedule DLQ message request.");
            forwardToDeadLetterQueueLater(messageExt, 1 + attempt, future0);
        }
    }

    public void ackMessage(final MessageExt messageExt) {
        final ListenableFuture<AckMessageResponse> future = consumerImpl.ackMessage(messageExt);
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to ack message, messageId={}, mq={}, code={}, status message=[{}]",
                              messageExt.getMsgId(), mq, code, status.getMessage());
                    return;
                }
                log.trace("Ack message successfully, messageId={}, mq={}", messageExt.getMsgId(), mq);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Exception raised while ack message, messageId={}, mq={}", messageExt.getMsgId(), mq, t);
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
        final Resource protoTopic = Resource.newBuilder().setArn(consumerImpl.getArn()).setName(mq.getTopic()).build();
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
