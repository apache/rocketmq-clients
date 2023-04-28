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

package org.apache.rocketmq.client.java.impl.consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.GetOffsetRequest;
import apache.rocketmq.v2.GetOffsetResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.QueryOffsetRequest;
import apache.rocketmq.v2.QueryOffsetResponse;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.UpdateOffsetRequest;
import apache.rocketmq.v2.UpdateOffsetResponse;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.TopicMessageQueueChangeListener;
import org.apache.rocketmq.client.apis.message.MessageQueue;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.impl.Settings;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.misc.CacheBlockingListQueue;
import org.apache.rocketmq.client.java.misc.ExcludeFromJacocoGeneratedReport;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"UnstableApiUsage", "NullableProblems", "UnusedReturnValue"})
public class PullConsumerImpl extends ConsumerImpl {
    private static final Logger log = LoggerFactory.getLogger(PullConsumerImpl.class);
    private static final Duration AUTO_COMMIT_DELAY = Duration.ofSeconds(1);

    final PullMessageQueuesScanner scanner;

    private final String consumerGroup;

    private final boolean autoCommitEnabled;

    private final Duration autoCommitInterval;

    private final PullSubscriptionSettings pullSubscriptionSettings;
    private final int maxCacheMessageCountTotalQueue;

    private final int maxCacheMessageCountEachQueue;

    private final int maxCacheMessageSizeInBytesTotalQueue;
    private final int maxCacheMessageSizeInBytesEachQueue;
    private volatile ConcurrentMap<MessageQueueImpl, FilterExpression> subscriptions;

    private final ConcurrentMap<MessageQueueImpl, PullProcessQueue> processQueueTable;
    private final ConcurrentMap<MessageQueueImpl, ReentrantReadWriteLock> processQueueLocks;
    private final CacheBlockingListQueue<PullProcessQueue, MessageViewImpl> blockingListQueue;

    private final AtomicLong pullTimes;
    private final AtomicLong pulledMessagesQuantity;

    private final ConcurrentMap<String, List<MessageQueueImpl>> topicMessageQueuesCache;
    private final ConcurrentMap<String, TopicMessageQueueChangeListener> topicMessageQueueChangeListenerMap;

    public PullConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup,
        boolean autoCommitEnabled, Duration autoCommitInterval, int maxCacheMessageCountTotalQueue,
        int maxCacheMessageCountEachQueue, int maxCacheMessageSizeInBytesTotalQueue,
        int maxCacheMessageSizeInBytesEachQueue) {
        super(clientConfiguration, consumerGroup, new HashSet<>());
        this.consumerGroup = consumerGroup;
        this.autoCommitEnabled = autoCommitEnabled;
        Resource groupResource = new Resource(consumerGroup);
        this.pullSubscriptionSettings = new PullSubscriptionSettings(clientId, endpoints, groupResource,
            clientConfiguration.getRequestTimeout());
        this.autoCommitInterval = autoCommitInterval;
        this.maxCacheMessageCountTotalQueue = maxCacheMessageCountTotalQueue;
        this.maxCacheMessageCountEachQueue = maxCacheMessageCountEachQueue;
        this.maxCacheMessageSizeInBytesTotalQueue = maxCacheMessageSizeInBytesTotalQueue;
        this.maxCacheMessageSizeInBytesEachQueue = maxCacheMessageSizeInBytesEachQueue;
        this.subscriptions = new ConcurrentHashMap<>();
        this.processQueueTable = new ConcurrentHashMap<>();
        this.processQueueLocks = new ConcurrentHashMap<>();
        this.blockingListQueue = new CacheBlockingListQueue<>();
        this.scanner = new PullMessageQueuesScanner(this);
        this.pullTimes = new AtomicLong(0);
        this.pulledMessagesQuantity = new AtomicLong(0);
        this.topicMessageQueuesCache = new ConcurrentHashMap<>();
        this.topicMessageQueueChangeListenerMap = new ConcurrentHashMap<>();
    }

    private ReadWriteLock getProcessQueueReadWriteLock(MessageQueueImpl mq) {
        return processQueueLocks.computeIfAbsent(mq, messageQueue -> new ReentrantReadWriteLock());
    }

    void cacheMessages(PullProcessQueue processQueue, List<MessageViewImpl> messageViews) {
        log.debug("Cached {} messages, mq={}, pq={}, clientId={}", messageViews.size(), processQueue.getMessageQueue(),
            processQueue, clientId);
        blockingListQueue.cache(processQueue, messageViews);
    }

    public List<MessageViewImpl> peekCachedMessages(PullProcessQueue processQueue) {
        return blockingListQueue.peek(processQueue);
    }

    Map<MessageQueueImpl, FilterExpression> getSubscriptions() {
        return subscriptions;
    }

    ConcurrentMap<MessageQueueImpl, PullProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    AtomicLong getPullTimes() {
        return pullTimes;
    }

    public AtomicLong getPulledMessagesQuantity() {
        return pulledMessagesQuantity;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void registerMessageQueueChangeListenerByTopic(String topic, TopicMessageQueueChangeListener listener)
        throws ClientException {
        checkNotNull(topic, "topic should not be null");
        checkNotNull(listener, "listener should not be null");
        if (!this.isRunning()) {
            log.error("Unable to register message queue listener because pull consumer is not running," +
                " state={}, clientId={}", this.state(), clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        if (topicMessageQueueChangeListenerMap.containsKey(topic)) {
            log.info("The listener of topic={} has been registered, the new listener will replace the existing one",
                topic);
        }
        topicMessageQueueChangeListenerMap.put(topic, listener);
        fetchMessageQueues(topic);
    }

    int getMaxCacheMessageCountEachQueue() {
        int size = processQueueTable.size();
        if (size < 1) {
            size = 1;
        }
        return Math.min(Math.max(1, maxCacheMessageCountTotalQueue / size), maxCacheMessageCountEachQueue);
    }

    int getMaxCacheMessageSizeInBytesEachQueue() {
        int size = processQueueTable.size();
        if (size < 1) {
            size = 1;
        }
        return Math.min(Math.max(1, maxCacheMessageSizeInBytesTotalQueue / size), maxCacheMessageSizeInBytesEachQueue);
    }

    public Collection<MessageQueue> fetchMessageQueues(String topic) throws ClientException {
        checkNotNull(topic, "topic should not be null");
        if (!this.isRunning()) {
            log.error("Unable to fetch message queue because pull consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        final ListenableFuture<TopicRouteData> future = fetchTopicRoute(topic);
        final TopicRouteData topicRouteData = handleClientFuture(future);
        final List<MessageQueueImpl> queues = transformTopicRouteData(topicRouteData);
        return new ArrayList<>(queues);
    }

    void tryPullMessageByMessageQueueImmediately(MessageQueueImpl mq, FilterExpression filterExpression) {
        final Optional<PullProcessQueue> pq = createProcessQueue(mq, filterExpression);
        pq.ifPresent(PullProcessQueue::pullMessageImmediately);
    }

    void tryPullMessageByMessageQueueImmediately(MessageQueueImpl mq, FilterExpression filterExpression, long offset) {
        final Optional<PullProcessQueue> pq = createProcessQueue(mq, filterExpression, offset);
        pq.ifPresent(PullProcessQueue::pullMessageImmediately);
    }

    Optional<PullProcessQueue> createProcessQueue(MessageQueueImpl mq, FilterExpression filterExpression) {
        final Lock lock = getProcessQueueReadWriteLock(mq).readLock();
        lock.lock();
        try {
            final PullProcessQueueImpl processQueue = new PullProcessQueueImpl(this, mq, filterExpression);
            final PullProcessQueue previous = processQueueTable.putIfAbsent(mq, processQueue);
            if (null != previous) {
                return Optional.empty();
            }
            return Optional.of(processQueue);
        } finally {
            lock.unlock();
        }
    }

    Optional<PullProcessQueue> createProcessQueue(MessageQueueImpl mq, FilterExpression filterExpression, long offset) {
        final Lock lock = getProcessQueueReadWriteLock(mq).readLock();
        lock.lock();
        try {
            final PullProcessQueueImpl processQueue = new PullProcessQueueImpl(this, mq, filterExpression,
                offset);
            final PullProcessQueue previous = processQueueTable.putIfAbsent(mq, processQueue);
            if (null != previous) {
                return Optional.empty();
            }
            return Optional.of(processQueue);
        } finally {
            lock.unlock();
        }
    }

    void dropProcessQueue(MessageQueueImpl mq) {
        final Lock lock = getProcessQueueReadWriteLock(mq).readLock();
        lock.lock();
        try {
            final PullProcessQueue pq = processQueueTable.remove(mq);
            if (null != pq) {
                pq.drop();
                blockingListQueue.drop(pq);
            }
        } finally {
            lock.unlock();
        }
    }

    public void assign(Collection<MessageQueue> mqs) {
        Map<MessageQueue, FilterExpression> subscriptions = new HashMap<>();
        for (MessageQueue mq : mqs) {
            subscriptions.put(mq, FilterExpression.SUB_ALL);
        }
        assign(subscriptions);
    }

    void assign(Map<MessageQueue, FilterExpression> subscriptions) {
        assign0(subscriptions);
        // Notify the scanner to scan the assigned queue immediately.
        scanner.signal();
    }

    /**
     * Replace the current subscriptions with the latest subscriptions.
     */
    void assign0(Map<MessageQueue, FilterExpression> subscriptions) {
        checkNotNull(subscriptions, "subscriptions to be assigned should not be null");
        checkArgument(!subscriptions.isEmpty(), "subscription should not be empty");
        if (!this.isRunning()) {
            log.error("Unable to assign subscription because pull consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        ConcurrentMap<MessageQueueImpl, FilterExpression> temp = new ConcurrentHashMap<>();
        for (Map.Entry<MessageQueue, FilterExpression> entry : subscriptions.entrySet()) {
            final MessageQueue mq = entry.getKey();
            final FilterExpression filterExpression = entry.getValue();
            temp.put((MessageQueueImpl) mq, filterExpression);
        }
        this.subscriptions = temp;
    }

    public List<MessageView> poll(Duration timeout) throws InterruptedException {
        checkNotNull(timeout, "timeout should not be null");
        if (!this.isRunning()) {
            log.error("Unable to pull message because pull consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        final Pair<PullProcessQueue, List<MessageViewImpl>> pair = blockingListQueue.poll(timeout);
        if (null == pair) {
            return new ArrayList<>();
        }
        // Update the latest consumed offset.
        final PullProcessQueue pq = pair.getKey();
        final List<MessageViewImpl> value = pair.getValue();
        final long offset = value.get(value.size() - 1).getOffset();
        pq.updateConsumedOffset(offset);
        return new ArrayList<>(value);
    }

    public void seek(MessageQueue messageQueue, long offset) {
        checkNotNull(messageQueue, "messageQueue should not be null");
        if (!this.isRunning()) {
            log.error("Unable to seek because pull consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        MessageQueueImpl mq = (MessageQueueImpl) messageQueue;
        if (!subscriptions.containsKey(mq)) {
            throw new IllegalArgumentException("The message queue is not contained in the assigned list");
        }
        seek0(mq, offset);
    }

    private void seek0(MessageQueueImpl mq, long offset) {
        final Lock lock = getProcessQueueReadWriteLock(mq).writeLock();
        lock.lock();
        try {
            final FilterExpression filterExpression = subscriptions.get(mq);
            if (null == filterExpression) {
                throw new IllegalArgumentException("The message queue is not contained in the assigned list");
            }
            log.info("Seek to the offset={}, mq={}, filterExpression={}, clientId={}", offset, mq, filterExpression,
                clientId);
            dropProcessQueue(mq);
            tryPullMessageByMessageQueueImmediately(mq, filterExpression, offset);
        } finally {
            lock.unlock();
        }
    }

    public void pause(Collection<MessageQueue> messageQueues) {
        checkNotNull(messageQueues, "messageQueues should not be null");
        checkArgument(!messageQueues.isEmpty(), "message queues should not be empty");
        if (!this.isRunning()) {
            log.error("Unable to pause because pull consumer is not running, state={}, clientId={}", this.state(),
                clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        for (MessageQueue mq : messageQueues) {
            final PullProcessQueue pq = processQueueTable.get((MessageQueueImpl) mq);
            if (null != pq) {
                pq.pause();
                log.info("Message queue is paused to pull, mq={}, clientId={}", mq, clientId);
            }
        }
    }

    public void resume(Collection<MessageQueue> messageQueues) {
        checkNotNull(messageQueues, "messageQueues should not be null");
        checkArgument(!messageQueues.isEmpty(), "message queues should not be null");
        if (!this.isRunning()) {
            log.error("Unable to resume because pull consumer is not running, state={}, clientId={}", this.state(),
                clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        for (MessageQueue mq : messageQueues) {
            final PullProcessQueue pq = processQueueTable.get((MessageQueueImpl) mq);
            pq.resume();
        }
    }

    public Optional<Long> offsetForTimestamp(MessageQueue messageQueue, long timestamp) throws ClientException {
        checkNotNull(messageQueue, "messageQueue should not be null");
        if (!this.isRunning()) {
            log.error("Unable to query offset because pull consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        final RpcFuture<QueryOffsetRequest, QueryOffsetResponse> future = queryOffset((MessageQueueImpl) messageQueue,
            timestamp);
        final QueryOffsetResponse response = handleClientFuture(future);
        final Status status = response.getStatus();
        StatusChecker.check(status, future);
        return Optional.of(response.getOffset());
    }

    public Optional<Long> committed(MessageQueue messageQueue) throws ClientException {
        checkNotNull(messageQueue, "messageQueue should not be null");
        if (!this.isRunning()) {
            log.error("Unable to query offset because pull consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        final RpcFuture<GetOffsetRequest, GetOffsetResponse> future = getOffset((MessageQueueImpl) messageQueue);
        final GetOffsetResponse response = handleClientFuture(future);
        final Status status = response.getStatus();
        StatusChecker.check(status, future);
        return Optional.of(response.getOffset());
    }

    public List<RpcFuture<UpdateOffsetRequest, UpdateOffsetResponse>> commit0() {
        if (!this.isRunning()) {
            log.error("Unable to commit offset because pull consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        List<RpcFuture<UpdateOffsetRequest, UpdateOffsetResponse>> futures = new ArrayList<>();
        for (Map.Entry<MessageQueueImpl, PullProcessQueue> entry : processQueueTable.entrySet()) {
            final MessageQueueImpl mq = entry.getKey();
            final PullProcessQueue pq = entry.getValue();
            final long offset = pq.getConsumedOffset();
            if (offset < 0) {
                continue;
            }
            final RpcFuture<UpdateOffsetRequest, UpdateOffsetResponse> future = updateOffset(mq, offset);
            Futures.addCallback(future, new FutureCallback<UpdateOffsetResponse>() {
                @Override
                public void onSuccess(UpdateOffsetResponse response) {
                    final Status status = response.getStatus();
                    final Code code = status.getCode();
                    if (Code.OK.equals(code)) {
                        log.info("Update offset successfully, mq={}, offset={}, clientId={}, consumerGroup={}", mq,
                            offset, clientId, consumerGroup);
                        return;
                    }
                    log.info("Failed to update offset, mq={}, offset={}, clientId={}, consumerGroup={}, code={}, " +
                        "status message=[{}]", mq, offset, clientId, consumerGroup, code, status.getMessage());
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Failed to update offset, mq={}, offset={}, clientId={}, consumerGroup={}", mq, offset,
                        clientId, consumerGroup, t);
                }
            }, MoreExecutors.directExecutor());
            futures.add(future);
        }
        return futures;
    }

    public void commit() throws ClientException {
        List<RpcFuture<UpdateOffsetRequest, UpdateOffsetResponse>> futures = commit0();
        final ListenableFuture<List<UpdateOffsetResponse>> future0 = Futures.allAsList(futures);
        final List<UpdateOffsetResponse> responses = handleClientFuture(future0);
        for (int i = 0; i < futures.size(); i++) {
            final RpcFuture<UpdateOffsetRequest, UpdateOffsetResponse> future = futures.get(i);
            final UpdateOffsetResponse response = responses.get(i);
            StatusChecker.check(response.getStatus(), future);
        }
    }

    public void seekToBegin(MessageQueue messageQueue) throws ClientException {
        checkNotNull(messageQueue, "messageQueue should not be null");
        if (!this.isRunning()) {
            log.error("Unable to seek because pull consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        MessageQueueImpl mq = (MessageQueueImpl) messageQueue;
        if (!subscriptions.containsKey(mq)) {
            throw new IllegalArgumentException("The message queue is not contained in the assigned list");
        }
        log.info("Seek to the beginning offset, mq={}, clientId={}", messageQueue, clientId);
        final RpcFuture<QueryOffsetRequest, QueryOffsetResponse> future = queryOffset(mq, OffsetPolicy.BEGINNING);
        final QueryOffsetResponse response = handleClientFuture(future);
        StatusChecker.check(response.getStatus(), future);
        seek0(mq, response.getOffset());
    }

    public void seekToEnd(MessageQueue messageQueue) throws ClientException {
        checkNotNull(messageQueue, "messageQueue should not be null");
        if (!this.isRunning()) {
            log.error("Unable to seek because pull consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Pull consumer is not running now");
        }
        MessageQueueImpl mq = (MessageQueueImpl) messageQueue;
        if (!subscriptions.containsKey(mq)) {
            throw new IllegalArgumentException("The message queue is not contained in the assigned list");
        }
        log.info("Seek to the end offset, mq={}, clientId={}", messageQueue, clientId);
        final RpcFuture<QueryOffsetRequest, QueryOffsetResponse> future = queryOffset(mq, OffsetPolicy.END);
        final QueryOffsetResponse response = handleClientFuture(future);
        StatusChecker.check(response.getStatus(), future);
        seek0(mq, response.getOffset());
    }

    @Override
    protected void startUp() throws Exception {
        log.info("Begin to start the rocketmq pull consumer, clientId={}", clientId);
        super.startUp();
        scanner.startUp();
        if (autoCommitEnabled) {
            this.getScheduler().scheduleWithFixedDelay(() -> {
                try {
                    commit();
                } catch (Throwable t) {
                    log.error("Failed to commit offset for pull consumer, clientId={}", clientId, t);
                }
            }, AUTO_COMMIT_DELAY.toNanos(), autoCommitInterval.toNanos(), TimeUnit.NANOSECONDS);
        }
        log.info("The rocketmq pull consumer starts successfully, clientId={}", clientId);
    }

    @Override
    protected void shutDown() throws InterruptedException {
        log.info("Begin to shutdown the rocketmq pull consumer, clientId={}", clientId);
        scanner.shutDown();
        super.shutDown();
        log.info("Shutdown the rocketmq pull consumer successfully, clientId={}", clientId);
    }

    public void close() {
        this.stopAsync().awaitTerminated();
    }

    @Override
    public Settings getSettings() {
        return pullSubscriptionSettings;
    }

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        return HeartbeatRequest.newBuilder().setGroup(getProtobufGroup())
            .setClientType(ClientType.PULL_CONSUMER).build();
    }

    private List<MessageQueueImpl> transformTopicRouteData(TopicRouteData topicRouteData) {
        return topicRouteData.getMessageQueues().stream()
            .filter((Predicate<MessageQueueImpl>) mq -> mq.getPermission().isReadable() &&
                Utilities.MASTER_BROKER_ID == mq.getBroker().getId())
            .collect(Collectors.toList());
    }

    public void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData) {
        final List<MessageQueueImpl> newMqs = transformTopicRouteData(topicRouteData);
        Set<MessageQueue> newMqSet = new HashSet<>(newMqs);
        synchronized (topicMessageQueuesCache) {
            final List<MessageQueueImpl> oldMqs = topicMessageQueuesCache.get(topic);
            Set<MessageQueue> oldMqSet = null == oldMqs ? null : new HashSet<>(oldMqs);
            if (!newMqSet.equals(oldMqSet)) {
                final TopicMessageQueueChangeListener listener = topicMessageQueueChangeListenerMap.get(topic);
                if (null != listener) {
                    listener.onChanged(topic, newMqSet);
                }
            }
            topicMessageQueuesCache.put(topic, newMqs);
        }
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    public void doStats() {
        final long pullTimes = this.pullTimes.getAndSet(0);
        final long pulledMessagesQuantity = this.pulledMessagesQuantity.getAndSet(0);
        log.info("clientId={}, consumerGroup={}, pullTimes={}, pulledMessageQuantity={}", clientId, consumerGroup,
            pullTimes, pulledMessagesQuantity);
        processQueueTable.values().forEach(ProcessQueue::doStats);
    }
}
