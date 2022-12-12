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

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.Status;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import net.javacrumbs.futureconverter.java8guava.FutureConverter;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.impl.Settings;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link SimpleConsumer}
 */
@SuppressWarnings("UnstableApiUsage")
class SimpleConsumerImpl extends ConsumerImpl implements SimpleConsumer {
    private static final Logger log = LoggerFactory.getLogger(SimpleConsumerImpl.class);

    private final SimpleSubscriptionSettings simpleSubscriptionSettings;
    private final String consumerGroup;
    private final Duration awaitDuration;

    private final AtomicInteger topicIndex;

    private final Map<String /* topic */, FilterExpression> subscriptionExpressions;
    private final ConcurrentMap<String /* topic */, SubscriptionLoadBalancer> subscriptionRouteDataCache;

    public SimpleConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup, Duration awaitDuration,
        Map<String, FilterExpression> subscriptionExpressions) {
        super(clientConfiguration, consumerGroup, subscriptionExpressions.keySet());
        Resource groupResource = new Resource(consumerGroup);
        this.simpleSubscriptionSettings = new SimpleSubscriptionSettings(clientId, endpoints,
            groupResource, clientConfiguration.getRequestTimeout(), awaitDuration, subscriptionExpressions);
        this.consumerGroup = consumerGroup;
        this.awaitDuration = awaitDuration;

        this.topicIndex = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));

        this.subscriptionExpressions = subscriptionExpressions;
        this.subscriptionRouteDataCache = new ConcurrentHashMap<>();
    }

    @Override
    protected void startUp() throws Exception {
        try {
            log.info("Begin to start the rocketmq simple consumer, clientId={}", clientId);
            super.startUp();
            log.info("The rocketmq simple consumer starts successfully, clientId={}", clientId);
        } catch (Throwable t) {
            log.error("Failed to start the rocketmq simple consumer, try to shutdown it, clientId={}", clientId, t);
            shutDown();
            throw t;
        }
    }

    @Override
    protected void shutDown() throws InterruptedException {
        log.info("Begin to shutdown the rocketmq simple consumer, clientId={}", clientId);
        super.shutDown();
        log.info("Shutdown the rocketmq simple consumer successfully, clientId={}", clientId);
    }

    /**
     * @see SimpleConsumer#getConsumerGroup()
     */
    @Override
    public String getConsumerGroup() {
        return consumerGroup;
    }

    /**
     * @see SimpleConsumer#subscribe(String, FilterExpression)
     */
    @Override
    public SimpleConsumer subscribe(String topic, FilterExpression filterExpression) throws ClientException {
        // Check consumer status.
        if (!this.isRunning()) {
            log.error("Unable to add subscription because simple consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Simple consumer is not running now");
        }
        final ListenableFuture<TopicRouteData> future = getRouteData(topic);
        handleClientFuture(future);
        subscriptionExpressions.put(topic, filterExpression);
        return this;
    }

    /**
     * @see SimpleConsumer#unsubscribe(String)
     */
    @Override
    public SimpleConsumer unsubscribe(String topic) {
        // Check consumer status.
        if (!this.isRunning()) {
            log.error("Unable to remove subscription because simple consumer is not running, state={}, "
                + "clientId={}", this.state(), clientId);
            throw new IllegalStateException("Simple consumer is not running now");
        }
        subscriptionExpressions.remove(topic);
        return this;
    }

    /**
     * @see SimpleConsumer#getSubscriptionExpressions()
     */
    @Override
    public Map<String, FilterExpression> getSubscriptionExpressions() {
        return new HashMap<>(subscriptionExpressions);
    }

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        return HeartbeatRequest.newBuilder().setGroup(getProtobufGroup())
            .setClientType(ClientType.SIMPLE_CONSUMER).build();
    }

    /**
     * @see SimpleConsumer#receive(int, Duration)
     */
    @Override
    public List<MessageView> receive(int maxMessageNum, Duration invisibleDuration) throws ClientException {
        final ListenableFuture<List<MessageView>> future = receive0(maxMessageNum, invisibleDuration);
        return handleClientFuture(future);
    }

    /**
     * @see SimpleConsumer#receiveAsync(int, Duration)
     */
    @Override
    public CompletableFuture<List<MessageView>> receiveAsync(int maxMessageNum, Duration invisibleDuration) {
        final ListenableFuture<List<MessageView>> future = receive0(maxMessageNum, invisibleDuration);
        return FutureConverter.toCompletableFuture(future);
    }

    public ListenableFuture<List<MessageView>> receive0(int maxMessageNum, Duration invisibleDuration) {
        if (!this.isRunning()) {
            log.error("Unable to receive message because simple consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            final IllegalStateException e = new IllegalStateException("Simple consumer is not running now");
            return Futures.immediateFailedFuture(e);
        }
        if (maxMessageNum <= 0) {
            final IllegalArgumentException e = new IllegalArgumentException("maxMessageNum must be greater than 0");
            return Futures.immediateFailedFuture(e);
        }
        final HashMap<String, FilterExpression> copy = new HashMap<>(subscriptionExpressions);
        final ArrayList<String> topics = new ArrayList<>(copy.keySet());
        // All topic is subscribed.
        if (topics.isEmpty()) {
            final IllegalArgumentException e = new IllegalArgumentException("There is no topic to receive message");
            return Futures.immediateFailedFuture(e);
        }
        final String topic = topics.get(IntMath.mod(topicIndex.getAndIncrement(), topics.size()));
        final FilterExpression filterExpression = copy.get(topic);
        final ListenableFuture<SubscriptionLoadBalancer> routeFuture = getSubscriptionLoadBalancer(topic);
        final ListenableFuture<ReceiveMessageResult> future0 = Futures.transformAsync(routeFuture, result -> {
            final MessageQueueImpl mq = result.takeMessageQueue();
            final ReceiveMessageRequest request = wrapReceiveMessageRequest(maxMessageNum, mq, filterExpression,
                invisibleDuration);
            return receiveMessage(request, mq, awaitDuration);
        }, MoreExecutors.directExecutor());
        return Futures.transformAsync(future0, result -> Futures.immediateFuture(result.getMessageViews()),
            clientCallbackExecutor);
    }

    /**
     * @see SimpleConsumer#ack(MessageView)
     */
    @Override
    public void ack(MessageView messageView) throws ClientException {
        final ListenableFuture<Void> future = ack0(messageView);
        handleClientFuture(future);
    }

    /**
     * @see SimpleConsumer#ackAsync(MessageView)
     */
    @Override
    public CompletableFuture<Void> ackAsync(MessageView messageView) {
        final ListenableFuture<Void> future = ack0(messageView);
        return FutureConverter.toCompletableFuture(future);
    }

    private ListenableFuture<Void> ack0(MessageView messageView) {
        // Check consumer status.
        if (!this.isRunning()) {
            log.error("Unable to ack message because simple consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            final IllegalStateException e = new IllegalStateException("Simple consumer is not running now");
            return Futures.immediateFailedFuture(e);
        }
        if (!(messageView instanceof MessageViewImpl)) {
            final IllegalArgumentException exception = new IllegalArgumentException("Failed downcasting for "
                + "messageView");
            return Futures.immediateFailedFuture(exception);
        }
        MessageViewImpl impl = (MessageViewImpl) messageView;
        final RpcFuture<AckMessageRequest, AckMessageResponse> future = ackMessage(impl);
        return Futures.transformAsync(future, response -> {
            final Status status = response.getStatus();
            StatusChecker.check(status, future);
            return Futures.immediateVoidFuture();
        }, clientCallbackExecutor);
    }

    /**
     * @see SimpleConsumer#changeInvisibleDuration(MessageView, Duration)
     */
    @Override
    public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration) throws ClientException {
        final ListenableFuture<Void> future = changeInvisibleDuration0(messageView, invisibleDuration);
        handleClientFuture(future);
    }

    /**
     * @see SimpleConsumer#changeInvisibleDurationAsync(MessageView, Duration)
     */
    @Override
    public CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView, Duration invisibleDuration) {
        final ListenableFuture<Void> future = changeInvisibleDuration0(messageView, invisibleDuration);
        return FutureConverter.toCompletableFuture(future);
    }

    public ListenableFuture<Void> changeInvisibleDuration0(MessageView messageView, Duration invisibleDuration) {
        // Check consumer status.
        if (!this.isRunning()) {
            log.error("Unable to change invisible duration because simple consumer is not running, state={}, "
                + "clientId={}", this.state(), clientId);
            final IllegalStateException e = new IllegalStateException("Simple consumer is not running now");
            return Futures.immediateFailedFuture(e);
        }
        if (!(messageView instanceof MessageViewImpl)) {
            final IllegalArgumentException exception = new IllegalArgumentException("Failed downcasting for "
                + "messageView");
            return Futures.immediateFailedFuture(exception);
        }
        MessageViewImpl impl = (MessageViewImpl) messageView;
        final RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> future =
            changeInvisibleDuration(impl, invisibleDuration);
        return Futures.transformAsync(future, response -> {
            // Refresh receipt handle manually.
            impl.setReceiptHandle(response.getReceiptHandle());
            final Status status = response.getStatus();
            StatusChecker.check(status, future);
            return Futures.immediateVoidFuture();
        }, MoreExecutors.directExecutor());
    }

    /**
     * @see SimpleConsumer#close()
     */
    @Override
    public void close() {
        this.stopAsync().awaitTerminated();
    }

    @Override
    public Settings getSettings() {
        return simpleSubscriptionSettings;
    }

    private SubscriptionLoadBalancer updateSubscriptionLoadBalancer(String topic, TopicRouteData topicRouteData) {
        SubscriptionLoadBalancer subscriptionLoadBalancer = subscriptionRouteDataCache.get(topic);
        subscriptionLoadBalancer = null == subscriptionLoadBalancer ? new SubscriptionLoadBalancer(topicRouteData) :
            subscriptionLoadBalancer.update(topicRouteData);
        subscriptionRouteDataCache.put(topic, subscriptionLoadBalancer);
        return subscriptionLoadBalancer;
    }

    @Override
    public void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData) {
        updateSubscriptionLoadBalancer(topic, topicRouteData);
    }

    private ListenableFuture<SubscriptionLoadBalancer> getSubscriptionLoadBalancer(final String topic) {
        final SubscriptionLoadBalancer loadBalancer = subscriptionRouteDataCache.get(topic);
        if (null != loadBalancer) {
            return Futures.immediateFuture(loadBalancer);
        }
        return Futures.transform(getRouteData(topic), topicRouteData -> updateSubscriptionLoadBalancer(topic,
            topicRouteData), MoreExecutors.directExecutor());
    }
}
