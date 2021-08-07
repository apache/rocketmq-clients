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

package org.apache.rocketmq.client.impl.producer;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.protobuf.util.Timestamps.fromMillis;

import apache.rocketmq.v1.ClientResourceBundle;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.ProducerGroup;
import apache.rocketmq.v1.ResolveOrphanedTransactionRequest;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Metadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.ClientImpl;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageAccessor;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageIdGenerator;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.protocol.Encoding;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;
import org.apache.rocketmq.client.misc.Validators;
import org.apache.rocketmq.client.producer.MessageGroupQueueSelector;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.Transaction;
import org.apache.rocketmq.client.producer.TransactionChecker;
import org.apache.rocketmq.client.producer.TransactionImpl;
import org.apache.rocketmq.client.producer.TransactionResolution;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.apache.rocketmq.utility.UtilAll;

@Slf4j
@Getter
@Setter
public class DefaultMQProducerImpl extends ClientImpl {

    public static final int MESSAGE_COMPRESSION_THRESHOLD = 1024 * 4;
    public static final int MESSAGE_COMPRESSION_LEVEL = 5;

    private int maxAttempts = 3;

    private long sendMessageTimeoutMillis = 10 * 1000;

    private long transactionResolveDelayMillis = 5 * 1000;

    @Getter(AccessLevel.NONE)
    private final ExecutorService defaultSendCallbackExecutor;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private ExecutorService customSendCallbackExecutor = null;

    @Getter(AccessLevel.NONE)
    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoCache;

    @Getter(AccessLevel.NONE)
    @GuardedBy("isolatedRouteEndpointsSetLock")
    private final Set<Endpoints> isolatedRouteEndpointsSet;

    @Getter(AccessLevel.NONE)
    private final ReadWriteLock isolatedRouteEndpointsSetLock;

    private TransactionChecker transactionChecker;

    @Getter(AccessLevel.NONE)
    private final ThreadPoolExecutor transactionCheckerExecutor;

    public DefaultMQProducerImpl(String group) {
        super(group);
        this.defaultSendCallbackExecutor = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("SendCallbackWorker"));

        this.topicPublishInfoCache = new ConcurrentHashMap<String, TopicPublishInfo>();

        this.isolatedRouteEndpointsSet = new HashSet<Endpoints>();
        this.isolatedRouteEndpointsSetLock = new ReentrantReadWriteLock();

        this.transactionCheckerExecutor = new ThreadPoolExecutor(
                1,
                1,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(128),
                new ThreadFactoryImpl("TransactionChecker"));
    }

    void ensureRunning() throws ClientException {
        if (ServiceState.STARTED != getState()) {
            throw new ClientException(ErrorCode.CLIENT_NOT_STARTED, "Please invoke #start() first!");
        }
    }

    /**
     * Start the rocketmq producer.
     *
     * @throws ClientException the mq client exception.
     */
    @Override
    public void start() throws ClientException {
        synchronized (this) {
            log.info("Begin to start the rocketmq producer.");
            super.start();

            if (ServiceState.STARTED == getState()) {
                log.info("The rocketmq producer starts successfully.");
            }
        }
    }

    /**
     * Shutdown the rocketmq producer.
     */
    @Override
    public void shutdown() {
        synchronized (this) {
            log.info("Begin to shutdown the rocketmq producer.");
            super.shutdown();

            if (ServiceState.STOPPED == getState()) {
                defaultSendCallbackExecutor.shutdown();
                log.info("Shutdown the rocketmq producer successfully.");
            }
        }
    }

    public void isolateEndpoints(Endpoints endpoints) {
        isolatedRouteEndpointsSetLock.writeLock().lock();
        try {
            isolatedRouteEndpointsSet.add(endpoints);
        } finally {
            isolatedRouteEndpointsSetLock.writeLock().unlock();
        }
    }

    @Override
    public void doHealthCheck() {
        final Set<Endpoints> routeEndpointsSet = getRouteEndpointsSet();
        final Sets.SetView<Endpoints> diff = Sets.difference(routeEndpointsSet, isolatedRouteEndpointsSet);
        final Set<Endpoints> aborted = new HashSet<Endpoints>(diff);
        // Remove all isolated endpoints where is not in the topic route.
        isolatedRouteEndpointsSetLock.writeLock().lock();
        try {
            isolatedRouteEndpointsSet.removeAll(aborted);
        } finally {
            isolatedRouteEndpointsSetLock.writeLock().unlock();
        }

        HealthCheckRequest request = HealthCheckRequest.newBuilder().build();
        isolatedRouteEndpointsSetLock.readLock().lock();
        try {
            for (final Endpoints endpoints : isolatedRouteEndpointsSet) {
                Metadata metadata;
                try {
                    metadata = sign();
                } catch (Throwable t) {
                    continue;
                }
                final ListenableFuture<HealthCheckResponse> future =
                        clientManager.healthCheck(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
                Futures.addCallback(future, new FutureCallback<HealthCheckResponse>() {
                    @Override
                    public void onSuccess(HealthCheckResponse response) {
                        final Status status = response.getCommon().getStatus();
                        final Code code = Code.forNumber(status.getCode());
                        // target endpoints is healthy, need to release it right now.
                        if (Code.OK == code) {
                            isolatedRouteEndpointsSetLock.writeLock().lock();
                            try {
                                isolatedRouteEndpointsSet.remove(endpoints);
                            } finally {
                                isolatedRouteEndpointsSetLock.writeLock().unlock();
                            }
                            log.info("Release isolated endpoints, clientId={}, endpoints={}", clientId, endpoints);
                            return;
                        }
                        log.warn("Failed to restore isolated endpoints, clientId={}, code={}, status message={}, "
                                 + "endpoints={}", clientId, code, status.getMessage(), endpoints);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.error("Failed to do health check, clientId={}, endpoints={}", clientId, endpoints, t);
                    }
                });
            }
        } finally {
            isolatedRouteEndpointsSetLock.readLock().unlock();
        }
    }

    /**
     * Allow to custom the executor to execute the {@link SendCallback}
     *
     * @param executor custom executor.
     */
    public void setCallbackExecutor(final ExecutorService executor) {
        checkNotNull(executor);
        this.customSendCallbackExecutor = executor;
    }

    public ExecutorService getSendCallbackExecutor() {
        if (null != customSendCallbackExecutor) {
            return customSendCallbackExecutor;
        }
        return defaultSendCallbackExecutor;
    }

    private SendMessageRequest wrapSendMessageRequest(Message message, Partition partition) {

        final Resource topicResource =
                Resource.newBuilder().setArn(arn).setName(message.getTopic()).build();

        final Resource groupResource =
                Resource.newBuilder().setArn(arn).setName(group).build();

        final apache.rocketmq.v1.SystemAttribute.Builder systemAttributeBuilder =
                apache.rocketmq.v1.SystemAttribute.newBuilder()
                                                  .setBornTimestamp(fromMillis(message.getBornTimeMillis()))
                                                  .setTag(message.getTag())
                                                  .addAllKeys(message.getKeysList())
                                                  .setProducerGroup(groupResource)
                                                  .setMessageId(message.getMessageExt().getMsgId())
                                                  .setBornHost(UtilAll.hostName())
                                                  .setPartitionId(partition.getId());
        Encoding encoding = Encoding.IDENTITY;
        byte[] body = message.getBody();
        if (body.length > MESSAGE_COMPRESSION_THRESHOLD) {
            try {
                body = UtilAll.compressBytesGzip(body, MESSAGE_COMPRESSION_LEVEL);
                encoding = Encoding.GZIP;
            } catch (IOException e) {
                log.warn("Failed to compress message", e);
            }
        }

        switch (encoding) {
            case GZIP:
                systemAttributeBuilder.setBodyEncoding(apache.rocketmq.v1.Encoding.GZIP);
                break;
            case SNAPPY:
                systemAttributeBuilder.setBodyEncoding(apache.rocketmq.v1.Encoding.SNAPPY);
                break;
            case IDENTITY:
            default:
                systemAttributeBuilder.setBodyEncoding(apache.rocketmq.v1.Encoding.IDENTITY);
        }

        final MessageImpl messageImpl = MessageAccessor.getMessageImpl(message);
        switch (messageImpl.getSystemAttribute().getMessageType()) {
            case FIFO:
                systemAttributeBuilder.setMessageType(apache.rocketmq.v1.MessageType.FIFO);
                // message group.
                final String messageGroup = message.getMessageGroup();
                if (null != messageGroup) {
                    systemAttributeBuilder.setMessageGroup(messageGroup);
                }
                break;
            case DELAY:
                systemAttributeBuilder.setMessageType(apache.rocketmq.v1.MessageType.DELAY);
                final int delayTimeLevel = message.getDelayTimeLevel();
                final long deliveryTimestamp = message.getDelayTimeMillis();
                if (delayTimeLevel > 0) {
                    systemAttributeBuilder.setDelayLevel(delayTimeLevel);
                } else if (deliveryTimestamp > 0) {
                    systemAttributeBuilder.setDeliveryTimestamp(fromMillis(deliveryTimestamp));
                }
                break;
            case TRANSACTION:
                systemAttributeBuilder.setMessageType(apache.rocketmq.v1.MessageType.TRANSACTION);
                break;
            default:
                systemAttributeBuilder.setMessageType(apache.rocketmq.v1.MessageType.NORMAL);
        }
        final apache.rocketmq.v1.SystemAttribute systemAttribute = systemAttributeBuilder.build();

        final apache.rocketmq.v1.Message msg =
                apache.rocketmq.v1.Message.newBuilder()
                                          .setTopic(topicResource)
                                          .setSystemAttribute(systemAttribute)
                                          .putAllUserAttribute(message.getUserProperties())
                                          .setBody(ByteString.copyFrom(body))
                                          .build();

        return SendMessageRequest.newBuilder().setMessage(msg).build();
    }

    public SendResult send(Message message)
            throws ClientException, InterruptedException, ServerException, TimeoutException {
        return send(message, sendMessageTimeoutMillis);
    }

    public SendResult send(Message message, long timeoutMillis)
            throws ClientException, InterruptedException, TimeoutException, ServerException {
        ensureRunning();
        final ListenableFuture<SendResult> future = send0(message, maxAttempts);
        // Limit the future timeout.
        Futures.withTimeout(future, timeoutMillis, TimeUnit.MILLISECONDS, this.getScheduler());
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw onExecutionException(e);
        }
    }

    public void send(Message message, SendCallback sendCallback)
            throws ClientException, InterruptedException {
        send(message, sendCallback, sendMessageTimeoutMillis);
    }

    public void send(Message message, final SendCallback sendCallback, long timeoutMillis)
            throws ClientException, InterruptedException {
        ensureRunning();
        final ListenableFuture<SendResult> future = send0(message, maxAttempts);
        // Limit the future timeout.
        Futures.withTimeout(future, timeoutMillis, TimeUnit.MILLISECONDS, this.getScheduler());
        final ExecutorService sendCallbackExecutor = getSendCallbackExecutor();
        Futures.addCallback(future, new FutureCallback<SendResult>() {
            @Override
            public void onSuccess(final SendResult sendResult) {
                try {
                    sendCallbackExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                sendCallback.onSuccess(sendResult);
                            } catch (Throwable t) {
                                log.error("Exception raised in SendCallback#onSuccess", t);
                            }
                        }
                    });
                } catch (Throwable t) {
                    log.error("Exception occurs while submitting task to send callback executor", t);
                }
            }

            @Override
            public void onFailure(final Throwable t) {
                try {
                    sendCallbackExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                sendCallback.onException(t);
                            } catch (Throwable t) {
                                log.error("Exception occurs in SendCallback#onException", t);
                            }
                        }
                    });
                } catch (Throwable t0) {
                    log.error("Exception occurs while submitting task to send callback executor", t0);
                }
            }
        });
    }

    public void sendOneway(Message message) throws ClientException {
        ensureRunning();
        send0(message, 1);
    }

    public SendResult send(Message message, String messageGroup) throws ServerException, ClientException,
                                                                        InterruptedException, TimeoutException {
        final MessageImpl messageImpl = MessageAccessor.getMessageImpl(message);
        messageImpl.getSystemAttribute().setMessageGroup(messageGroup);
        final MessageGroupQueueSelector selector = new MessageGroupQueueSelector(messageGroup);
        return send(message, selector, null);
    }

    public SendResult send(Message message, MessageQueueSelector selector, Object arg)
            throws ClientException, InterruptedException, ServerException, TimeoutException {
        return send(message, selector, arg, sendMessageTimeoutMillis);
    }

    public SendResult send(Message message, MessageQueueSelector selector, Object arg, long timeoutMillis)
            throws ClientException, ServerException, InterruptedException, TimeoutException {
        ensureRunning();
        final ListenableFuture<SendResult> future = send0(message, selector, arg, maxAttempts);
        Futures.withTimeout(future, timeoutMillis, TimeUnit.MILLISECONDS, this.getScheduler());
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw onExecutionException(e);
        }
    }

    public Transaction prepare(Message message) throws ServerException, InterruptedException,
                                                       ClientException, TimeoutException {
        // set message type as transaction.
        final MessageImpl messageImpl = MessageAccessor.getMessageImpl(message);
        final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
        systemAttribute.setMessageType(MessageType.TRANSACTION);
        // set transaction resolve delay millis.
        systemAttribute.setOrphanedTransactionRecoveryPeriodMillis(transactionResolveDelayMillis);

        final SendResult sendResult = send(message);
        return new TransactionImpl(sendResult, this);
    }

    public void commit(Endpoints endpoints, String messageId, String transactionId) throws ClientException,
                                                                                           ServerException,
                                                                                           InterruptedException,
                                                                                           TimeoutException {
        endTransaction(endpoints, messageId, transactionId, TransactionResolution.COMMIT);
    }

    public void rollback(Endpoints endpoints, String messageId, String transactionId) throws ClientException,
                                                                                             ServerException,
                                                                                             InterruptedException,
                                                                                             TimeoutException {
        endTransaction(endpoints, messageId, transactionId, TransactionResolution.ROLLBACK);
    }

    private void endTransaction(Endpoints endpoints, String messageId, String transactionId,
                                TransactionResolution resolution) throws ClientException, ServerException,
                                                                         InterruptedException, TimeoutException {
        final EndTransactionRequest.Builder builder =
                EndTransactionRequest.newBuilder().setMessageId(messageId).setTransactionId(transactionId);
        switch (resolution) {
            case COMMIT:
                builder.setResolution(EndTransactionRequest.TransactionResolution.COMMIT);
                break;
            case ROLLBACK:
            default:
                builder.setResolution(EndTransactionRequest.TransactionResolution.ROLLBACK);
        }
        final EndTransactionRequest request = builder.build();
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            throw new ClientException(ErrorCode.SIGNATURE_FAILURE);
        }
        final ListenableFuture<EndTransactionResponse> future =
                clientManager.endTransaction(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        try {
            final EndTransactionResponse response = future.get(ioTimeoutMillis, TimeUnit.MILLISECONDS);
            final Status status = response.getCommon().getStatus();
            final Code code = Code.forNumber(status.getCode());

            if (Code.OK != code) {
                log.error("Failed to end transaction, messageId={}, transactionId={}, code={}, status message={}",
                          messageId, transactionId, code, status.getMessage());
                throw new ServerException(status.getMessage());
            }

        } catch (ExecutionException e) {
            throw onExecutionException(e);
        }
    }

    @Override
    public void resolveOrphanedTransaction(final Endpoints endpoints, ResolveOrphanedTransactionRequest request) {
        final apache.rocketmq.v1.Message message = request.getOrphanedTransactionalMessage();
        final String messageId = message.getSystemAttribute().getMessageId();
        if (null == transactionChecker) {
            log.error("No transaction checker registered, ignore it, messageId={}", messageId);
            return;
        }
        MessageImpl messageImpl;
        try {
            messageImpl = wrapMessageImpl(message);
        } catch (Throwable t) {
            log.error("Failed to decode message, ignore it, messageId={}", messageId);
            return;
        }
        final MessageExt messageExt = new MessageExt(messageImpl);
        final String transactionId = request.getTransactionId();
        try {
            transactionCheckerExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        final TransactionResolution resolution = transactionChecker.check(messageExt);
                        if (null == resolution || TransactionResolution.UNKNOWN.equals(resolution)) {
                            return;
                        }
                        endTransaction(endpoints, messageId, transactionId, resolution);
                    } catch (Throwable t) {
                        log.error("Exception raised while check and end transaction, messageId={}, transactionId={}, "
                                  + "endpoints={}", messageId, transactionId, endpoints, t);
                    }
                }
            });
        } catch (Throwable t) {
            log.error("Failed to submit task for check and end transaction, messageId={}, transactionId={}",
                      messageId, transactionId, t);
        }
    }

    @Override
    public void beforeTopicRouteDataUpdate(String topic, TopicRouteData topicRouteData) {
        topicPublishInfoCache.put(topic, new TopicPublishInfo(topicRouteData));
    }

    private ListenableFuture<TopicPublishInfo> getPublishInfo(final String topic) {
        SettableFuture<TopicPublishInfo> future0 = SettableFuture.create();
        final TopicPublishInfo cachedTopicPublishInfo = topicPublishInfoCache.get(topic);
        if (null != cachedTopicPublishInfo) {
            future0.set(cachedTopicPublishInfo);
            return future0;
        }
        final ListenableFuture<TopicRouteData> future = getRouteFor(topic);
        return Futures.transform(future, new Function<TopicRouteData, TopicPublishInfo>() {
            @Override
            public TopicPublishInfo apply(TopicRouteData topicRouteData) {
                final TopicPublishInfo topicPublishInfo = new TopicPublishInfo(topicRouteData);
                topicPublishInfoCache.put(topic, topicPublishInfo);
                return topicPublishInfo;
            }
        });
    }

    private ListenableFuture<SendResult> send0(final Message message, final int maxAttempts) {
        final ListenableFuture<TopicPublishInfo> future = getPublishInfo(message.getTopic());
        return Futures.transformAsync(future, new AsyncFunction<TopicPublishInfo, SendResult>() {
            @Override
            public ListenableFuture<SendResult> apply(TopicPublishInfo topicPublishInfo) throws ClientException {
                // Prepare the candidate partitions for retry-sending in advance.
                final List<Partition> candidates = takePartitionsRoundRobin(topicPublishInfo, maxAttempts);
                return send0(message, candidates, maxAttempts);
            }
        });
    }

    private ListenableFuture<SendResult> send0(final Message message, MessageQueueSelector selector, Object arg,
                                               final int maxAttempts) {
        // set message type as fifo.
        final MessageImpl messageImpl = MessageAccessor.getMessageImpl(message);
        final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
        systemAttribute.setMessageType(MessageType.FIFO);

        final ListenableFuture<Partition> future0 = selectPartition(message, selector, arg);
        return Futures.transformAsync(future0, new AsyncFunction<Partition, SendResult>() {
            @Override
            public ListenableFuture<SendResult> apply(Partition partition) {
                List<Partition> candidates = new ArrayList<Partition>();
                candidates.add(partition);
                return send0(message, candidates, maxAttempts);
            }
        });
    }

    private ListenableFuture<SendResult> send0(final Message message, final List<Partition> candidates,
                                               int maxAttempts) {
        final SettableFuture<SendResult> future = SettableFuture.create();
        // filter illegal message.
        try {
            Validators.checkMessage(message);
        } catch (ClientException e) {
            future.setException(e);
            return future;
        }

        // set message id, if user send message to different topic, they should have different message id.
        final MessageImpl messageImpl = MessageAccessor.getMessageImpl(message);
        final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
        final String messageId = MessageIdGenerator.getInstance().next();
        systemAttribute.setMessageId(messageId);

        // check if it is delay message or not.
        final int delayTimeLevel = message.getDelayTimeLevel();
        final long deliveryTimestamp = message.getDelayTimeMillis();
        if (delayTimeLevel > 0 || deliveryTimestamp > 0) {
            systemAttribute.setMessageType(MessageType.DELAY);
        }

        send0(future, candidates, message, 1, maxAttempts);
        return future;
    }

    private void send0(final SettableFuture<SendResult> future, final List<Partition> candidates,
                       final Message message, final int attempt, final int maxAttempts) {
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            // failed to sign, return in advance.
            future.setException(t);
            return;
        }

        // calculate the current partition.
        final Partition partition = candidates.get(UtilAll.positiveMod(attempt - 1, candidates.size()));
        final Endpoints endpoints = partition.getBroker().getEndpoints();

        final SendMessageRequest request = wrapSendMessageRequest(message, partition);
        final Stopwatch stopwatch = Stopwatch.createStarted();

        // intercept before message sending.
        final MessageInterceptorContext.MessageInterceptorContextBuilder contextBuilder =
                MessageInterceptorContext.builder().attempt(attempt);
        intercept(MessageHookPoint.PRE_SEND_MESSAGE, message.getMessageExt(), contextBuilder.build());

        final ListenableFuture<SendMessageResponse> responseFuture =
                clientManager.sendMessage(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);

        // return the future of send result for current attempt.
        final ListenableFuture<SendResult> attemptFuture = Futures.transformAsync(
                responseFuture, new AsyncFunction<SendMessageResponse, SendResult>() {
                    @Override
                    public ListenableFuture<SendResult> apply(SendMessageResponse response) throws Exception {
                        final SettableFuture<SendResult> future0 = SettableFuture.create();
                        final SendResult sendResult = processSendResponse(endpoints, response);
                        future0.set(sendResult);
                        return future0;
                    }
                });

        final String topic = message.getTopic();
        final String msgId = message.getMsgId();
        Futures.addCallback(attemptFuture, new FutureCallback<SendResult>() {
            @Override
            public void onSuccess(SendResult sendResult) {
                // no need more attempts.
                future.set(sendResult);
                log.trace("Send message successfully, topic={}, msgId={}, maxAttempts={}, attempt={}", topic, msgId,
                          maxAttempts, attempt);
                // intercept after message sending.
                final long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                final MessageInterceptorContext context = contextBuilder.duration(duration)
                                                                        .timeUnit(TimeUnit.MILLISECONDS)
                                                                        .status(MessageHookPoint.PointStatus.OK)
                                                                        .build();
                intercept(MessageHookPoint.POST_SEND_MESSAGE, message.getMessageExt(), context);
            }

            @Override
            public void onFailure(Throwable t) {
                // intercept after message sending.
                final long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                final MessageInterceptorContext context = contextBuilder.duration(duration)
                                                                        .timeUnit(TimeUnit.MILLISECONDS)
                                                                        .status(MessageHookPoint.PointStatus.ERROR)
                                                                        .build();
                intercept(MessageHookPoint.POST_SEND_MESSAGE, message.getMessageExt(), context);
                // isolate endpoints for a while.
                isolateEndpoints(endpoints);

                if (attempt >= maxAttempts) {
                    // no need more attempts.
                    future.setException(t);
                    log.error("Failed to send message, attempt times is exhausted, maxAttempts={}, attempt={}, "
                              + "topic={}, messageId={}", maxAttempts, attempt, topic, msgId, t);
                    return;
                }
                // try to do more attempts.
                log.warn("Failed to send message, would attempt to re-send right now, maxAttempts={}, "
                         + "attempt={}, topic={}, messageId={}", maxAttempts, attempt, topic, msgId, t);
                send0(future, candidates, message, 1 + attempt, maxAttempts);
            }
        });
    }

    List<Partition> takePartitionsRoundRobin(TopicPublishInfo topicPublishInfo, int maxAttempts)
            throws ClientException {
        Set<Endpoints> isolated = new HashSet<Endpoints>();
        isolatedRouteEndpointsSetLock.readLock().lock();
        try {
            isolated.addAll(isolatedRouteEndpointsSet);
        } finally {
            isolatedRouteEndpointsSetLock.readLock().unlock();
        }
        return topicPublishInfo.takePartitions(isolated, maxAttempts);
    }

    private ListenableFuture<Partition> selectPartition(final Message message, final MessageQueueSelector selector,
                                                        final Object arg) {
        final String topic = message.getTopic();
        final ListenableFuture<TopicPublishInfo> future = getPublishInfo(topic);
        return Futures.transformAsync(future, new AsyncFunction<TopicPublishInfo, Partition>() {
            @Override
            public ListenableFuture<Partition> apply(TopicPublishInfo topicPublishInfo) throws ClientException {
                if (topicPublishInfo.isEmpty()) {
                    log.warn("No available partition for selector, topic={}", topic);
                    throw new ClientException(ErrorCode.NO_PERMISSION);
                }
                final MessageQueue mq = selector.select(topicPublishInfo.getMessageQueues(), message, arg);
                final SettableFuture<Partition> future0 = SettableFuture.create();
                future0.set(mq.getPartition());
                return future0;
            }
        });
    }

    @Override
    public HeartbeatEntry prepareHeartbeatData() {
        ProducerGroup producerGroup = ProducerGroup.newBuilder().setGroup(getGroupResource()).build();
        return HeartbeatEntry.newBuilder()
                             .setClientId(clientId)
                             .setProducerGroup(producerGroup)
                             .build();
    }

    @Override
    public void doStats() {
    }

    @Override
    public ClientResourceBundle wrapClientResourceBundle() {
        final ClientResourceBundle.Builder builder =
                ClientResourceBundle.newBuilder().setClientId(clientId).setProducerGroup(getGroupResource());
        for (String topic : topicPublishInfoCache.keySet()) {
            Resource topicResource = Resource.newBuilder().setArn(arn).setName(topic).build();
            builder.addTopics(topicResource);
        }
        return builder.build();
    }


    public static SendResult processSendResponse(Endpoints endpoints, SendMessageResponse response)
            throws ServerException {
        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        if (Code.OK == code) {
            return new SendResult(endpoints, response.getMessageId(), response.getTransactionId());
        }
        log.debug("Response indicates failure of sending message, information={}", status.getMessage());
        throw new ServerException(ErrorCode.OTHER, status.getMessage());
    }

    public ClientException onExecutionException(ExecutionException e) throws ServerException {
        final Throwable cause = e.getCause();
        if (cause instanceof ClientException) {
            return (ClientException) cause;
        }
        if (cause instanceof ServerException) {
            throw (ServerException) cause;
        }
        return new ClientException(ErrorCode.OTHER, e);
    }

    public void setTransactionChecker(final TransactionChecker checker) {
        checkNotNull(checker);
        this.transactionChecker = checker;
    }
}
