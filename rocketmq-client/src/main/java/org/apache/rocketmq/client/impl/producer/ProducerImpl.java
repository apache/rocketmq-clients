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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.protobuf.util.Timestamps.fromMillis;

import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.GenericPollingRequest;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.ProducerData;
import apache.rocketmq.v1.RecoverOrphanedTransactionRequest;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.ClientImpl;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageHookPointStatus;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageImplAccessor;
import org.apache.rocketmq.client.message.MessageInterceptor;
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
import org.apache.rocketmq.utility.ExecutorServices;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.apache.rocketmq.utility.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings(value = {"UnstableApiUsage", "NullableProblems"})
public class ProducerImpl extends ClientImpl {
    /**
     * If message body size exceeds the threshold, it would be compressed for convenience of transport.
     */
    public static final int MESSAGE_COMPRESSION_THRESHOLD = 1024 * 4;

    /**
     * The default GZIP compression level for message body.
     */
    public static final int MESSAGE_COMPRESSION_LEVEL = 5;

    private static final Logger log = LoggerFactory.getLogger(ProducerImpl.class);

    /**
     * Maximum attempt times for auto-retry of sending message.
     */
    private int maxAttempts = 3;

    /**
     * Sending message timeout, including the auto-retry.
     */
    private long sendMessageTimeoutMillis = 5 * 1000;

    /**
     * It indicates the delay time of server check before the transaction message is committed/rollback.
     */
    private long transactionResolveDelayMillis = 5 * 1000;

    private TransactionChecker transactionChecker;

    /**
     * Default callback executor for asynchronous sending, if {@link #customSendCallbackExecutor} is not defined,
     * default executor would be applied for asynchronous message sending.
     */
    private final ExecutorService defaultSendCallbackExecutor;

    /**
     * Custom callback executor for asynchronous sending, it is set, {@link #defaultSendCallbackExecutor} would not
     * be applied.
     */
    private ExecutorService customSendCallbackExecutor = null;

    private final ConcurrentMap<String/* topic */, SendingTopicRouteData> sendingRouteDataCache;

    @GuardedBy("isolatedEndpointsSetLock")
    private final Set<Endpoints> isolatedEndpointsSet;
    private final ReadWriteLock isolatedEndpointsSetLock;

    private final ThreadPoolExecutor transactionCheckerExecutor;

    public ProducerImpl(String group) throws ClientException {
        super(group);
        this.defaultSendCallbackExecutor = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("SendCallbackWorker"));

        this.sendingRouteDataCache = new ConcurrentHashMap<String, SendingTopicRouteData>();

        this.isolatedEndpointsSet = new HashSet<Endpoints>();
        this.isolatedEndpointsSetLock = new ReentrantReadWriteLock();

        this.transactionCheckerExecutor = new ThreadPoolExecutor(
                1,
                1,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(128),
                new ThreadFactoryImpl("TransactionChecker"));
    }

    private void preconditionCheck(Message message) throws ClientException {
        if (!clientService.isRunning()) {
            throw new ClientException(ErrorCode.NOT_STARTED, "Please invoke #start() first!");
        }
        Validators.checkMessage(message);
    }

    /**
     * Start the rocketmq producer.
     *
     * @throws ClientException the mq client exception.
     */
    @Override
    protected void setUp() throws ClientException {
        log.info("Begin to start the rocketmq producer, clientId={}", id);
        super.setUp();
        log.info("The rocketmq producer starts successfully, clientId={}", id);
    }

    /**
     * Shutdown the rocketmq producer.
     */
    @Override
    protected void tearDown() throws InterruptedException {
        log.info("Begin to shutdown the rocketmq producer, clientId={}", id);
        super.tearDown();
        defaultSendCallbackExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(defaultSendCallbackExecutor)) {
            log.error("[Bug] Failed to shutdown default send callback executor, clientId={}", id);
        }
        log.info("Shutdown the rocketmq producer successfully, clientId={}", id);
    }

    public void start() {
        clientService.startAsync().awaitRunning();
    }

    public void shutdown() {
        clientService.stopAsync().awaitTerminated();
    }

    public void isolateEndpoints(Endpoints endpoints) {
        isolatedEndpointsSetLock.writeLock().lock();
        try {
            isolatedEndpointsSet.add(endpoints);
        } finally {
            isolatedEndpointsSetLock.writeLock().unlock();
        }
    }

    /**
     * Check the status of isolated {@link Endpoints}, rejoin it if it is healthy.
     */
    @Override
    public void doHealthCheck() {
        final Set<Endpoints> routeEndpointsSet = getRouteEndpointsSet();
        final Set<Endpoints> expired = new HashSet<Endpoints>(Sets.difference(routeEndpointsSet, isolatedEndpointsSet));
        // remove all isolated endpoints which is expired.
        isolatedEndpointsSetLock.writeLock().lock();
        try {
            isolatedEndpointsSet.removeAll(expired);
        } finally {
            isolatedEndpointsSetLock.writeLock().unlock();
        }

        HealthCheckRequest request = HealthCheckRequest.newBuilder().build();
        isolatedEndpointsSetLock.readLock().lock();
        try {
            for (final Endpoints endpoints : isolatedEndpointsSet) {
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
                        // target endpoints is healthy, rejoin it.
                        if (Code.OK == code) {
                            isolatedEndpointsSetLock.writeLock().lock();
                            try {
                                isolatedEndpointsSet.remove(endpoints);
                            } finally {
                                isolatedEndpointsSetLock.writeLock().unlock();
                            }
                            log.info("Rejoin endpoints which is isolated before, clientId={}, endpoints={}", id,
                                     endpoints);
                            return;
                        }
                        log.warn("Failed to rejoin the endpoints which is isolated before, clientId={}, code={}, "
                                 + "status message=[{}], endpoints={}", id, code, status.getMessage(), endpoints);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.error("Failed to do health check, clientId={}, endpoints={}", id, endpoints, t);
                    }
                });
            }
        } finally {
            isolatedEndpointsSetLock.readLock().unlock();
        }
    }

    /**
     * Allow to custom the executor to execute the {@link SendCallback}
     *
     * @param executor custom executor.
     */
    public void setCallbackExecutor(final ExecutorService executor) {
        this.customSendCallbackExecutor = checkNotNull(executor, "executor");
    }

    public ExecutorService getSendCallbackExecutor() {
        if (null != customSendCallbackExecutor) {
            return customSendCallbackExecutor;
        }
        return defaultSendCallbackExecutor;
    }

    private SendMessageRequest wrapSendMessageRequest(Message message, Partition partition) {
        final Resource topicResource =
                Resource.newBuilder().setResourceNamespace(namespace).setName(message.getTopic()).build();

        final apache.rocketmq.v1.SystemAttribute.Builder systemAttributeBuilder =
                apache.rocketmq.v1.SystemAttribute.newBuilder().setTag(message.getTag())
                                                  .addAllKeys(message.getKeysList())
                                                  .setMessageId(message.getMessageExt().getMsgId())
                                                  .setBornTimestamp(fromMillis(message.getBornTimeMillis()))
                                                  .setBornHost(message.getBornHost()).setPartitionId(partition.getId())
                                                  .setProducerGroup(getPbGroup());
        Encoding encoding = Encoding.IDENTITY;
        byte[] body = message.getBody();
        if (body.length > MESSAGE_COMPRESSION_THRESHOLD) {
            try {
                body = UtilAll.compressBytesGzip(body, MESSAGE_COMPRESSION_LEVEL);
                encoding = Encoding.GZIP;
            } catch (IOException e) {
                log.warn("Failed to compress message, messageId={}", message.getMsgId(), e);
            }
        }
        switch (encoding) {
            case GZIP:
                systemAttributeBuilder.setBodyEncoding(apache.rocketmq.v1.Encoding.GZIP);
                break;
            case IDENTITY:
            default:
                systemAttributeBuilder.setBodyEncoding(apache.rocketmq.v1.Encoding.IDENTITY);
        }
        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(message);
        // set trace context.
        final String traceContext = messageImpl.getSystemAttribute().getTraceContext();
        if (null != traceContext) {
            systemAttributeBuilder.setTraceContext(traceContext);
        }
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
                apache.rocketmq.v1.Message.newBuilder().setTopic(topicResource).setSystemAttribute(systemAttribute)
                                          .putAllUserAttribute(message.getUserProperties())
                                          .setBody(ByteString.copyFrom(body)).build();

        return SendMessageRequest.newBuilder().setMessage(msg).build();
    }

    public SendResult send(Message message) throws ClientException, InterruptedException, ServerException,
                                                   TimeoutException {
        return send(message, sendMessageTimeoutMillis);
    }

    public SendResult send(Message message, long timeoutMillis)
            throws ClientException, InterruptedException, TimeoutException, ServerException {
        preconditionCheck(message);
        final ListenableFuture<SendResult> future = send0(message, maxAttempts);
        // limit the future timeout.
        Futures.withTimeout(future, timeoutMillis, TimeUnit.MILLISECONDS, this.getScheduler());
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw onExecutionException(e);
        }
    }

    public void send(Message message, SendCallback sendCallback) throws ClientException, InterruptedException {
        send(message, sendCallback, sendMessageTimeoutMillis);
    }

    public void send(Message message, final SendCallback sendCallback, long timeoutMillis)
            throws ClientException, InterruptedException {
        preconditionCheck(message);
        final ListenableFuture<SendResult> future = send0(message, maxAttempts);
        // limit the future timeout.
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
        preconditionCheck(message);
        send0(message, 1);
    }

    public SendResult send(Message message, String messageGroup) throws ServerException, ClientException,
                                                                        InterruptedException, TimeoutException {
        if (StringUtils.isBlank(messageGroup)) {
            throw new ClientException(ErrorCode.ILLEGAL_FORMAT, "message group is blank");
        }
        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(message);
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
        preconditionCheck(message);
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
        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(message);
        final SystemAttribute systemAttribute = messageImpl.getSystemAttribute();
        systemAttribute.setMessageType(MessageType.TRANSACTION);
        // set transaction resolve delay millis.
        systemAttribute.setOrphanedTransactionRecoveryPeriodMillis(transactionResolveDelayMillis);

        final SendResult sendResult = send(message);
        return new TransactionImpl(sendResult, message, this);
    }

    public void commit(Endpoints endpoints, MessageExt messageExt, String transactionId) throws ClientException,
                                                                                                ServerException,
                                                                                                InterruptedException,
                                                                                                TimeoutException {
        endTransaction(endpoints, messageExt, transactionId, TransactionResolution.COMMIT);
    }

    public void rollback(Endpoints endpoints, MessageExt messageExt, String transactionId) throws ClientException,
                                                                                                  ServerException,
                                                                                                  InterruptedException,
                                                                                                  TimeoutException {
        endTransaction(endpoints, messageExt, transactionId, TransactionResolution.ROLLBACK);
    }

    private void endTransaction(Endpoints endpoints, final MessageExt messageExt, String transactionId,
                                final TransactionResolution resolution) throws ClientException, ServerException,
                                                                               InterruptedException, TimeoutException {
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            throw new ClientException(ErrorCode.SIGNATURE_FAILURE, t);
        }
        final String messageId = messageExt.getMsgId();
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
        final MessageHookPoint preHookPoint = TransactionResolution.COMMIT.equals(resolution) ?
                                              MessageHookPoint.PRE_COMMIT_MESSAGE :
                                              MessageHookPoint.PRE_ROLLBACK_MESSAGE;
        final MessageHookPoint postHookPoint = TransactionResolution.COMMIT.equals(resolution) ?
                                               MessageHookPoint.POST_COMMIT_MESSAGE :
                                               MessageHookPoint.POST_ROLLBACK_MESSAGE;

        final String topic = messageExt.getTopic();
        // intercept before commit/rollback message.
        final MessageInterceptorContext preContext = MessageInterceptorContext.builder().setTopic(topic).build();
        intercept(preHookPoint, messageExt, preContext);
        final Stopwatch stopwatch = Stopwatch.createStarted();

        final ListenableFuture<EndTransactionResponse> future =
                clientManager.endTransaction(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);

        Futures.addCallback(future, new FutureCallback<EndTransactionResponse>() {
            @Override
            public void onSuccess(EndTransactionResponse response) {
                final Code code = Code.forNumber(response.getCommon().getStatus().getCode());
                // intercept after commit/rollback message.
                MessageHookPointStatus status = Code.OK.equals(code) ? MessageHookPointStatus.OK
                                                                     : MessageHookPointStatus.ERROR;
                final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                final MessageInterceptorContext postContext = preContext.toBuilder().setDuration(duration)
                                                                        .setStatus(status).build();
                intercept(postHookPoint, messageExt, postContext);
            }

            @Override
            public void onFailure(Throwable t) {
                // intercept after commit/rollback message.
                final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                final MessageInterceptorContext postContext = preContext.toBuilder().setDuration(duration)
                                                                        .setStatus(MessageHookPointStatus.ERROR)
                                                                        .setThrowable(t).build();
                intercept(postHookPoint, messageExt, postContext);
            }
        });
        try {
            final EndTransactionResponse response = future.get(ioTimeoutMillis, TimeUnit.MILLISECONDS);
            final Status status = response.getCommon().getStatus();
            final Code code = Code.forNumber(status.getCode());

            if (Code.OK != code) {
                log.error("Failed to end transaction, namespace={}, topic={}, messageId={}, transactionId={}, "
                          + "resolution={}, code={}, status message=[{}]", namespace, topic, messageId, transactionId,
                          resolution, code, status.getMessage());

                throw new ServerException(ErrorCode.OTHER, status.getMessage());
            }
        } catch (ExecutionException e) {
            throw onExecutionException(e);
        }
    }

    @Override
    public void recoverOrphanedTransaction(final Endpoints endpoints, final RecoverOrphanedTransactionRequest request) {
        final apache.rocketmq.v1.Message message = request.getOrphanedTransactionalMessage();
        final String messageId = message.getSystemAttribute().getMessageId();
        if (null == transactionChecker) {
            log.error("No transaction checker registered, ignore it, messageId={}", messageId);
            return;
        }
        final MessageExt messageExt;
        try {
            MessageImpl messageImpl = MessageImplAccessor.wrapMessageImpl(message);
            messageExt = new MessageExt(messageImpl);
        } catch (Throwable t) {
            log.error("[Bug] Failed to decode message while resolving orphaned transaction, messageId={}", messageId,
                      t);
            return;
        }
        try {
            transactionCheckerExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        final TransactionResolution resolution = transactionChecker.check(messageExt);
                        if (null == resolution || TransactionResolution.UNKNOWN.equals(resolution)) {
                            return;
                        }
                        endTransaction(endpoints, messageExt, request.getTransactionId(), resolution);
                    } catch (Throwable t) {
                        log.error("Exception raised while check and end transaction, messageId={}, transactionId={}, "
                                  + "endpoints={}", messageId, request.getTransactionId(), endpoints, t);
                    }
                }
            });
        } catch (Throwable t) {
            log.error("Failed to submit task for check and end transaction, messageId={}, transactionId={}",
                      messageId, request.getTransactionId(), t);
        }
    }

    @Override
    public void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData) {
        sendingRouteDataCache.put(topic, new SendingTopicRouteData(topicRouteData));
    }

    private ListenableFuture<SendingTopicRouteData> getSendingTopicRouteData(final String topic) {
        SettableFuture<SendingTopicRouteData> future0 = SettableFuture.create();
        final SendingTopicRouteData cachedSendingRouteData = sendingRouteDataCache.get(topic);
        if (null != cachedSendingRouteData) {
            future0.set(cachedSendingRouteData);
            return future0;
        }
        final ListenableFuture<TopicRouteData> future = getRouteData(topic);
        return Futures.transform(future, new Function<TopicRouteData, SendingTopicRouteData>() {
            @Override
            public SendingTopicRouteData apply(TopicRouteData topicRouteData) {
                final SendingTopicRouteData sendingRouteData = new SendingTopicRouteData(topicRouteData);
                sendingRouteDataCache.put(topic, sendingRouteData);
                return sendingRouteData;
            }
        });
    }

    private ListenableFuture<SendResult> send0(final Message message, final int maxAttempts) {
        final ListenableFuture<SendingTopicRouteData> future = getSendingTopicRouteData(message.getTopic());
        return Futures.transformAsync(future, new AsyncFunction<SendingTopicRouteData, SendResult>() {
            @Override
            public ListenableFuture<SendResult> apply(SendingTopicRouteData sendingRouteData) throws ClientException {
                // prepare the candidate partitions for retry-sending in advance.
                final List<Partition> candidates = takePartitions(sendingRouteData, maxAttempts);
                return send0(message, candidates, maxAttempts);
            }
        });
    }

    private ListenableFuture<SendResult> send0(final Message message, MessageQueueSelector selector, Object arg,
                                               final int maxAttempts) {
        // set message type as fifo.
        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(message);
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
        // check if it is delay message or not.
        final int delayTimeLevel = message.getDelayTimeLevel();
        final long deliveryTimestamp = message.getDelayTimeMillis();
        if (delayTimeLevel > 0 || deliveryTimestamp > 0) {
            MessageImplAccessor.getMessageImpl(message).getSystemAttribute().setMessageType(MessageType.DELAY);
        }

        send0(future, candidates, message, 1, maxAttempts);
        return future;
    }

    private void send0(final SettableFuture<SendResult> future, final List<Partition> candidates,
                       final Message message, final int attempt, final int maxAttempts) {
        final String topic = message.getTopic();
        final String msgId = message.getMsgId();
        // timeout, no need to proceed.
        if (future.isCancelled()) {
            log.error("No need for sending because of timeout, namespace={}, topic={}, messageId={}, maxAttempts={}, "
                      + "attempt={}", namespace, topic, msgId, maxAttempts, attempt);
            return;
        }

        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            // failed to sign, return in advance.
            future.setException(t);
            return;
        }

        // calculate the current partition.
        final Partition partition = candidates.get(IntMath.mod(attempt - 1, candidates.size()));
        final Endpoints endpoints = partition.getBroker().getEndpoints();

        // intercept before message sending.
        final MessageInterceptorContext preContext = MessageInterceptorContext.builder().setAttempt(attempt)
                                                                              .setTopic(topic).build();
        intercept(MessageHookPoint.PRE_SEND_MESSAGE, message.getMessageExt(), preContext);

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final SendMessageRequest request = wrapSendMessageRequest(message, partition);

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

        Futures.addCallback(attemptFuture, new FutureCallback<SendResult>() {
            @Override
            public void onSuccess(SendResult sendResult) {
                // no need more attempts.
                future.set(sendResult);

                // resend message successfully.
                if (1 < attempt) {
                    log.info("Resend message successfully, namespace={}, topic={}, messageId={}, maxAttempts={}, "
                             + "attempt={}, endpoints={}.", namespace, topic, msgId, maxAttempts, attempt, endpoints);
                }

                // intercept after message sending.
                final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                final MessageInterceptorContext context = preContext.toBuilder().setDuration(duration)
                                                                    .setStatus(MessageHookPointStatus.OK)
                                                                    .build();
                intercept(MessageHookPoint.POST_SEND_MESSAGE, message.getMessageExt(), context);
            }

            @Override
            public void onFailure(Throwable t) {
                // intercept after message sending.
                final long duration = stopwatch.elapsed(MessageInterceptor.DEFAULT_TIME_UNIT);
                final MessageInterceptorContext context = preContext.toBuilder().setDuration(duration)
                                                                    .setStatus(MessageHookPointStatus.ERROR)
                                                                    .setThrowable(t)
                                                                    .build();
                intercept(MessageHookPoint.POST_SEND_MESSAGE, message.getMessageExt(), context);
                // isolate endpoints for a while.
                isolateEndpoints(endpoints);

                if (attempt >= maxAttempts) {
                    // no need more attempts.
                    future.setException(t);
                    log.error("Failed to send message finally, run out of attempt times, maxAttempts={}, attempt={}, "
                              + ", namespace={}, topic={}, messageId={}, endpoints={}", maxAttempts, attempt,
                              namespace, topic, msgId, endpoints, t);
                    return;
                }
                // try to do more attempts.
                log.warn("Failed to send message, would attempt to resend right now, maxAttempts={}, "
                         + "attempt={}, namespace={}, topic={}, messageId={}, endpoints={}", maxAttempts, attempt,
                         namespace, topic, msgId, endpoints, t);
                send0(future, candidates, message, 1 + attempt, maxAttempts);
            }
        });
    }

    private List<Partition> takePartitions(SendingTopicRouteData sendingRouteData, int maxAttempts)
            throws ClientException {
        Set<Endpoints> isolated = new HashSet<Endpoints>();
        isolatedEndpointsSetLock.readLock().lock();
        try {
            isolated.addAll(isolatedEndpointsSet);
        } finally {
            isolatedEndpointsSetLock.readLock().unlock();
        }
        return sendingRouteData.takePartitions(isolated, maxAttempts);
    }

    private ListenableFuture<Partition> selectPartition(final Message message, final MessageQueueSelector selector,
                                                        final Object arg) {
        final String topic = message.getTopic();
        final ListenableFuture<SendingTopicRouteData> future = getSendingTopicRouteData(topic);
        return Futures.transformAsync(future, new AsyncFunction<SendingTopicRouteData, Partition>() {
            @Override
            public ListenableFuture<Partition> apply(SendingTopicRouteData sendingRouteData) throws ClientException {
                if (sendingRouteData.isEmpty()) {
                    log.warn("No available sending route for selector, namespace={}, topic={}", namespace, topic);
                    throw new ClientException(ErrorCode.NO_PERMISSION);
                }
                final MessageQueue mq = selector.select(sendingRouteData.getMessageQueues(), message, arg);
                final SettableFuture<Partition> future0 = SettableFuture.create();
                future0.set(mq.getPartition());
                return future0;
            }
        });
    }

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        ProducerData producerData = ProducerData.newBuilder().setGroup(getPbGroup()).build();
        return HeartbeatRequest.newBuilder().setClientId(id).setProducerData(producerData).build();
    }

    @Override
    public void doStats() {
    }

    @Override
    public GenericPollingRequest wrapGenericPollingRequest() {
        final GenericPollingRequest.Builder builder =
                GenericPollingRequest.newBuilder().setClientId(id).setProducerGroup(getPbGroup());
        for (String topic : sendingRouteDataCache.keySet()) {
            Resource topicResource = Resource.newBuilder().setResourceNamespace(namespace).setName(topic).build();
            builder.addTopics(topicResource);
        }
        return builder.build();
    }

    public static SendResult processSendResponse(Endpoints endpoints, SendMessageResponse response)
            throws ServerException {
        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        if (Code.OK.equals(code)) {
            return new SendResult(endpoints, response.getMessageId(), response.getTransactionId());
        }
        log.debug("Response indicates failure of sending message, code={}, status message=[{}]",
                  code, status.getMessage());
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
        this.transactionChecker = checkNotNull(checker, "checker");
    }

    public int getMaxAttempts() {
        return this.maxAttempts;
    }

    public long getSendMessageTimeoutMillis() {
        return this.sendMessageTimeoutMillis;
    }

    public long getTransactionRecoverDelayMillis() {
        return this.transactionResolveDelayMillis;
    }

    public TransactionChecker getTransactionChecker() {
        return this.transactionChecker;
    }

    public void setMaxAttempts(int maxAttempts) {
        checkArgument(maxAttempts > 0, "Must be positive");
        this.maxAttempts = maxAttempts;
    }

    public void setSendMessageTimeoutMillis(long sendMessageTimeoutMillis) {
        checkArgument(sendMessageTimeoutMillis > 0, "Must be positive");
        this.sendMessageTimeoutMillis = sendMessageTimeoutMillis;
    }

    public void setTransactionRecoverDelayMillis(long transactionResolveDelayMillis) {
        checkArgument(transactionResolveDelayMillis > 0, "Must be positive");
        this.transactionResolveDelayMillis = transactionResolveDelayMillis;
    }
}
