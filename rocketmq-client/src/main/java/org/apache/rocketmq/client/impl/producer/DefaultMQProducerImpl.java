package org.apache.rocketmq.client.impl.producer;

import static com.google.protobuf.util.Timestamps.fromMillis;

import apache.rocketmq.v1.ClientResourceBundle;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.MessageType;
import apache.rocketmq.v1.ProducerGroup;
import apache.rocketmq.v1.ResolveOrphanedTransactionRequest;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SystemAttribute;
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
import com.google.protobuf.util.Timestamps;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.ClientBaseImpl;
import org.apache.rocketmq.client.impl.Signature;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageAccessor;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageIdUtils;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.protocol.Encoding;
import org.apache.rocketmq.client.misc.Validators;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionChecker;
import org.apache.rocketmq.client.producer.TransactionImpl;
import org.apache.rocketmq.client.producer.TransactionResolution;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.apache.rocketmq.utility.UtilAll;

@Slf4j
public class DefaultMQProducerImpl extends ClientBaseImpl {

    private static final int MESSAGE_COMPRESSION_THRESHOLD = 1024 * 4;
    private static final int MESSAGE_COMPRESSION_LEVEL = 5;

    @Setter
    private int maxAttemptTimes = 3;
    @Setter
    private long sendMessageTimeoutMillis = 10 * 1000;
    @Setter
    private int maxMessageSize = 1024 * 1024 * 4;

    private final ThreadPoolExecutor defaultSendCallbackExecutor;

    private ThreadPoolExecutor customizedSendCallbackExecutor = null;

    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoCache;

    @GuardedBy("isolatedRouteEndpointsSetLock")
    private final Set<Endpoints> isolatedRouteEndpointsSet;
    private final ReadWriteLock isolatedRouteEndpointsSetLock;

    @Setter
    private TransactionChecker transactionChecker;
    private final ThreadPoolExecutor transactionCheckerExecutor;

    public DefaultMQProducerImpl(String group) {
        super(group);
        this.defaultSendCallbackExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("SendCallbackThread"));

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

    @Override
    public void doHealthCheck() {
        final Set<Endpoints> routeEndpointsSet = getRouteEndpointsSet();
        final Set<Endpoints> diff = new HashSet<Endpoints>(Sets.difference(routeEndpointsSet,
                                                                           isolatedRouteEndpointsSet));
        // Remove all isolated endpoints where is not in the topic route.
        isolatedRouteEndpointsSetLock.writeLock().lock();
        try {
            isolatedRouteEndpointsSet.removeAll(diff);
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
                        clientInstance.healthCheck(endpoints, metadata, request, ioTimeoutMillis,
                                                   TimeUnit.MILLISECONDS);
                Futures.addCallback(future, new FutureCallback<HealthCheckResponse>() {
                    @Override
                    public void onSuccess(HealthCheckResponse response) {
                        final Status status = response.getCommon().getStatus();
                        final Code code = Code.forNumber(status.getCode());
                        if (Code.OK == code) {
                            isolatedRouteEndpointsSetLock.writeLock().lock();
                            try {
                                isolatedRouteEndpointsSet.remove(endpoints);
                            } finally {
                                isolatedRouteEndpointsSetLock.writeLock().unlock();
                            }
                            log.info("Restore isolated endpoints, endpoints={}", endpoints);
                            return;
                        }
                        log.warn("Failed to restore isolated endpoints, code={}, status message={}, endpoints={}",
                                 code, status.getMessage(), endpoints);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.error("Failed to do health check, endpoints={}", endpoints, t);
                    }
                });
            }
        } finally {
            isolatedRouteEndpointsSetLock.readLock().unlock();
        }
    }

    public void setDefaultSendCallbackExecutor(final ThreadPoolExecutor callbackExecutor) throws ClientException {
        synchronized (this) {
            if (null == callbackExecutor) {
                throw new ClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            this.customizedSendCallbackExecutor = callbackExecutor;
        }
    }

    public ThreadPoolExecutor getSendCallbackExecutor() {
        if (null != customizedSendCallbackExecutor) {
            return customizedSendCallbackExecutor;
        }
        return defaultSendCallbackExecutor;
    }

    private SendMessageRequest wrapSendMessageRequest(Message message, Partition partition) {

        final Resource topicResource =
                Resource.newBuilder().setArn(arn).setName(message.getTopic()).build();

        final Resource groupResource =
                Resource.newBuilder().setArn(arn).setName(group).build();

        final SystemAttribute.Builder systemAttributeBuilder =
                SystemAttribute.newBuilder()
                               .setBornTimestamp(fromMillis(System.currentTimeMillis()))
                               .setProducerGroup(groupResource)
                               .setMessageId(message.getMessageExt().getMsgId())
                               .setBornHost(UtilAll.getIpv4Address())
                               .setPartitionId(partition.getId());

        final int delayTimeLevel = message.getDelayTimeLevel();
        if (delayTimeLevel > 0) {
            systemAttributeBuilder.setDelayLevel(delayTimeLevel);
        } else {
            final long deliveryTimestamp = message.getDeliveryTimestamp();
            if (deliveryTimestamp > 0) {
                systemAttributeBuilder.setDeliveryTimestamp(Timestamps.fromMillis(deliveryTimestamp));
            }
        }

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
                systemAttributeBuilder.setMessageType(MessageType.FIFO);
                break;
            case DELAY:
                systemAttributeBuilder.setMessageType(MessageType.DELAY);
                break;
            case TRANSACTION:
                systemAttributeBuilder.setMessageType(MessageType.TRANSACTION);
                break;
            default:
                systemAttributeBuilder.setMessageType(MessageType.NORMAL);
        }
        final SystemAttribute systemAttribute = systemAttributeBuilder.build();

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
        final ListenableFuture<SendResult> future = send0(message, maxAttemptTimes);
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
        final ListenableFuture<SendResult> future = send0(message, maxAttemptTimes);
        // Limit the future timeout.
        Futures.withTimeout(future, timeoutMillis, TimeUnit.MILLISECONDS, this.getScheduler());
        final ThreadPoolExecutor sendCallbackExecutor = getSendCallbackExecutor();
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
                                log.error("Exception occurs in SendCallback#onSuccess", t);
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

    public SendResult send(Message message, MessageQueueSelector selector, Object arg)
            throws ClientException, InterruptedException, ServerException, TimeoutException {
        return send(message, selector, arg, sendMessageTimeoutMillis);
    }

    public SendResult send(Message message, MessageQueueSelector selector, Object arg, long timeoutMillis)
            throws ClientException, ServerException, InterruptedException, TimeoutException {
        ensureRunning();
        final ListenableFuture<SendResult> future = send0(message, selector, arg, maxAttemptTimes);
        Futures.withTimeout(future, timeoutMillis, TimeUnit.MILLISECONDS, this.getScheduler());
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw onExecutionException(e);
        }
    }

    public void send(
            Message message, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
            throws ClientException, InterruptedException {
        send(message, selector, arg, sendCallback, sendMessageTimeoutMillis);
    }

    public void send(
            Message message,
            MessageQueueSelector selector,
            Object arg,
            final SendCallback sendCallback,
            long timeoutMillis)
            throws ClientException, InterruptedException {
        ensureRunning();
        final ListenableFuture<SendResult> future = send0(message, selector, arg, maxAttemptTimes);
        Futures.withTimeout(future, timeoutMillis, TimeUnit.MILLISECONDS, this.getScheduler());
        Futures.addCallback(future, new FutureCallback<SendResult>() {
            @Override
            public void onSuccess(final SendResult sendResult) {
                defaultSendCallbackExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sendCallback.onSuccess(sendResult);
                        } catch (Throwable t) {
                            log.error("Exception raised in SendCallback#onSuccess", t);
                        }
                    }
                });
            }

            @Override
            public void onFailure(final Throwable t) {
                defaultSendCallbackExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sendCallback.onException(t);
                        } catch (Throwable t) {
                            log.error("Exception raised in SendCallback#onFailure", t);
                        }
                    }
                });
            }
        });
    }

    public void sendOneway(Message message, MessageQueueSelector selector, Object arg) {
        send0(message, selector, arg, 1);
    }

    public TransactionImpl prepare(Message message) throws ServerException, InterruptedException,
                                                           ClientException, TimeoutException {
        final SendResult sendResult = send(message);
        final String msgId = sendResult.getMsgId();
        final String transactionId = sendResult.getTransactionId();
        final Endpoints endpoints = sendResult.getEndpoints();
        return new TransactionImpl(msgId, transactionId, endpoints, this);
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
            metadata = Signature.sign(this);
        } catch (Throwable t) {
            throw new ClientException(ErrorCode.SIGNATURE_FAILURE);
        }
        final ListenableFuture<EndTransactionResponse> future =
                clientInstance.endTransaction(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
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
        final String transactionId = messageExt.getTransactionId();
        try {
            transactionCheckerExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        final TransactionResolution resolution = transactionChecker.check(messageExt);
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
    protected void updateTopicRouteCache(String topic, TopicRouteData topicRouteData) {
        topicPublishInfoCache.put(topic, new TopicPublishInfo(topicRouteData));
        super.updateTopicRouteCache(topic, topicRouteData);
    }

    public ListenableFuture<TopicPublishInfo> getPublishInfo(final String topic) {
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

    public ListenableFuture<SendResult> send0(final Message message, final int maxAttemptTimes) {
        final ListenableFuture<TopicPublishInfo> future = getPublishInfo(message.getTopic());
        return Futures.transformAsync(future, new AsyncFunction<TopicPublishInfo, SendResult>() {
            @Override
            public ListenableFuture<SendResult> apply(TopicPublishInfo topicPublishInfo) throws ClientException {
                // Prepare the candidate partitions for retry-sending in advance.
                final List<Partition> candidates = takePartitionsRoundRobin(topicPublishInfo, maxAttemptTimes);
                return send0(message, candidates, maxAttemptTimes);
            }
        });
    }

    private ListenableFuture<SendResult> send0(final Message message, MessageQueueSelector selector, Object arg,
                                               final int maxAttemptTimes) {
        final ListenableFuture<Partition> future0 = selectPartition(message, selector, arg);
        return Futures.transformAsync(future0, new AsyncFunction<Partition, SendResult>() {
            @Override
            public ListenableFuture<SendResult> apply(Partition partition) {
                List<Partition> candidates = new ArrayList<Partition>();
                candidates.add(partition);
                return send0(message, candidates, maxAttemptTimes);
            }
        });
    }

    private ListenableFuture<SendResult> send0(final Message message, final List<Partition> candidates,
                                               int maxAttemptTimes) {
        // Set messageId
        final MessageImpl messageImpl = MessageAccessor.getMessageImpl(message);
        final String messageId = MessageIdUtils.createUniqId();
        messageImpl.getSystemAttribute().setMessageId(messageId);

        final SettableFuture<SendResult> future = SettableFuture.create();
        // Filter illegal message.
        try {
            Validators.messageCheck(message, maxMessageSize);
        } catch (ClientException e) {
            future.setException(e);
            return future;
        }
        send0(future, candidates, message, 1, maxAttemptTimes);
        return future;
    }

    private void send0(final SettableFuture<SendResult> future, final List<Partition> candidates,
                       final Message message, final int attemptTimes, final int maxAttemptTimes) {
        // Calculate the current partition.
        final Partition partition = candidates.get((attemptTimes - 1) % candidates.size());

        final Endpoints endpoints = partition.getBroker().getEndpoints();
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            // Failed to sign, return in advance.
            future.setException(t);
            return;
        }

        final SendMessageRequest request = wrapSendMessageRequest(message, partition);
        final Stopwatch stopwatch = Stopwatch.createStarted();

        // Intercept message while PRE_SEND_MESSAGE.
        final MessageInterceptorContext.MessageInterceptorContextBuilder contextBuilder =
                MessageInterceptorContext.builder().attemptTimes(attemptTimes);
        interceptMessage(MessageHookPoint.PRE_SEND_MESSAGE, message.getMessageExt(), contextBuilder.build());

        final ListenableFuture<SendMessageResponse> responseFuture =
                clientInstance.sendMessage(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);

        // Return the future of send result for current attempt.
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
                // No need more attempts.
                future.set(sendResult);

                // Intercept message while POST_SEND_MESSAGE.
                final long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                final MessageInterceptorContext context = contextBuilder.duration(duration)
                                                                      .timeUnit(TimeUnit.MILLISECONDS)
                                                                      .status(MessageHookPoint.PointStatus.OK)
                                                                      .build();
                interceptMessage(MessageHookPoint.POST_SEND_MESSAGE, message.getMessageExt(), context);
            }

            @Override
            public void onFailure(Throwable t) {
                // Intercept message while POST_SEND_MESSAGE.
                final long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                final MessageInterceptorContext context = contextBuilder.duration(duration)
                                                                      .timeUnit(TimeUnit.MILLISECONDS)
                                                                      .status(MessageHookPoint.PointStatus.ERROR)
                                                                      .build();
                interceptMessage(MessageHookPoint.POST_SEND_MESSAGE, message.getMessageExt(), context);

                if (attemptTimes >= maxAttemptTimes) {
                    // No need more attempts.
                    future.setException(t);
                    log.error("Failed to send message, attempt times is exhausted, maxAttemptTimes={}, currentTimes={}",
                              maxAttemptTimes, attemptTimes, t);
                    return;
                }
                // Try to do more attempts.
                log.warn("Failed to send message, would attempt to re-send right now, maxAttemptTimes={}, "
                         + "currentTimes={}", maxAttemptTimes, attemptTimes, t);
                send0(future, candidates, message, 1 + attemptTimes, maxAttemptTimes);
            }
        });
    }

    List<Partition> takePartitionsRoundRobin(TopicPublishInfo topicPublishInfo, int maxAttemptTimes)
            throws ClientException {
        final Set<Endpoints> isolated = clientInstance.getAllIsolatedEndpoints();
        return topicPublishInfo.takePartitions(isolated, maxAttemptTimes);
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
        Resource groupResource =
                Resource.newBuilder().setArn(arn).setName(group).build();
        ProducerGroup producerGroup = ProducerGroup.newBuilder().setGroup(groupResource).build();
        return HeartbeatEntry.newBuilder()
                             .setClientId(clientId)
                             .setProducerGroup(producerGroup)
                             .build();
    }

    @Override
    public void logStats() {
    }

    @Override
    public ClientResourceBundle wrapClientResourceBundle() {
        Resource groupResource = Resource.newBuilder().setArn(arn).setName(group).build();
        final ClientResourceBundle.Builder builder =
                ClientResourceBundle.newBuilder().setClientId(clientId).setProducerGroup(groupResource);
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
        return new ClientException(cause);
    }
}
