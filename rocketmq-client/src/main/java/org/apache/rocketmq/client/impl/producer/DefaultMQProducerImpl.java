package org.apache.rocketmq.client.impl.producer;

import static com.google.protobuf.util.Timestamps.fromMillis;

import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.ProducerGroup;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SystemAttribute;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Metadata;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.constant.SystemProperty;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.exception.RemotingException;
import org.apache.rocketmq.client.impl.ClientBaseImpl;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.impl.ClientManager;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageConst;
import org.apache.rocketmq.client.message.MessageIdUtils;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.protocol.Encoding;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.misc.Validators;
import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionResolution;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.remoting.RpcTarget;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.client.tracing.EventName;
import org.apache.rocketmq.client.tracing.SpanName;
import org.apache.rocketmq.client.tracing.TracingAttribute;
import org.apache.rocketmq.client.tracing.TracingUtility;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.apache.rocketmq.utility.UtilAll;

@Slf4j
public class DefaultMQProducerImpl extends ClientBaseImpl {

    public static final int MESSAGE_COMPRESSION_THRESHOLD = 1024 * 1024 * 4;

    public static final int DEFAULT_MESSAGE_COMPRESSION_LEVEL = 5;

    public static final int MESSAGE_COMPRESSION_LEVEL =
            Integer.parseInt(System.getProperty(SystemProperty.MESSAGE_COMPRESSION_LEVEL,
                                                Integer.toString(DEFAULT_MESSAGE_COMPRESSION_LEVEL)));

    @Setter
    private int maxAttemptTimes = 3;
    @Setter
    private long sendMessageTimeoutMillis = 10 * 1000;
    @Setter
    private int maxMessageSize = 1024 * 1024 * 4;

    @NonNull
    private ThreadPoolExecutor sendCallbackExecutor;

    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoCache;
    private final ClientInstance clientInstance;

    public DefaultMQProducerImpl(String group) {
        super(group);
        this.sendCallbackExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("SendCallbackThread"));

        this.topicPublishInfoCache = new ConcurrentHashMap<String, TopicPublishInfo>();
        this.clientInstance = ClientManager.getInstance().getClientInstance(this);
    }

    /**
     * Start the rocketmq producer.
     *
     * @throws MQClientException the mq client exception.
     */
    @Override
    public void start() throws MQClientException {
        synchronized (this) {
            log.info("Begin to start the rocketmq producer.");
            if (!state.compareAndSet(ServiceState.CREATED, ServiceState.READY)) {
                log.warn("The rocketmq producer has been started before.");
                return;
            }
            super.start();
            state.compareAndSet(ServiceState.READY, ServiceState.STARTED);
            log.info("The rocketmq producer starts successfully.");
        }
    }

    /**
     * Shutdown the rocketmq producer.
     */
    @Override
    public void shutdown() {
        synchronized (this) {
            log.info("Begin to shutdown the rocketmq producer.");
            if (!state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING)) {
                log.warn("The rocketmq producer has not been started before");
                return;
            }
            super.shutdown();
            sendCallbackExecutor.shutdown();
            state.compareAndSet(ServiceState.STOPPING, ServiceState.READY);
            log.info("Shutdown the rocketmq producer successfully.");
        }
    }

    public void setSendCallbackExecutor(final ThreadPoolExecutor callbackExecutor) throws MQClientException {
        synchronized (this) {
            if (null == callbackExecutor) {
                throw new MQClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            if (ServiceState.CREATED != state.get() || ServiceState.READY != state.get()) {
                throw new MQClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            this.sendCallbackExecutor.shutdown();
            this.sendCallbackExecutor = callbackExecutor;
        }
    }

    private SendMessageRequest wrapSendMessageRequest(Message message, Partition partition) {

        final Resource topicResource =
                Resource.newBuilder().setArn(this.getArn()).setName(message.getTopic()).build();

        final Resource groupResource =
                Resource.newBuilder().setArn(this.getArn()).setName(group).build();

        final Map<String, String> properties = message.getUserProperties();

        final boolean transactionFlag =
                Boolean.parseBoolean(properties.get(MessageConst.PROPERTY_TRANSACTION_PREPARED));

        final SystemAttribute.Builder systemAttributeBuilder =
                SystemAttribute.newBuilder()
                               .setBornTimestamp(fromMillis(System.currentTimeMillis()))
                               .setProducerGroup(groupResource)
                               .setMessageId(MessageIdUtils.createUniqID())
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
        // TODO
        if (transactionFlag) {
            systemAttributeBuilder.setMessageType(apache.rocketmq.v1.MessageType.TRANSACTION);
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

    boolean isRunning() {
        return ServiceState.STARTED == state.get();
    }

    void ensureRunning() throws MQClientException {
        if (!isRunning()) {
            throw new MQClientException("Producer is not started");
        }
    }

    public SendResult send(Message message)
            throws MQClientException, InterruptedException, MQServerException,
                   TimeoutException {
        return send(message, sendMessageTimeoutMillis);
    }

    public SendResult send(Message message, long timeoutMillis)
            throws MQClientException, InterruptedException, TimeoutException, MQServerException {
        final ListenableFuture<SendResult> future = send0(message, maxAttemptTimes);
        // Limit the future timeout.
        Futures.withTimeout(future, timeoutMillis, TimeUnit.MILLISECONDS, this.getScheduler());
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof MQClientException) {
                throw (MQClientException) cause;
            }
            if (cause instanceof MQServerException) {
                throw (MQServerException) cause;
            }
            throw new MQClientException(cause);
        }
    }

    public void send(Message message, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        send(message, sendCallback, sendMessageTimeoutMillis);
    }

    public void send(Message message, final SendCallback sendCallback, long timeoutMillis)
            throws MQClientException, RemotingException, InterruptedException {
        final ListenableFuture<SendResult> future = send0(message, maxAttemptTimes);
        // Limit the future timeout.
        Futures.withTimeout(future, timeoutMillis, TimeUnit.MILLISECONDS, this.getScheduler());
        Futures.addCallback(future, new FutureCallback<SendResult>() {
            @Override
            public void onSuccess(final SendResult sendResult) {
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
            }

            @Override
            public void onFailure(final Throwable t) {
                sendCallbackExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sendCallback.onException(t);
                        } catch (Throwable t) {
                            log.error("Exception occurs in SendCallback#onFailure", t);
                        }
                    }
                });
            }
        });
    }

    public void sendOneway(Message message) {
        send0(message, 1);
    }

    public SendResult send(Message message, MessageQueueSelector selector, Object arg)
            throws MQClientException, MQBrokerException, InterruptedException, MQServerException,
                   TimeoutException {
        return send(message, selector, arg, sendMessageTimeoutMillis);
    }

    public SendResult send(Message message, MessageQueueSelector selector, Object arg, long timeoutMillis)
            throws MQClientException, MQServerException, InterruptedException, TimeoutException {
        final ListenableFuture<SendResult> future = send0(message, selector, arg, maxAttemptTimes);
        Futures.withTimeout(future, timeoutMillis, TimeUnit.MILLISECONDS, this.getScheduler());
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof MQClientException) {
                throw (MQClientException) cause;
            }
            if (cause instanceof MQServerException) {
                throw (MQServerException) cause;
            }
            throw new MQClientException(cause);
        }
    }

    public void send(
            Message message, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        send(message, selector, arg, sendCallback, sendMessageTimeoutMillis);
    }

    public void send(
            Message message,
            MessageQueueSelector selector,
            Object arg,
            final SendCallback sendCallback,
            long timeoutMillis)
            throws MQClientException, RemotingException, InterruptedException {
        final ListenableFuture<SendResult> future = send0(message, selector, arg, maxAttemptTimes);
        Futures.withTimeout(future, timeoutMillis, TimeUnit.MILLISECONDS, this.getScheduler());
        Futures.addCallback(future, new FutureCallback<SendResult>() {
            @Override
            public void onSuccess(final SendResult sendResult) {
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
            }

            @Override
            public void onFailure(final Throwable t) {
                sendCallbackExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sendCallback.onException(t);
                        } catch (Throwable t) {
                            log.error("Exception occurs in SendCallback#onFailure", t);
                        }
                    }
                });
            }
        });
    }

    public void sendOneway(Message message, MessageQueueSelector selector, Object arg) {
        send0(message, selector, arg, 1);
    }

    public TransactionSendResult sendTransaction(Message message, LocalTransactionExecuter executor, Object object)
            throws MQClientException {
        throw new UnsupportedOperationException();
    }

    private void endTransaction(RpcTarget target, final String messageId, final String transactionId,
                                String traceContext, TransactionResolution resolution) throws MQClientException {
        EndTransactionRequest request =
                EndTransactionRequest.newBuilder()
                                     .setMessageId(messageId)
                                     .setTransactionId(transactionId)
                                     .setTraceContext(traceContext)
                                     .setResolution(resolution == TransactionResolution.COMMIT ?
                                                    EndTransactionRequest.TransactionResolution.COMMIT :
                                                    EndTransactionRequest.TransactionResolution.ROLLBACK)
                                     .build();
        final Metadata metadata = sign();
        final Span span = startEndTransactionSpan(messageId, transactionId, traceContext);
        final ListenableFuture<EndTransactionResponse> future =
                clientInstance.endTransaction(target, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        Futures.addCallback(future, new FutureCallback<EndTransactionResponse>() {
            @Override
            public void onSuccess(EndTransactionResponse response) {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                if (Code.OK != code) {
                    log.error("Failed to end transaction, messageId={}, transactionId={}, code={}, message={}",
                              messageId, transactionId, code, status.getMessage());
                    endSpan(span, StatusCode.ERROR);
                    return;
                }
                log.trace("End transaction successfully, messageId={}, transactionId={}, code={}, message={}",
                          messageId, transactionId, code, status.getMessage());
                endSpan(span, StatusCode.OK);
            }

            @Override
            public void onFailure(Throwable t) {
                endSpan(span, StatusCode.ERROR, t);
            }
        });
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
            public ListenableFuture<SendResult> apply(TopicPublishInfo topicPublishInfo) throws MQClientException {
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
        final SettableFuture<SendResult> future = SettableFuture.create();
        // Filter illegal message.
        try {
            Validators.messageCheck(message, maxMessageSize);
        } catch (MQClientException e) {
            future.setException(e);
            return future;
        }
        int attemptTimes = 1;
        final Partition partition = candidates.get(attemptTimes);
        final SendMessageRequest request = wrapSendMessageRequest(message, partition);
        send0(future, candidates, request, attemptTimes, maxAttemptTimes);
        return future;
    }

    private void send0(final SettableFuture<SendResult> future, final List<Partition> candidates,
                       final SendMessageRequest request, final int attemptTimes, final int maxAttemptTimes) {
        // Calculate the current partition.
        final Partition partition = candidates.get((attemptTimes - 1) % candidates.size());
        final RpcTarget target = partition.getTarget();
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            future.setException(t);
            return;
        }
        final SendMessageRequest.Builder requestBuilder = request.toBuilder();
        final Span span = startSendMessageSpan(requestBuilder);

        final ListenableFuture<SendMessageResponse> responseFuture =
                clientInstance.sendMessage(target, metadata, requestBuilder.build(), ioTimeoutMillis,
                                           TimeUnit.MILLISECONDS);
        final ListenableFuture<SendResult> attemptFuture = Futures.transformAsync(
                responseFuture, new AsyncFunction<SendMessageResponse, SendResult>() {
                    @Override
                    public ListenableFuture<SendResult> apply(SendMessageResponse response) throws Exception {
                        final SettableFuture<SendResult> future0 = SettableFuture.create();
                        final SendResult sendResult = processSendResponse(target, response);
                        future0.set(sendResult);
                        return future0;
                    }
                });

        Futures.addCallback(attemptFuture, new FutureCallback<SendResult>() {
            @Override
            public void onSuccess(SendResult sendResult) {
                future.set(sendResult);
                endSpan(span, StatusCode.OK);
            }

            @Override
            public void onFailure(Throwable t) {
                endSpan(span, StatusCode.ERROR);
                // No need more attempts.
                if (attemptTimes >= maxAttemptTimes) {
                    future.setException(t);
                    log.error("Failed to send message, attempt times is exhausted, maxAttemptTimes={}, currentTimes={}",
                              maxAttemptTimes, attemptTimes, t);
                    return;
                }
                log.warn("Failed to send message, would attempt to re-send right now, maxAttemptTimes={}, "
                         + "currentTimes={}", maxAttemptTimes, attemptTimes, t);
                // Turn to the next partition.
                final int nextAttemptTimes = attemptTimes + 1;
                final Partition nextPartition = candidates.get((nextAttemptTimes - 1) % candidates.size());
                // Replace the partition-id in request by the newest one.
                SystemAttribute systemAttribute = request.getMessage().getSystemAttribute();
                SystemAttribute nextSystemAttribute =
                        systemAttribute.toBuilder().setPartitionId(nextPartition.getId()).build();
                final apache.rocketmq.v1.Message nextMessage =
                        request.getMessage().toBuilder().setSystemAttribute(nextSystemAttribute).build();
                final SendMessageRequest nextRequest = request.toBuilder().setMessage(nextMessage).build();
                send0(future, candidates, nextRequest, nextAttemptTimes, maxAttemptTimes);
            }
        });
    }

    List<Partition> takePartitionsRoundRobin(TopicPublishInfo topicPublishInfo, int maxAttemptTimes)
            throws MQClientException {
        final Set<Endpoints> isolated = clientInstance.getAllIsolatedEndpoints();
        return topicPublishInfo.takePartitions(isolated, maxAttemptTimes);
    }

    private ListenableFuture<Partition> selectPartition(final Message message, final MessageQueueSelector selector,
                                                        final Object arg) {
        final String topic = message.getTopic();
        final ListenableFuture<TopicPublishInfo> future = getPublishInfo(topic);
        return Futures.transformAsync(future, new AsyncFunction<TopicPublishInfo, Partition>() {
            @Override
            public ListenableFuture<Partition> apply(TopicPublishInfo topicPublishInfo) throws MQClientException {
                if (topicPublishInfo.isEmpty()) {
                    log.warn("No available partition for selector, topic={}", topic);
                    throw new MQClientException(ErrorCode.NO_PERMISSION);
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
                Resource.newBuilder().setArn(this.getArn()).setName(group).build();
        ProducerGroup producerGroup = ProducerGroup.newBuilder().setGroup(groupResource).build();
        return HeartbeatEntry.newBuilder()
                             .setClientId(clientId)
                             .setProducerGroup(producerGroup)
                             .build();
    }

    @Override
    public void logStats() {
    }

    private Span startSendMessageSpan(SendMessageRequest.Builder requestBuilder) {
        if (null == tracer || !isMessageTracingEnabled()) {
            return null;
        }
        final Span span = tracer.spanBuilder(SpanName.SEND_MESSAGE).startSpan();
        apache.rocketmq.v1.Message message = requestBuilder.getMessage();
        SystemAttribute systemAttribute = message.getSystemAttribute();

        span.addEvent(EventName.MSG_BORN, Timestamps.toMillis(systemAttribute.getBornTimestamp()),
                      TimeUnit.MILLISECONDS);
        span.setAttribute(TracingAttribute.ACCESS_KEY, getAccessCredential().getAccessKey());
        span.setAttribute(TracingAttribute.ARN, message.getTopic().getArn());
        span.setAttribute(TracingAttribute.TOPIC, message.getTopic().getName());
        span.setAttribute(TracingAttribute.MSG_ID, systemAttribute.getMessageId());
        span.setAttribute(TracingAttribute.GROUP, systemAttribute.getProducerGroup().getName());
        span.setAttribute(TracingAttribute.TAGS, systemAttribute.getTag());
        StringBuilder keys = new StringBuilder();
        for (String key : systemAttribute.getKeysList()) {
            keys.append(key);
        }
        span.setAttribute(TracingAttribute.KEYS, keys.toString().trim());
        span.setAttribute(TracingAttribute.BORN_HOST, systemAttribute.getBornHost());
        final apache.rocketmq.v1.MessageType messageType = systemAttribute.getMessageType();
        switch (messageType) {
            case FIFO:
                span.setAttribute(TracingAttribute.MSG_TYPE, MessageType.FIFO.getName());
                break;
            case DELAY:
                span.setAttribute(TracingAttribute.MSG_TYPE, MessageType.DELAY.getName());
                break;
            case TRANSACTION:
                span.setAttribute(TracingAttribute.MSG_TYPE, MessageType.TRANSACTION.getName());
                break;
            default:
                span.setAttribute(TracingAttribute.MSG_TYPE, MessageType.NORMAL.getName());
        }
        final long deliveryTimestamp = Timestamps.toMillis(systemAttribute.getDeliveryTimestamp());
        if (deliveryTimestamp > 0) {
            span.setAttribute(TracingAttribute.DELIVERY_TIMESTAMP, deliveryTimestamp);
        }
        final String serializedSpanContext = TracingUtility.injectSpanContextToTraceParent(span.getSpanContext());

        systemAttribute = systemAttribute.toBuilder().setTraceContext(serializedSpanContext).build();
        message = message.toBuilder().setSystemAttribute(systemAttribute).build();
        requestBuilder.setMessage(message);

        return span;
    }

    private Span startEndTransactionSpan(String messageId, String transactionId, String traceContext) {
        if (null == tracer || !isMessageTracingEnabled()) {
            return null;
        }
        final SpanBuilder spanBuilder = tracer.spanBuilder(SpanName.END_MESSAGE);

        final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
        if (spanContext.isValid()) {
            spanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
        }
        final Span span = spanBuilder.startSpan();
        span.setAttribute(TracingAttribute.ACCESS_KEY, getAccessCredential().getAccessKey());
        span.setAttribute(TracingAttribute.MSG_ID, messageId);
        span.setAttribute(TracingAttribute.TRANSACTION_ID, transactionId);

        return span;
    }

    private void endSpan(Span span, StatusCode statusCode) {
        endSpan(span, statusCode, null);
    }

    private void endSpan(Span span, StatusCode statusCode, Throwable t) {
        if (null == span) {
            return;
        }
        span.setStatus(statusCode);
        if (null != t) {
            span.recordException(t);
        }
        span.end();
    }
}
