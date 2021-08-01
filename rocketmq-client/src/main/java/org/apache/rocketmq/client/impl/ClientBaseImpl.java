package org.apache.rocketmq.client.impl;

import apache.rocketmq.v1.ClientResourceBundle;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.GenericPollingRequest;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.MultiplexingRequest;
import apache.rocketmq.v1.MultiplexingResponse;
import apache.rocketmq.v1.PrintThreadStackResponse;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.ResolveOrphanedTransactionRequest;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.ResponseCommon;
import apache.rocketmq.v1.SystemAttribute;
import apache.rocketmq.v1.VerifyMessageConsumptionRequest;
import apache.rocketmq.v1.VerifyMessageConsumptionResponse;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.constant.Permission;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.OffsetQuery;
import org.apache.rocketmq.client.consumer.PullMessageQuery;
import org.apache.rocketmq.client.consumer.PullMessageResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.QueryOffsetPolicy;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.protocol.Digest;
import org.apache.rocketmq.client.message.protocol.DigestType;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.misc.TopAddressing;
import org.apache.rocketmq.client.remoting.Address;
import org.apache.rocketmq.client.remoting.ClientAuthInterceptor;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.remoting.IpNameResolverFactory;
import org.apache.rocketmq.client.route.AddressScheme;
import org.apache.rocketmq.client.route.Broker;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.client.tracing.TracingMessageInterceptor;
import org.apache.rocketmq.utility.UtilAll;

@Slf4j
public abstract class ClientBaseImpl extends ClientConfig implements ClientObserver {

    private static final long MULTIPLEXING_CALL_LATER_DELAY_MILLIS = 3 * 1000L;
    private static final long MULTIPLEXING_CALL_TIMEOUT_MILLIS = 60 * 1000L;
    private static final long VERIFY_CONSUMPTION_TIMEOUT_MILLIS = 15 * 1000L;

    private static final String TRACER_INSTRUMENTATION_NAME = "org.apache.rocketmq.message.tracer";
    private static final long TRACE_EXPORTER_SCHEDULE_DELAY_MILLIS = 1000L;
    private static final long TRACE_EXPORTER_RPC_TIMEOUT_MILLIS = 3000L;
    private static final int TRACE_EXPORTER_BATCH_SIZE = 65536;

    @Getter
    protected volatile Tracer tracer;
    protected volatile Endpoints tracingEndpoints;

    @Getter
    protected volatile ClientInstance clientInstance;

    private final AtomicReference<ServiceState> state;

    private final TopAddressing topAddressing;
    private final AtomicInteger nameServerIndex;

    @GuardedBy("messageInterceptorsLock")
    private final List<MessageInterceptor> messageInterceptors;
    private final ReadWriteLock messageInterceptorsLock;

    @GuardedBy("nameServerEndpointsListLock")
    private final List<Endpoints> nameServerEndpointsList;
    private final ReadWriteLock nameServerEndpointsListLock;

    @GuardedBy("inflightRouteFutureLock")
    private final Map<String /* topic */, Set<SettableFuture<TopicRouteData>>> inflightRouteFutureTable;
    private final Lock inflightRouteFutureLock;

    private volatile ScheduledFuture<?> renewNameServerListFuture;
    private volatile ScheduledFuture<?> updateRouteCacheFuture;

    private final ConcurrentMap<String /* topic */, TopicRouteData> topicRouteCache;

    public ClientBaseImpl(String group) {
        super(group);
        this.state = new AtomicReference<ServiceState>(ServiceState.READY);

        this.tracer = null;
        this.tracingEndpoints = null;

        this.topAddressing = new TopAddressing();
        this.nameServerIndex = new AtomicInteger(RandomUtils.nextInt());

        this.messageInterceptors = new ArrayList<MessageInterceptor>();
        this.messageInterceptorsLock = new ReentrantReadWriteLock();

        this.nameServerEndpointsList = new ArrayList<Endpoints>();
        this.nameServerEndpointsListLock = new ReentrantReadWriteLock();

        this.inflightRouteFutureTable = new HashMap<String, Set<SettableFuture<TopicRouteData>>>();
        this.inflightRouteFutureLock = new ReentrantLock();

        this.topicRouteCache = new ConcurrentHashMap<String, TopicRouteData>();
    }

    public void registerMessageInterceptor(MessageInterceptor messageInterceptor) {
        messageInterceptorsLock.writeLock().lock();
        try {
            messageInterceptors.add(messageInterceptor);
        } finally {
            messageInterceptorsLock.writeLock().unlock();
        }
    }

    public void start() throws ClientException {
        synchronized (this) {
            log.info("Begin to start the rocketmq client base.");
            if (!state.compareAndSet(ServiceState.READY, ServiceState.STARTING)) {
                log.warn("The rocketmq client base has been started before.");
                return;
            }
            clientInstance = ClientManager.getInstance().getClientInstance(this);
            clientInstance.registerClientObserver(this);

            if (messageTracingEnabled) {
                final TracingMessageInterceptor tracingInterceptor = new TracingMessageInterceptor(this);
                registerMessageInterceptor(tracingInterceptor);
            }

            final ScheduledExecutorService scheduler = clientInstance.getScheduler();
            if (nameServerIsNotSet()) {
                // Acquire name server list immediately.
                renewNameServerList();

                log.info("Name server list was not set, schedule a task to fetch and renew periodically");
                renewNameServerListFuture = scheduler.scheduleWithFixedDelay(
                        new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    renewNameServerList();
                                } catch (Throwable t) {
                                    log.error("Exception raised while updating nameserver from top addressing", t);
                                }
                            }
                        },
                        0,
                        30,
                        TimeUnit.SECONDS);
            }
            updateRouteCacheFuture = scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                updateRouteCache();
                            } catch (Throwable t) {
                                log.error("Exception raised while updating topic route cache", t);
                            }
                        }
                    },
                    10,
                    30,
                    TimeUnit.SECONDS);
            state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
            log.info("The rocketmq client base starts successfully.");
        }
    }

    public void shutdown() {
        synchronized (this) {
            log.info("Begin to shutdown the rocketmq client base.");
            if (!state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING)) {
                log.warn("The rocketmq client base has not been started before");
                return;
            }
            if (null != renewNameServerListFuture) {
                renewNameServerListFuture.cancel(false);
            }
            if (null != updateRouteCacheFuture) {
                updateRouteCacheFuture.cancel(false);
            }
            clientInstance.unregisterClientObserver(this);
            state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED);
            log.info("Shutdown the rocketmq client base successfully.");
        }
    }

    public void intercept(MessageHookPoint hookPoint, MessageExt messageExt, MessageInterceptorContext context) {
        messageInterceptorsLock.readLock().lock();
        try {
            for (MessageInterceptor interceptor : messageInterceptors) {
                try {
                    interceptor.intercept(hookPoint, messageExt, context);
                } catch (Throwable t) {
                    log.error("Exception occurs while intercepting message, hookPoint={}, message={}", hookPoint,
                              messageExt, t);
                }
            }
        } finally {
            messageInterceptorsLock.readLock().unlock();
        }
    }

    public ScheduledExecutorService getScheduler() {
        return clientInstance.getScheduler();
    }

    public ServiceState getState() {
        return this.state.get();
    }

    public Metadata sign() throws ClientException {
        try {
            return Signature.sign(this);
        } catch (Throwable t) {
            log.error("Failed to calculate signature", t);
            throw new ClientException(ErrorCode.SIGNATURE_FAILURE);
        }
    }

    public Set<Endpoints> getRouteEndpointsSet() {
        Set<Endpoints> endpointsSet = new HashSet<Endpoints>();
        for (TopicRouteData topicRouteData : topicRouteCache.values()) {
            endpointsSet.addAll(topicRouteData.allEndpoints());
        }
        return endpointsSet;
    }

    public boolean nameServerIsNotSet() {
        nameServerEndpointsListLock.readLock().lock();
        try {
            return nameServerEndpointsList.isEmpty();
        } finally {
            nameServerEndpointsListLock.readLock().unlock();
        }
    }

    public void renewNameServerList() {
        log.info("Start to renew name server list for a new round");
        List<Endpoints> newNameServerList;
        try {
            newNameServerList = topAddressing.fetchNameServerAddresses();
        } catch (Throwable t) {
            log.error("Failed to fetch name server list from top addressing", t);
            return;
        }
        if (newNameServerList.isEmpty()) {
            log.warn("Yuck, got an empty name server list.");
            return;
        }
        nameServerEndpointsListLock.writeLock().lock();
        try {
            if (nameServerEndpointsList == newNameServerList) {
                log.debug("Name server list remains no changed");
                return;
            }
            nameServerEndpointsList.clear();
            nameServerEndpointsList.addAll(newNameServerList);
        } finally {
            nameServerEndpointsListLock.writeLock().unlock();
        }
    }

    protected void updateTopicRouteCache(String topic, TopicRouteData topicRouteData) {
        final Set<Endpoints> before = getRouteEndpointsSet();
        topicRouteCache.put(topic, topicRouteData);
        final Set<Endpoints> after = getRouteEndpointsSet();
        final Set<Endpoints> diff = new HashSet<Endpoints>(Sets.difference(after, before));

        for (Endpoints endpoints : diff) {
            log.info("Start multiplexing call first time, endpoints={}", endpoints);
            dispatchGenericPollRequest(endpoints);
        }

        if (messageTracingEnabled) {
            updateTracer();
        }
    }

    private void updateRouteCache() {
        log.info("Start to update route cache for a new round");
        for (final String topic : topicRouteCache.keySet()) {
            final ListenableFuture<TopicRouteData> future = fetchTopicRoute(topic);
            Futures.addCallback(future, new FutureCallback<TopicRouteData>() {
                @Override
                public void onSuccess(TopicRouteData topicRouteData) {
                    updateTopicRouteCache(topic, topicRouteData);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Failed to fetch topic route, topic={}", topic, t);
                }
            });
        }
    }

    public ListenableFuture<TopicRouteData> getRouteFor(final String topic) {
        SettableFuture<TopicRouteData> future0 = SettableFuture.create();
        TopicRouteData topicRouteData = topicRouteCache.get(topic);
        // If route was cached before, get it directly.
        if (null != topicRouteData) {
            future0.set(topicRouteData);
            return future0;
        }
        inflightRouteFutureLock.lock();
        try {
            // If route was fetched by last in-flight route request, get it directly.
            topicRouteData = topicRouteCache.get(topic);
            if (null != topicRouteData) {
                future0.set(topicRouteData);
                return future0;
            }
            Set<SettableFuture<TopicRouteData>> inflightFutures = inflightRouteFutureTable.get(topic);
            // request is in-flight, return future directly.
            if (null != inflightFutures) {
                inflightFutures.add(future0);
                return future0;
            }
            inflightFutures = new HashSet<SettableFuture<TopicRouteData>>();
            inflightFutures.add(future0);
            inflightRouteFutureTable.put(topic, inflightFutures);
        } finally {
            inflightRouteFutureLock.unlock();
        }
        final ListenableFuture<TopicRouteData> future = fetchTopicRoute(topic);
        Futures.addCallback(future, new FutureCallback<TopicRouteData>() {
            @Override
            public void onSuccess(TopicRouteData newTopicRouteData) {
                inflightRouteFutureLock.lock();
                try {
                    updateTopicRouteCache(topic, newTopicRouteData);
                    final Set<SettableFuture<TopicRouteData>> newFutureSet = inflightRouteFutureTable.remove(topic);
                    if (null == newFutureSet) {
                        // Should never reach here.
                        log.error("[Bug] in-flight route futures was empty, topic={}", topic);
                        return;
                    }
                    log.debug("Fetch topic route successfully, topic={}, in-flight route future size={}", topic,
                              newFutureSet.size());
                    for (SettableFuture<TopicRouteData> newFuture : newFutureSet) {
                        newFuture.set(newTopicRouteData);
                    }
                } finally {
                    inflightRouteFutureLock.unlock();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                inflightRouteFutureLock.lock();
                try {
                    final Set<SettableFuture<TopicRouteData>> newFutureSet = inflightRouteFutureTable.remove(topic);
                    if (null == newFutureSet) {
                        // Should never reach here.
                        log.error("[Bug] in-flight route futures was empty, topic={}", topic);
                        return;
                    }
                    log.error("Failed to fetch topic route, topic={}, in-flight route future size={}", topic,
                              newFutureSet.size(), t);
                    for (SettableFuture<TopicRouteData> newFuture : newFutureSet) {
                        final ClientException exception = new ClientException(ErrorCode.FETCH_TOPIC_ROUTE_FAILURE, t);
                        newFuture.setException(exception);
                    }
                } finally {
                    inflightRouteFutureLock.unlock();
                }
            }
        });
        return future0;
    }

    public void setNamesrvAddr(String namesrv) {
        nameServerEndpointsListLock.writeLock().lock();
        try {
            this.nameServerEndpointsList.clear();
            final String[] addressArray = namesrv.split(";");
            for (String address : addressArray) {
                // TODO: check name server format, IPv4/IPv6/DOMAIN_NAME
                final String[] split = address.split(":");
                String host = split[0];
                int port = Integer.parseInt(split[1]);

                List<Address> addresses = new ArrayList<Address>();
                addresses.add(new Address(host, port));
                this.nameServerEndpointsList.add(new Endpoints(AddressScheme.IPv4, addresses));
            }
        } finally {
            nameServerEndpointsListLock.writeLock().unlock();
        }
    }

    private Endpoints selectNameServerEndpoints() {
        nameServerEndpointsListLock.readLock().lock();
        try {
            return nameServerEndpointsList.get(nameServerIndex.get() % nameServerEndpointsList.size());
        } finally {
            nameServerEndpointsListLock.readLock().unlock();
        }
    }

    public ListenableFuture<TopicRouteData> fetchTopicRoute(final String topic) {
        final SettableFuture<TopicRouteData> future = SettableFuture.create();
        try {
            final Endpoints endpoints = selectNameServerEndpoints();
            Resource topicResource = Resource.newBuilder().setArn(arn).setName(topic).build();
            final QueryRouteRequest request = QueryRouteRequest.newBuilder().setTopic(topicResource).build();
            final Metadata metadata = sign();
            final ListenableFuture<QueryRouteResponse> responseFuture =
                    clientInstance.queryRoute(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
            return Futures.transformAsync(responseFuture, new AsyncFunction<QueryRouteResponse, TopicRouteData>() {
                @Override
                public ListenableFuture<TopicRouteData> apply(QueryRouteResponse response) throws Exception {
                    final Status status = response.getCommon().getStatus();
                    final Code code = Code.forNumber(status.getCode());
                    if (Code.OK != code) {
                        throw new ClientException(ErrorCode.TOPIC_NOT_FOUND, status.toString());
                    }
                    final List<apache.rocketmq.v1.Partition> partitionsList = response.getPartitionsList();
                    if (partitionsList.isEmpty()) {
                        throw new ClientException(ErrorCode.TOPIC_NOT_FOUND, "Partitions is empty unexpectedly.");
                    }
                    final TopicRouteData topicRouteData = new TopicRouteData(partitionsList);
                    future.set(topicRouteData);
                    return future;
                }
            });
        } catch (Throwable e) {
            future.setException(e);
            return future;
        }
    }

    public abstract HeartbeatEntry prepareHeartbeatData();

    @Override
    public void doHeartbeat() {
        final Set<Endpoints> routeEndpointsSet = getRouteEndpointsSet();
        if (routeEndpointsSet.isEmpty()) {
            log.info("No endpoints is needed to send heartbeat at present, clientId={}", clientId);
            return;
        }
        final HeartbeatRequest.Builder builder = HeartbeatRequest.newBuilder();
        final HeartbeatEntry heartbeatEntry = prepareHeartbeatData();
        if (null == heartbeatEntry) {
            log.info("No heartbeat entries to send, skip it, clientId={}", clientId);
            return;
        }
        builder.addHeartbeats(heartbeatEntry);
        final HeartbeatRequest request = builder.build();
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            return;
        }
        for (final Endpoints endpoints : routeEndpointsSet) {
            final ListenableFuture<HeartbeatResponse> future = clientInstance.heartbeat(endpoints, metadata, request,
                                                                                        ioTimeoutMillis,
                                                                                        TimeUnit.MILLISECONDS);
            Futures.addCallback(future, new FutureCallback<HeartbeatResponse>() {
                @Override
                public void onSuccess(HeartbeatResponse response) {
                    final Status status = response.getCommon().getStatus();
                    final Code code = Code.forNumber(status.getCode());
                    if (Code.OK != code) {
                        log.warn("Failed to send heartbeat, code={}, status message={}, endpoints={}", code,
                                 status.getMessage(), endpoints);
                        return;
                    }
                    log.info("Send heartbeat successfully, endpoints={}", endpoints);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.warn("Failed to send heartbeat, endpoints={}", endpoints, t);
                }
            });
        }
    }

    public abstract ClientResourceBundle wrapClientResourceBundle();

    public PullMessageRequest wrapPullMessageRequest(PullMessageQuery pullMessageQuery) {
        final MessageQueue messageQueue = pullMessageQuery.getMessageQueue();
        final long queueOffset = pullMessageQuery.getQueueOffset();
        final long awaitTimeMillis = pullMessageQuery.getAwaitTimeMillis();
        final int batchSize = pullMessageQuery.getBatchSize();
        final String arn = this.getArn();
        final Resource groupResource =
                Resource.newBuilder().setArn(arn).setName(group).build();

        final Resource topicResource = Resource.newBuilder().setArn(arn).setName(messageQueue.getTopic()).build();
        final apache.rocketmq.v1.Partition partition =
                apache.rocketmq.v1.Partition.newBuilder()
                                            .setTopic(topicResource)
                                            .setId(messageQueue.getPartition().getId())
                                            .build();
        final PullMessageRequest.Builder requestBuilder =
                PullMessageRequest.newBuilder().setClientId(clientId)
                                  .setAwaitTime(Durations.fromMillis(awaitTimeMillis))
                                  .setBatchSize(batchSize)
                                  .setGroup(groupResource)
                                  .setPartition(partition)
                                  .setOffset(queueOffset);

        final FilterExpression filterExpression = pullMessageQuery.getFilterExpression();
        apache.rocketmq.v1.FilterExpression.Builder filterExpressionBuilder =
                apache.rocketmq.v1.FilterExpression.newBuilder();
        switch (filterExpression.getExpressionType()) {
            case TAG:
                filterExpressionBuilder.setType(FilterType.TAG);
                break;
            case SQL92:
            default:
                filterExpressionBuilder.setType(FilterType.SQL);
        }
        filterExpressionBuilder.setExpression(filterExpression.getExpression());
        requestBuilder.setFilterExpression(filterExpressionBuilder.build());

        return requestBuilder.build();
    }

    public ListenableFuture<PullMessageResult> pull(final PullMessageQuery pullMessageQuery) {
        final PullMessageRequest request = wrapPullMessageRequest(pullMessageQuery);
        final SettableFuture<PullMessageResult> future0 = SettableFuture.create();
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            future0.setException(t);
            return future0;
        }
        final Endpoints endpoints = pullMessageQuery.getMessageQueue().getPartition().getBroker().getEndpoints();
        final long timeoutMillis = pullMessageQuery.getTimeoutMillis();
        final ListenableFuture<PullMessageResponse> future =
                clientInstance.pullMessage(endpoints, metadata, request, timeoutMillis, TimeUnit.MILLISECONDS);
        return Futures.transformAsync(future, new AsyncFunction<PullMessageResponse, PullMessageResult>() {
            @Override
            public ListenableFuture<PullMessageResult> apply(PullMessageResponse response) throws ClientException {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                // TODO: polish code.
                if (Code.OK != code) {
                    log.error("Failed to pull message, pullMessageQuery={}, code={}, message={}", pullMessageQuery,
                              code, status.getMessage());
                    throw new ClientException(ErrorCode.OTHER);
                }
                final PullMessageResult pullMessageResult = processPullMessageResponse(endpoints, response);
                future0.set(pullMessageResult);
                return future0;
            }
        });
    }

    public static PullMessageResult processPullMessageResponse(Endpoints endpoints, PullMessageResponse response) {
        PullStatus pullStatus;

        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        switch (code != null ? code : Code.UNKNOWN) {
            case OK:
                pullStatus = PullStatus.OK;
                break;
            case RESOURCE_EXHAUSTED:
                pullStatus = PullStatus.RESOURCE_EXHAUSTED;
                log.warn("Too many request in server, server endpoints={}, status message={}", endpoints,
                         status.getMessage());
                break;
            case DEADLINE_EXCEEDED:
                pullStatus = PullStatus.DEADLINE_EXCEEDED;
                log.warn("Gateway timeout, server endpoints={}, status message={}", endpoints, status.getMessage());
                break;
            case NOT_FOUND:
                pullStatus = PullStatus.NOT_FOUND;
                log.warn("Target partition does not exist, server endpoints={}, status message={}", endpoints,
                         status.getMessage());
                break;
            case OUT_OF_RANGE:
                pullStatus = PullStatus.OUT_OF_RANGE;
                log.warn("Pulled offset is out of range, server endpoints={}, status message{}", endpoints,
                         status.getMessage());
                break;
            default:
                pullStatus = PullStatus.INTERNAL;
                log.warn("Pull response indicated server-side error, server endpoints={}, code={}, status message{}",
                         endpoints, code, status.getMessage());
        }
        List<MessageExt> msgFoundList = new ArrayList<MessageExt>();
        if (PullStatus.OK == pullStatus) {
            final List<Message> messageList = response.getMessagesList();
            for (Message message : messageList) {
                try {
                    MessageImpl messageImpl = ClientBaseImpl.wrapMessageImpl(message);
                    msgFoundList.add(new MessageExt(messageImpl));
                } catch (ClientException e) {
                    log.error("Failed to wrap messageImpl, topic={}, messageId={}", message.getTopic(),
                              message.getSystemAttribute().getMessageId(), e);
                } catch (IOException e) {
                    log.error("Failed to wrap messageImpl, topic={}, messageId={}", message.getTopic(),
                              message.getSystemAttribute().getMessageId(), e);
                } catch (Throwable t) {
                    log.error("Exception raised while wrapping messageImpl, topic={}, messageId={}",
                              message.getTopic(), message.getSystemAttribute().getMessageId(), t);
                }
            }
        }
        return new PullMessageResult(pullStatus, response.getNextOffset(), response.getMinOffset(),
                                     response.getMaxOffset(), msgFoundList);
    }

    /**
     * Verify message's consumption ,the default is not supported, only would be implemented by push consumer.
     *
     * @param request request of verify message consumption.
     * @return future of verify message consumption response.
     */
    public ListenableFuture<VerifyMessageConsumptionResponse> verifyConsumption(VerifyMessageConsumptionRequest
                                                                                        request) {
        SettableFuture<VerifyMessageConsumptionResponse> future = SettableFuture.create();
        final ClientException exception = new ClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
        future.setException(exception);
        return future;
    }

    /**
     * Resolve orphaned transaction message, only would be implemented by producer.
     *
     * <p>SHOULD NEVER THROW ANY EXCEPTION.</p>
     *
     * @param endpoints remote endpoints.
     * @param request   resolve orphaned transaction request.
     */
    public void resolveOrphanedTransaction(Endpoints endpoints, ResolveOrphanedTransactionRequest request) {
    }

    public GenericPollingRequest wrapGenericPollingRequest() {
        final ClientResourceBundle bundle = wrapClientResourceBundle();
        return GenericPollingRequest.newBuilder().setClientResourceBundle(bundle).build();
    }

    private void onMultiplexingResponse(final Endpoints endpoints, MultiplexingResponse response) {
        switch (response.getTypeCase()) {
            case PRINT_THREAD_STACK_REQUEST:
                log.debug("Receive thread stack request from remote.");
                final String stackTrace = UtilAll.stackTrace();
                PrintThreadStackResponse printThreadStackResponse =
                        PrintThreadStackResponse.newBuilder().setStackTrace(stackTrace).build();
                MultiplexingRequest multiplexingRequest = MultiplexingRequest
                        .newBuilder().setPrintThreadStackResponse(printThreadStackResponse).build();
                multiplexingCall(endpoints, multiplexingRequest);
                log.debug("Send thread stack response to remote.");
                break;
            case VERIFY_MESSAGE_CONSUMPTION_REQUEST:
                log.debug("Receive verify message consumption request from remote.");
                VerifyMessageConsumptionRequest verifyRequest = response.getVerifyMessageConsumptionRequest();
                ListenableFuture<VerifyMessageConsumptionResponse> future = verifyConsumption(verifyRequest);

                ScheduledExecutorService scheduler = clientInstance.getScheduler();
                // In case block while message consumption.
                Futures.withTimeout(future, VERIFY_CONSUMPTION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS, scheduler);
                Futures.addCallback(future, new FutureCallback<VerifyMessageConsumptionResponse>() {
                    @Override
                    public void onSuccess(VerifyMessageConsumptionResponse response) {
                        MultiplexingRequest multiplexingRequest = MultiplexingRequest
                                .newBuilder().setVerifyMessageConsumptionResponse(response).build();
                        multiplexingCall(endpoints, multiplexingRequest);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        Status status = Status.newBuilder()
                                              .setCode(Code.ABORTED_VALUE)
                                              .setMessage(t.getMessage())
                                              .build();

                        ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
                        final VerifyMessageConsumptionResponse verifyResponse =
                                VerifyMessageConsumptionResponse.newBuilder().setCommon(common).build();
                        MultiplexingRequest multiplexingRequest =
                                MultiplexingRequest.newBuilder()
                                                   .setVerifyMessageConsumptionResponse(verifyResponse).build();
                        multiplexingCall(endpoints, multiplexingRequest);
                    }
                });
                break;
            case RESOLVE_ORPHANED_TRANSACTION_REQUEST:
                log.trace("Receive resolve orphaned transaction request from remote.");
                ResolveOrphanedTransactionRequest orphanedRequest = response.getResolveOrphanedTransactionRequest();
                resolveOrphanedTransaction(endpoints, orphanedRequest);
                /* fall through on purpose. */
            case POLLING_RESPONSE:
                log.trace("Receive polling response from remote.");
                /* fall through on purpose. */
            default:
                dispatchGenericPollRequest(endpoints);
                break;
        }
    }

    public void dispatchGenericPollRequest(Endpoints endpoints) {
        final GenericPollingRequest genericPollingRequest = wrapGenericPollingRequest();
        MultiplexingRequest multiplexingRequest =
                MultiplexingRequest.newBuilder().setPollingRequest(genericPollingRequest).build();
        log.debug("Start to dispatch generic poll request, endpoints={}", endpoints);
        multiplexingCall(endpoints, multiplexingRequest);
    }

    public void multiplexingCall(final Endpoints endpoints, final MultiplexingRequest request) {
        try {
            final Set<Endpoints> routeEndpointsSet = getRouteEndpointsSet();
            if (!routeEndpointsSet.contains(endpoints)) {
                log.info("Endpoints was removed, no need to do more multiplexing call, endpoints={}", endpoints);
                return;
            }
            final ListenableFuture<MultiplexingResponse> future = multiplexingCall0(endpoints, request);
            Futures.addCallback(future, new FutureCallback<MultiplexingResponse>() {
                @Override
                public void onSuccess(MultiplexingResponse response) {
                    try {
                        onMultiplexingResponse(endpoints, response);
                    } catch (Throwable t) {
                        // Should never reach here.
                        log.error("[Bug] Exception raised while handling multiplexing response, would call later, "
                                  + "endpoints={}.", endpoints, t);
                        multiplexingCallLater(endpoints, request);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Exception raised while multiplexing call, would call later, endpoints={}.", endpoints,
                              t);
                    multiplexingCallLater(endpoints, request);
                }
            });
        } catch (Throwable t) {
            log.error("Exception raised while multiplexing call, would call later, endpoints={}.", endpoints, t);
            multiplexingCallLater(endpoints, request);
        }
    }

    public ListenableFuture<MultiplexingResponse> multiplexingCall0(Endpoints endpoints, MultiplexingRequest request) {
        final SettableFuture<MultiplexingResponse> future = SettableFuture.create();
        try {
            final Metadata metadata = sign();
            return clientInstance.multiplexingCall(endpoints, metadata, request, MULTIPLEXING_CALL_TIMEOUT_MILLIS,
                                                   TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            // Failed to sign, set the future in advance.
            future.setException(t);
            return future;
        }
    }

    public void multiplexingCallLater(final Endpoints endpoints, final MultiplexingRequest request) {
        final ScheduledExecutorService scheduler = clientInstance.getScheduler();
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        multiplexingCall(endpoints, request);
                    } catch (Throwable t) {
                        multiplexingCallLater(endpoints, request);
                    }
                }
            }, MULTIPLEXING_CALL_LATER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            log.error("Failed to schedule multiplexing request", t);
            multiplexingCallLater(endpoints, request);
        }
    }

    private Set<Endpoints> getTracingEndpointsSet() {
        Set<Endpoints> tracingEndpointsSet = new HashSet<Endpoints>();
        for (TopicRouteData topicRouteData : topicRouteCache.values()) {
            final List<Partition> partitions = topicRouteData.getPartitions();
            for (Partition partition : partitions) {
                final Broker broker = partition.getBroker();
                if (MixAll.MASTER_BROKER_ID != broker.getId()) {
                    continue;
                }
                if (Permission.NONE == partition.getPermission()) {
                    continue;
                }
                tracingEndpointsSet.add(broker.getEndpoints());
            }
        }
        return tracingEndpointsSet;
    }

    private void updateTracer() {
        try {
            log.debug("Start to update tracer.");
            final Set<Endpoints> tracingEndpointsSet = getTracingEndpointsSet();
            if (tracingEndpointsSet.isEmpty()) {
                log.warn("No available tracing endpoints.");
                return;
            }
            if (null != tracingEndpoints && tracingEndpointsSet.contains(tracingEndpoints)) {
                log.debug("Tracing target remains unchanged");
                return;
            }
            List<Endpoints> tracingRpcTargetList = new ArrayList<Endpoints>(tracingEndpointsSet);
            Collections.shuffle(tracingRpcTargetList);
            // Pick up tracing rpc target randomly.
            final Endpoints randomTracingEndpoints = tracingRpcTargetList.iterator().next();
            final SslContext sslContext =
                    GrpcSslContexts.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

            final NettyChannelBuilder channelBuilder =
                    NettyChannelBuilder.forTarget(randomTracingEndpoints.getFacade())
                                       .sslContext(sslContext)
                                       .intercept(new ClientAuthInterceptor(this));

            final List<InetSocketAddress> socketAddresses = randomTracingEndpoints.convertToSocketAddresses();
            // If scheme is not domain.
            if (null != socketAddresses) {
                IpNameResolverFactory tracingResolverFactory = new IpNameResolverFactory(socketAddresses);
                channelBuilder.nameResolverFactory(tracingResolverFactory);
            }

            OtlpGrpcSpanExporter exporter =
                    OtlpGrpcSpanExporter.builder().setChannel(channelBuilder.build())
                                        .setTimeout(TRACE_EXPORTER_RPC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).build();

            BatchSpanProcessor spanProcessor =
                    BatchSpanProcessor.builder(exporter)
                                      .setScheduleDelay(TRACE_EXPORTER_SCHEDULE_DELAY_MILLIS, TimeUnit.MILLISECONDS)
                                      .setMaxExportBatchSize(TRACE_EXPORTER_BATCH_SIZE)
                                      .build();

            SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder().addSpanProcessor(spanProcessor).build();
            // TODO: no need propagators here.
            OpenTelemetrySdk openTelemetry =
                    OpenTelemetrySdk.builder()
                                    .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                                    .setTracerProvider(sdkTracerProvider).build();
            tracer = openTelemetry.getTracer(TRACER_INSTRUMENTATION_NAME);
            tracingEndpoints = randomTracingEndpoints;
        } catch (Throwable t) {
            log.error("Exception occurs while updating tracer.", t);
        }
    }

    public ListenableFuture<Long> queryOffset(final OffsetQuery offsetQuery) {
        final QueryOffsetRequest request = wrapQueryOffsetRequest(offsetQuery);
        final SettableFuture<Long> future0 = SettableFuture.create();
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            future0.setException(t);
            return future0;
        }
        final Partition partition = offsetQuery.getMessageQueue().getPartition();
        final Endpoints endpoints = partition.getBroker().getEndpoints();
        final ListenableFuture<QueryOffsetResponse> future =
                clientInstance.queryOffset(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        return Futures.transformAsync(future, new AsyncFunction<QueryOffsetResponse, Long>() {
            @Override
            public ListenableFuture<Long> apply(QueryOffsetResponse response) throws Exception {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                // TODO: polish code.
                if (Code.OK != code) {
                    log.error("Failed to query offset, offsetQuery={}, code={}, message={}", offsetQuery, code,
                              status.getMessage());
                    throw new ClientException(ErrorCode.OTHER);
                }
                final long offset = response.getOffset();
                future0.set(offset);
                return future0;
            }
        });
    }

    public QueryOffsetRequest wrapQueryOffsetRequest(OffsetQuery offsetQuery) {
        final QueryOffsetRequest.Builder builder = QueryOffsetRequest.newBuilder();
        final QueryOffsetPolicy queryOffsetPolicy = offsetQuery.getQueryOffsetPolicy();
        switch (queryOffsetPolicy) {
            case END:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.END);
                break;
            case TIME_POINT:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.TIME_POINT);
                builder.setTimePoint(Timestamps.fromMillis(offsetQuery.getTimePoint()));
                break;
            case BEGINNING:
            default:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.BEGINNING);
        }
        final MessageQueue messageQueue = offsetQuery.getMessageQueue();
        Resource topicResource = Resource.newBuilder().setArn(this.getArn()).setName(messageQueue.getTopic()).build();
        int partitionId = messageQueue.getPartition().getId();
        final apache.rocketmq.v1.Partition partition = apache.rocketmq.v1.Partition.newBuilder()
                                                                                   .setTopic(topicResource)
                                                                                   .setId(partitionId)
                                                                                   .build();
        return QueryOffsetRequest.newBuilder().setPartition(partition).build();
    }

    public static MessageImpl wrapMessageImpl(Message message) throws IOException, ClientException {
        MessageImpl impl = new MessageImpl(message.getTopic().getName());
        final SystemAttribute systemAttribute = message.getSystemAttribute();
        // Tag
        impl.getSystemAttribute().setTag(systemAttribute.getTag());
        // Key
        List<String> keys = new ArrayList<String>(systemAttribute.getKeysList());
        impl.getSystemAttribute().setKeys(keys);
        // Message Id
        impl.getSystemAttribute().setMessageId(systemAttribute.getMessageId());
        // Check digest.
        final apache.rocketmq.v1.Digest bodyDigest = systemAttribute.getBodyDigest();
        byte[] body = message.getBody().toByteArray();
        boolean bodyDigestMatch = false;
        String expectedCheckSum;
        DigestType digestType = DigestType.CRC32;
        final String checksum = bodyDigest.getChecksum();
        switch (bodyDigest.getType()) {
            case CRC32:
                expectedCheckSum = UtilAll.getCrc32CheckSum(body);
                if (expectedCheckSum.equals(checksum)) {
                    bodyDigestMatch = true;
                }
                break;
            case MD5:
                try {
                    expectedCheckSum = UtilAll.getMd5CheckSum(body);
                    if (expectedCheckSum.equals(checksum)) {
                        bodyDigestMatch = true;
                    }
                } catch (NoSuchAlgorithmException e) {
                    bodyDigestMatch = true;
                    log.warn("MD5 is not supported unexpectedly, skip it.");
                }
                break;
            case SHA1:
                try {
                    expectedCheckSum = UtilAll.getSha1CheckSum(body);
                    if (expectedCheckSum.equals(checksum)) {
                        bodyDigestMatch = true;
                    }
                } catch (NoSuchAlgorithmException e) {
                    bodyDigestMatch = true;
                    log.warn("SHA-1 is not supported unexpectedly, skip it.");
                }
                break;
            default:
                log.warn("Unsupported message body digest algorithm.");
        }
        if (!bodyDigestMatch) {
            // Need NACK immediately ?
            throw new ClientException("Message body checksum failure");
        }
        impl.getSystemAttribute().setDigest(new Digest(digestType, checksum));

        switch (systemAttribute.getBodyEncoding()) {
            case GZIP:
                body = UtilAll.uncompressBytesGzip(body);
                break;
            case SNAPPY:
                // TODO
                log.warn("SNAPPY encoding algorithm is not supported.");
                break;
            case IDENTITY:
                break;
            default:
                log.warn("Unsupported message encoding algorithm.");
        }
        // Body
        impl.setBody(body);

        MessageType messageType;
        // TODO: messageType not set yet.
        switch (systemAttribute.getMessageType()) {
            case NORMAL:
                messageType = MessageType.NORMAL;
                break;
            case FIFO:
                messageType = MessageType.FIFO;
                break;
            case DELAY:
                messageType = MessageType.DELAY;
                break;
            case TRANSACTION:
                messageType = MessageType.TRANSACTION;
                break;
            default:
                messageType = MessageType.NORMAL;
                log.warn("Unknown message type, fall through to normal type");
        }
        // MessageType
        impl.getSystemAttribute().setMessageType(messageType);
        // BornTimestamp
        impl.getSystemAttribute().setBornTimestamp(Timestamps.toMillis(systemAttribute.getBornTimestamp()));
        // BornHost
        impl.getSystemAttribute().setBornHost(systemAttribute.getBornHost());

        switch (systemAttribute.getTimedDeliveryCase()) {
            case DELAY_LEVEL:
                // DelayLevel
                impl.getSystemAttribute().setDelayLevel(systemAttribute.getDelayLevel());
                break;
            case DELIVERY_TIMESTAMP:
                // DelayTimestamp
                impl.getSystemAttribute()
                    .setDeliveryTimestamp(Timestamps.toMillis(systemAttribute.getDeliveryTimestamp()));
                break;
            case TIMEDDELIVERY_NOT_SET:
            default:
                break;
        }

        // DeliveryTimestamp
        impl.getSystemAttribute()
            .setDeliveryTimestamp(Timestamps.toMillis(systemAttribute.getDeliveryTimestamp()));
        // DecodedTimestamp
        impl.getSystemAttribute().setDecodedTimestamp(System.currentTimeMillis());
        // BornTimestamp
        impl.getSystemAttribute().setBornTimestamp(Timestamps.toMillis(systemAttribute.getBornTimestamp()));
        // ReceiptHandle
        impl.getSystemAttribute().setReceiptHandle(systemAttribute.getReceiptHandle());
        // PartitionId
        impl.getSystemAttribute().setPartitionId(systemAttribute.getPartitionId());
        // PartitionOffset
        impl.getSystemAttribute().setPartitionOffset(systemAttribute.getPartitionOffset());
        // InvisiblePeriod
        impl.getSystemAttribute().setInvisiblePeriod(Durations.toMillis(systemAttribute.getInvisiblePeriod()));
        // DeliveryCount
        impl.getSystemAttribute().setDeliveryAttempt(systemAttribute.getDeliveryAttempt());
        // ProducerGroup
        impl.getSystemAttribute().setProducerGroup(systemAttribute.getProducerGroup().getName());
        // TraceContext
        impl.getSystemAttribute().setTraceContext(systemAttribute.getTraceContext());
        // UserProperties
        impl.getUserAttribute().putAll(message.getUserAttributeMap());

        return impl;
    }

    @Override
    public void doHealthCheck() {
    }
}
