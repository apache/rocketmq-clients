package org.apache.rocketmq.client.impl;

import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SystemAttribute;
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
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.message.protocol.Digest;
import org.apache.rocketmq.client.message.protocol.DigestType;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.misc.TopAddressing;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.remoting.Address;
import org.apache.rocketmq.client.remoting.ClientAuthInterceptor;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.remoting.IpNameResolverFactory;
import org.apache.rocketmq.client.remoting.RpcTarget;
import org.apache.rocketmq.client.route.AddressScheme;
import org.apache.rocketmq.client.route.Broker;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.client.tracing.TracingMessageInterceptor;
import org.apache.rocketmq.utility.UtilAll;

@Slf4j
public abstract class ClientBaseImpl extends ClientConfig implements ClientObserver {
    @Getter
    protected volatile Tracer tracer;
    protected volatile RpcTarget tracingTarget;

    @Getter
    protected volatile ClientInstance clientInstance;

    private final AtomicReference<ServiceState> state;

    private final TopAddressing topAddressing;
    private final AtomicInteger nameServerIndex;

    @GuardedBy("messageInterceptorsLock")
    private final List<MessageInterceptor> messageInterceptors;
    private final ReadWriteLock messageInterceptorsLock;

    @GuardedBy("nameServerListLock")
    private final List<Endpoints> nameServerList;
    private final ReadWriteLock nameServerListLock;

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
        this.tracingTarget = null;

        this.topAddressing = new TopAddressing();
        this.nameServerIndex = new AtomicInteger(RandomUtils.nextInt());

        this.messageInterceptors = new ArrayList<MessageInterceptor>();
        this.messageInterceptorsLock = new ReentrantReadWriteLock();

        this.nameServerList = new ArrayList<Endpoints>();
        this.nameServerListLock = new ReentrantReadWriteLock();

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

    public void start() throws MQClientException {
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
                                    log.error("Unexpected errors while updating nameserver from top addressing", t);
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
                                log.error("Unexpected error while updating topic route cache", t);
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

    public void interceptMessage(MessageHookPoint hookPoint, MessageExt messageExt,
                                 MessageInterceptorContext context) {
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

    @Override
    public Set<Endpoints> getEndpointsNeedHeartbeat() {
        Set<Endpoints> endpointsSet = new HashSet<Endpoints>();
        for (TopicRouteData topicRouteData : topicRouteCache.values()) {
            endpointsSet.addAll(topicRouteData.getAllEndpoints());
        }
        return endpointsSet;
    }

    public boolean nameServerIsNotSet() {
        nameServerListLock.readLock().lock();
        try {
            return nameServerList.isEmpty();
        } finally {
            nameServerListLock.readLock().unlock();
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
        nameServerListLock.writeLock().lock();
        try {
            if (nameServerList == newNameServerList) {
                log.debug("Name server list remains no changed");
                return;
            }
            nameServerList.clear();
            nameServerList.addAll(newNameServerList);
        } finally {
            nameServerListLock.writeLock().unlock();
        }
    }

    protected void updateTopicRouteCache(String topic, TopicRouteData topicRouteData) {
        topicRouteCache.put(topic, topicRouteData);
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
                        return;
                    }
                    log.error("Failed to fetch topic route, topic={}, in-flight route future size={}", topic,
                              newFutureSet.size(), t);
                    for (SettableFuture<TopicRouteData> newFuture : newFutureSet) {
                        newFuture.setException(t);
                    }
                } finally {
                    inflightRouteFutureLock.unlock();
                }
            }
        });
        return future;
    }

    public void setNamesrvAddr(String namesrv) {
        nameServerListLock.writeLock().lock();
        try {
            this.nameServerList.clear();
            final String[] addressArray = namesrv.split(";");
            for (String address : addressArray) {
                // TODO: check name server format, IPv4/IPv6/DOMAIN_NAME
                final String[] split = address.split(":");
                String host = split[0];
                int port = Integer.parseInt(split[1]);

                List<Address> addresses = new ArrayList<Address>();
                addresses.add(new Address(host, port));
                this.nameServerList.add(new Endpoints(AddressScheme.IPv4, addresses));
            }
        } finally {
            nameServerListLock.writeLock().unlock();
        }
    }

    private Endpoints selectNameServer() {
        nameServerListLock.readLock().lock();
        try {
            return nameServerList.get(nameServerIndex.get() % nameServerList.size());
        } finally {
            nameServerListLock.readLock().unlock();
        }
    }

    public ListenableFuture<TopicRouteData> fetchTopicRoute(final String topic) {
        final SettableFuture<TopicRouteData> future = SettableFuture.create();
        try {
            final Endpoints nameServerEndpoints = selectNameServer();
            final RpcTarget target = new RpcTarget(nameServerEndpoints);
            Resource topicResource = Resource.newBuilder().setArn(arn).setName(topic).build();
            final QueryRouteRequest request = QueryRouteRequest.newBuilder().setTopic(topicResource).build();
            final Metadata metadata = sign();
            final ListenableFuture<QueryRouteResponse> responseFuture =
                    clientInstance.queryRoute(target, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
            return Futures.transformAsync(responseFuture, new AsyncFunction<QueryRouteResponse, TopicRouteData>() {
                @Override
                public ListenableFuture<TopicRouteData> apply(QueryRouteResponse response) throws Exception {
                    final Status status = response.getCommon().getStatus();
                    final Code code = Code.forNumber(status.getCode());
                    if (Code.OK != code) {
                        throw new MQClientException(ErrorCode.TOPIC_NOT_FOUND, status.toString());
                    }
                    final List<apache.rocketmq.v1.Partition> partitionsList = response.getPartitionsList();
                    if (partitionsList.isEmpty()) {
                        throw new MQClientException(ErrorCode.TOPIC_NOT_FOUND, "Partitions is empty unexpectedly");
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

    @Override
    public void doHeartbeat() {
        final Set<Endpoints> endpointsNeedHeartbeat = getEndpointsNeedHeartbeat();
        if (endpointsNeedHeartbeat.isEmpty()) {
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
        for (final Endpoints endpoints : endpointsNeedHeartbeat) {
            final RpcTarget target = new RpcTarget(endpoints, true);
            final ListenableFuture<HeartbeatResponse> future = clientInstance.heartbeat(target, metadata, request,
                                                                                        ioTimeoutMillis,
                                                                                        TimeUnit.MILLISECONDS);
            Futures.addCallback(future, new FutureCallback<HeartbeatResponse>() {
                @Override
                public void onSuccess(HeartbeatResponse response) {
                    final Status status = response.getCommon().getStatus();
                    final Code code = Code.forNumber(status.getCode());
                    if (Code.OK != code) {
                        log.warn("Failed to send heartbeat, code={}, message={}, endpoints={}", code,
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

    public Metadata sign() throws MQClientException {
        try {
            return Signature.sign(this);
        } catch (Throwable t) {
            log.error("Failed to calculate signature", t);
            throw new MQClientException(ErrorCode.SIGNATURE_FAILURE);
        }
    }

    private Set<RpcTarget> getTracingTargets() {
        Set<RpcTarget> tracingTargets = new HashSet<RpcTarget>();
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
                tracingTargets.add(broker.getTarget());
            }
        }
        return tracingTargets;
    }

    private void updateTracer() {
        try {
            log.info("Start to update tracer.");
            final Set<RpcTarget> tracingTargets = getTracingTargets();
            if (tracingTargets.isEmpty()) {
                log.warn("No available tracing targets.");
                return;
            }
            if (null != tracingTarget && tracingTargets.contains(tracingTarget)) {
                log.info("Tracing target remains unchanged");
                return;
            }
            List<RpcTarget> tracingRpcTargetList = new ArrayList<RpcTarget>(tracingTargets);
            Collections.shuffle(tracingRpcTargetList);
            // Pick up tracing rpc target randomly.
            final RpcTarget randomTracingTarget = tracingRpcTargetList.iterator().next();
            final SslContext sslContext =
                    GrpcSslContexts.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

            final NettyChannelBuilder channelBuilder =
                    NettyChannelBuilder.forTarget(randomTracingTarget.getEndpoints().getFacade())
                                       .sslContext(sslContext)
                                       .intercept(new ClientAuthInterceptor(this));

            final List<InetSocketAddress> socketAddresses =
                    randomTracingTarget.getEndpoints().convertToSocketAddresses();
            // If scheme is not domain.
            if (null != socketAddresses) {
                IpNameResolverFactory tracingResolverFactory = new IpNameResolverFactory(socketAddresses);
                channelBuilder.nameResolverFactory(tracingResolverFactory);
            }

            OtlpGrpcSpanExporter exporter =
                    OtlpGrpcSpanExporter.builder().setChannel(channelBuilder.build())
                                        .setTimeout(MixAll.DEFAULT_EXPORTER_RPC_TIMEOUT_MILLIS,
                                                    TimeUnit.MILLISECONDS).build();
            BatchSpanProcessor spanProcessor =
                    BatchSpanProcessor.builder(exporter)
                                      .setScheduleDelay(MixAll.DEFAULT_EXPORTER_SCHEDULE_DELAY_TIME_MILLIS,
                                                        TimeUnit.MILLISECONDS)
                                      .setMaxExportBatchSize(MixAll.DEFAULT_EXPORTER_BATCH_SIZE)
                                      .build();

            SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder().addSpanProcessor(spanProcessor).build();
            // TODO: no need propagators here.
            OpenTelemetrySdk openTelemetry =
                    OpenTelemetrySdk.builder()
                                    .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                                    .setTracerProvider(sdkTracerProvider).build();
            tracer = openTelemetry.getTracer(MixAll.DEFAULT_TRACER_INSTRUMENTATION_NAME);
            tracingTarget = randomTracingTarget;
        } catch (Throwable t) {
            log.error("Exception occurs while updating tracer.", t);
        }
    }

    public static MessageImpl wrapMessageImpl(Message message) throws IOException, MQClientException {
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
            throw new MQClientException("Message body checksum failure");
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
        impl.getSystemAttribute()
            .setInvisiblePeriod(Durations.toMillis(systemAttribute.getInvisiblePeriod()));
        // DeliveryCount
        impl.getSystemAttribute().setDeliveryCount(systemAttribute.getDeliveryCount());
        // ProducerGroup
        impl.getSystemAttribute().setProducerGroup(systemAttribute.getProducerGroup().getName());
        // TraceContext
        impl.getSystemAttribute().setTraceContext(systemAttribute.getTraceContext());
        // UserProperties
        impl.getUserAttribute().putAll(message.getUserAttributeMap());

        return impl;
    }

    // TODO: handle the case that the topic does not exist.
    public static PopResult processReceiveMessageResponse(RpcTarget target, ReceiveMessageResponse response) {
        PopStatus popStatus;

        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        switch (code != null ? code : Code.UNKNOWN) {
            case OK:
                popStatus = PopStatus.OK;
                break;
            case RESOURCE_EXHAUSTED:
                popStatus = PopStatus.RESOURCE_EXHAUSTED;
                log.warn("Too many request in server, server endpoints={}, status message={}", target.getEndpoints(),
                         status.getMessage());
                break;
            case DEADLINE_EXCEEDED:
                popStatus = PopStatus.DEADLINE_EXCEEDED;
                log.warn("Gateway timeout, server endpoints={}, status message={}", target.getEndpoints(),
                         status.getMessage());
                break;
            default:
                popStatus = PopStatus.INTERNAL;
                log.warn("Pop response indicated server-side error, server endpoints={}, code={}, status message={}",
                         target.getEndpoints(), code, status.getMessage());
        }

        List<MessageExt> msgFoundList = new ArrayList<MessageExt>();
        if (PopStatus.OK == popStatus) {
            final List<Message> messageList = response.getMessagesList();
            for (Message message : messageList) {
                try {
                    MessageImpl messageImpl = ClientBaseImpl.wrapMessageImpl(message);
                    messageImpl.getSystemAttribute().setAckTarget(target);
                    msgFoundList.add(new MessageExt(messageImpl));
                } catch (MQClientException e) {
                    // TODO: need nack immediately.
                    log.error("Failed to wrap messageImpl, topic={}, messageId={}", message.getTopic(),
                              message.getSystemAttribute().getMessageId(), e);
                } catch (IOException e) {
                    log.error("Failed to wrap messageImpl, topic={}, messageId={}", message.getTopic(),
                              message.getSystemAttribute().getMessageId(), e);
                } catch (Throwable t) {
                    log.error("Unexpected error while wrapping messageImpl, topic={}, messageId={}",
                              message.getTopic(), message.getSystemAttribute().getMessageId(), t);
                }
            }
        }

        return new PopResult(target, popStatus, Timestamps.toMillis(response.getDeliveryTimestamp()),
                             Durations.toMillis(response.getInvisibleDuration()), msgFoundList);
    }


    public static PullResult processPullMessageResponse(RpcTarget target, PullMessageResponse response) {
        PullStatus pullStatus;

        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        switch (code != null ? code : Code.UNKNOWN) {
            case OK:
                pullStatus = PullStatus.OK;
                break;
            case RESOURCE_EXHAUSTED:
                pullStatus = PullStatus.RESOURCE_EXHAUSTED;
                log.warn("Too many request in server, server endpoints={}, status message={}", target.getEndpoints(),
                         status.getMessage());
                break;
            case DEADLINE_EXCEEDED:
                pullStatus = PullStatus.DEADLINE_EXCEEDED;
                log.warn("Gateway timeout, server endpoints={}, status message={}", target.getEndpoints(),
                         status.getMessage());
                break;
            case NOT_FOUND:
                pullStatus = PullStatus.NOT_FOUND;
                log.warn("Target partition does not exist, server endpoints={}, status message={}",
                         target.getEndpoints(), status.getMessage());
                break;
            case OUT_OF_RANGE:
                pullStatus = PullStatus.OUT_OF_RANGE;
                log.warn("Pulled offset is out of range, server endpoints={}, status message{}",
                         target.getEndpoints(), status.getMessage());
                break;
            default:
                pullStatus = PullStatus.INTERNAL;
                log.warn("Pull response indicated server-side error, server endpoints={}, code={}, status message{}",
                         target.getEndpoints(), code, status.getMessage());
        }
        List<MessageExt> msgFoundList = new ArrayList<MessageExt>();
        if (PullStatus.OK == pullStatus) {
            final List<Message> messageList = response.getMessagesList();
            for (Message message : messageList) {
                try {
                    MessageImpl messageImpl = ClientBaseImpl.wrapMessageImpl(message);
                    msgFoundList.add(new MessageExt(messageImpl));
                } catch (MQClientException e) {
                    log.error("Failed to wrap messageImpl, topic={}, messageId={}", message.getTopic(),
                              message.getSystemAttribute().getMessageId(), e);
                } catch (IOException e) {
                    log.error("Failed to wrap messageImpl, topic={}, messageId={}", message.getTopic(),
                              message.getSystemAttribute().getMessageId(), e);
                } catch (Throwable t) {
                    log.error("Unexpected error while wrapping messageImpl, topic={}, messageId={}",
                              message.getTopic(), message.getSystemAttribute().getMessageId(), t);
                }
            }
        }
        return new PullResult(pullStatus, response.getNextOffset(), response.getMinOffset(), response.getMaxOffset(),
                              msgFoundList);
    }

    public static SendResult processSendResponse(RpcTarget target, SendMessageResponse response)
            throws MQServerException {
        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        if (Code.OK == code) {
            return new SendResult(target, response.getMessageId(), response.getTransactionId());
        }
        log.debug("Response indicates failure of sending message, information={}", status.getMessage());
        throw new MQServerException(ErrorCode.OTHER, status.getMessage());
    }
}
