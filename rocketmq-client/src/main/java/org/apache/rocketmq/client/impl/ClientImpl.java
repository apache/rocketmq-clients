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

package org.apache.rocketmq.client.impl;

import apache.rocketmq.v1.ClientResourceBundle;
import apache.rocketmq.v1.GenericPollingRequest;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.MultiplexingRequest;
import apache.rocketmq.v1.MultiplexingResponse;
import apache.rocketmq.v1.PrintThreadStackResponse;
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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.message.protocol.Digest;
import org.apache.rocketmq.client.message.protocol.DigestType;
import org.apache.rocketmq.client.message.protocol.Encoding;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.misc.TopAddressing;
import org.apache.rocketmq.client.misc.Validators;
import org.apache.rocketmq.client.remoting.AuthInterceptor;
import org.apache.rocketmq.client.remoting.IpNameResolverFactory;
import org.apache.rocketmq.client.route.Address;
import org.apache.rocketmq.client.route.AddressScheme;
import org.apache.rocketmq.client.route.Broker;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.Permission;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.client.tracing.TracingMessageInterceptor;
import org.apache.rocketmq.utility.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ClientImpl extends ClientConfig implements ClientObserver, MessageInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ClientImpl.class);

    /**
     * Delay interval while multiplexing call encounters failure.
     */
    private static final long MULTIPLEXING_CALL_LATER_DELAY_MILLIS = 3 * 1000L;

    /**
     * Maximum time allowed client to execute multiplexing call before being cancelled.
     */
    private static final long MULTIPLEXING_CALL_TIMEOUT_MILLIS = 60 * 1000L;

    /**
     * For {@link DefaultMQPushConsumer} only, maximum time allowed consumer to verify specified message's consumption.
     */
    private static final long VERIFY_CONSUMPTION_TIMEOUT_MILLIS = 15 * 1000L;

    /**
     * Name for tracer. See <a href="https://opentelemetry.io">OpenTelemetry</a> for more details.
     */
    private static final String TRACER_INSTRUMENTATION_NAME = "org.apache.rocketmq.message.tracer";

    /**
     * Delay interval between two consecutive trace span exports to collector. See
     * <a href="https://opentelemetry.io">OpenTelemetry</a> for more details.
     */
    private static final long TRACE_EXPORTER_SCHEDULE_DELAY_MILLIS = 500L;

    /**
     * Maximum time to wait for the collector to process an exported batch of spans. See
     * <a href="https://opentelemetry.io">OpenTelemetry</a> for more details.
     */
    private static final long TRACE_EXPORTER_RPC_TIMEOUT_MILLIS = 3 * 1000L;

    /**
     * Maximum batch size for every export of span, must be smaller than {@link #TRACE_EXPORTER_MAX_QUEUE_SIZE}.
     * See <a href="https://opentelemetry.io">OpenTelemetry</a> for more details.
     */
    private static final int TRACE_EXPORTER_BATCH_SIZE = 2048;

    /**
     * Maximum number of {@link Span} that are kept in the queue before start dropping. See
     * <a href="https://opentelemetry.io">OpenTelemetry</a> for more details.
     */
    private static final int TRACE_EXPORTER_MAX_QUEUE_SIZE = 16384;

    protected volatile ClientManager clientManager;

    /**
     * OpenTelemetry tracer for message tracing.
     */
    protected volatile Tracer tracer;
    protected volatile Endpoints messageTracingEndpoints;
    private volatile SdkTracerProvider messageTracerProvider;

    private volatile ServiceState state;

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

    /**
     * Cache all used topic route, route cache would be updated periodically from name server.
     */
    private final ConcurrentMap<String /* topic */, TopicRouteData> topicRouteCache;

    public ClientImpl(String group) throws ClientException {
        super(group);
        this.state = ServiceState.READY;

        this.tracer = null;
        this.messageTracingEndpoints = null;
        this.messageTracerProvider = null;

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

    /**
     * Underlying implement could intercept the {@link TopicRouteData}.
     *
     * @param topic          topic's name
     * @param topicRouteData route data of specified topic.
     */
    public abstract void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData);

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
            switch (state) {
                case READY:
                    log.info("Begin to start the rocketmq client, clientId={}", clientId);
                    this.state = ServiceState.STARTING;
                    if (null == clientManager) {
                        clientManager = ClientManagerFactory.getInstance().registerObserver(arn, this);
                    }

                    if (messageTracingEnabled) {
                        final TracingMessageInterceptor interceptor = new TracingMessageInterceptor(this);
                        registerMessageInterceptor(interceptor);
                    }

                    final ScheduledExecutorService scheduler = clientManager.getScheduler();
                    if (isNameServerNotSet()) {
                        // acquire name server list immediately.
                        renewNameServerList();

                        log.info("Name server list was not set, schedule a task to fetch and renew periodically");
                        renewNameServerListFuture = scheduler.scheduleWithFixedDelay(
                                new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            renewNameServerList();
                                        } catch (Throwable t) {
                                            log.error("Exception raised while updating nameserver from top "
                                                      + "addressing", t);
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
                    this.state = ServiceState.STARTED;
                    log.info("The rocketmq client starts successfully, clientId={}", clientId);
                    break;
                case STARTING:
                case STARTED:
                case STOPPING:
                case STOPPED:
                default:
                    throw new ClientException(ErrorCode.STARTED_BEFORE, "client may has been started before.");
            }
        }
    }

    public void shutdown() throws InterruptedException {
        synchronized (this) {
            switch (state) {
                case STARTED:
                    log.info("Begin to shutdown the rocketmq client, clientId={}", clientId);
                    this.state = ServiceState.STOPPING;
                    if (null != renewNameServerListFuture) {
                        renewNameServerListFuture.cancel(false);
                    }
                    if (null != updateRouteCacheFuture) {
                        updateRouteCacheFuture.cancel(false);
                    }
                    ClientManagerFactory.getInstance().unregisterObserver(arn, this);
                    if (null != messageTracerProvider) {
                        messageTracerProvider.shutdown();
                    }
                    this.state = ServiceState.STOPPED;
                    log.info("Shutdown the rocketmq client successfully, clientId={}", clientId);
                    break;
                case READY:
                    log.info("The rocketmq client has not been started before, clientId={}", clientId);
                    break;
                case STARTING:
                case STOPPING:
                    log.error("[Bug] The rocketmq client state is abnormal, clientId={}, state={}", clientId, state);
                    break;
                case STOPPED:
                    log.info("The rocketmq client has been shutdown before, clientId={}", clientId);
                    // fall through on purpose.
                default:
                    break;
            }
        }
    }

    @Override
    public void intercept(MessageHookPoint hookPoint, MessageExt messageExt, MessageInterceptorContext context) {
        messageInterceptorsLock.readLock().lock();
        try {
            for (MessageInterceptor interceptor : messageInterceptors) {
                try {
                    interceptor.intercept(hookPoint, messageExt, context);
                } catch (Throwable t) {
                    log.error("Exception raised while intercepting message, hookPoint={}, message={}", hookPoint,
                              messageExt, t);
                }
            }
        } finally {
            messageInterceptorsLock.readLock().unlock();
        }
    }

    public ScheduledExecutorService getScheduler() {
        return clientManager.getScheduler();
    }

    public boolean isStarted() {
        return ServiceState.STARTED.equals(state);
    }

    public boolean isStopped() {
        return ServiceState.STOPPED.equals(state);
    }

    public Metadata sign() throws ClientException {
        try {
            return Signature.sign(this);
        } catch (Throwable t) {
            log.error("Failed to calculate signature", t);
            throw new ClientException(ErrorCode.SIGNATURE_FAILURE, t);
        }
    }

    protected Set<Endpoints> getRouteEndpointsSet() {
        Set<Endpoints> endpointsSet = new HashSet<Endpoints>();
        for (TopicRouteData topicRouteData : topicRouteCache.values()) {
            endpointsSet.addAll(topicRouteData.allEndpoints());
        }
        return endpointsSet;
    }

    private boolean isNameServerNotSet() {
        nameServerEndpointsListLock.readLock().lock();
        try {
            return nameServerEndpointsList.isEmpty();
        } finally {
            nameServerEndpointsListLock.readLock().unlock();
        }
    }

    private void renewNameServerList() {
        log.info("Start to renew name server list for a new round");
        List<Endpoints> newNameServerEndpointsList;
        try {
            newNameServerEndpointsList = topAddressing.fetchNameServerAddresses();
        } catch (Throwable t) {
            log.error("Failed to fetch name server list from top addressing", t);
            return;
        }
        if (newNameServerEndpointsList.isEmpty()) {
            log.warn("Got an empty name server list.");
            return;
        }
        nameServerEndpointsListLock.writeLock().lock();
        try {
            if (nameServerEndpointsList.equals(newNameServerEndpointsList)) {
                log.debug("Name server list remains no changed, name server list={}", nameServerEndpointsList);
                return;
            }
            nameServerEndpointsList.clear();
            nameServerEndpointsList.addAll(newNameServerEndpointsList);
        } finally {
            nameServerEndpointsListLock.writeLock().unlock();
        }
    }

    private void onTopicRouteDataUpdate(String topic, TopicRouteData topicRouteData) {
        onTopicRouteDataUpdate0(topic, topicRouteData);
        final Set<Endpoints> before = getRouteEndpointsSet();
        topicRouteCache.put(topic, topicRouteData);
        final Set<Endpoints> after = getRouteEndpointsSet();
        final Set<Endpoints> diff = new HashSet<Endpoints>(Sets.difference(after, before));

        for (Endpoints endpoints : diff) {
            log.info("Start multiplexing call for new endpoints={}", endpoints);
            dispatchGenericPollRequest(endpoints);
        }

        final HeartbeatRequest request = wrapHeartbeatRequest();
        for (Endpoints endpoints : diff) {
            log.info("Start to send heartbeat to new endpoints={}", endpoints);
            doHeartbeat(request, endpoints);
        }
        if (!messageTracingEnabled) {
            return;
        }
        updateMessageTracer();
    }

    private void updateRouteCache() {
        log.info("Start to update route cache for a new round");
        for (final String topic : topicRouteCache.keySet()) {
            final ListenableFuture<TopicRouteData> future = fetchTopicRoute(topic);
            Futures.addCallback(future, new FutureCallback<TopicRouteData>() {
                @Override
                public void onSuccess(TopicRouteData topicRouteData) {
                    onTopicRouteDataUpdate(topic, topicRouteData);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Failed to fetch topic route, topic={}", topic, t);
                }
            });
        }
    }

    protected ListenableFuture<TopicRouteData> getRouteData(final String topic) {
        SettableFuture<TopicRouteData> future0 = SettableFuture.create();
        TopicRouteData topicRouteData = topicRouteCache.get(topic);
        // if route was cached before, get it directly.
        if (null != topicRouteData) {
            future0.set(topicRouteData);
            return future0;
        }
        inflightRouteFutureLock.lock();
        try {
            // if route was fetched by last in-flight route request, get it directly.
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
                    onTopicRouteDataUpdate(topic, newTopicRouteData);
                    final Set<SettableFuture<TopicRouteData>> newFutureSet = inflightRouteFutureTable.remove(topic);
                    if (null == newFutureSet) {
                        // should never reach here.
                        log.error("[Bug] in-flight route futures was empty, topic={}", topic);
                        return;
                    }
                    log.debug("Fetch topic route successfully, topic={}, in-flight route future size={}", topic,
                              newFutureSet.size());
                    for (SettableFuture<TopicRouteData> newFuture : newFutureSet) {
                        newFuture.set(newTopicRouteData);
                    }
                } catch (Throwable t) {
                    // should never reach here.
                    log.error("[Bug] Exception raises while update route data, clientId={}, arn={}, topic={}",
                              clientId, arn, topic, t);
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
                        // should never reach here.
                        log.error("[Bug] in-flight route futures was empty, arn={}, topic={}", arn, topic);
                        return;
                    }
                    log.error("Failed to fetch topic route, arn={}, topic={}, in-flight route future size={}", arn,
                              topic, newFutureSet.size(), t);
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

    public void setNamesrvAddr(String namesrv) throws ClientException {
        Validators.checkNamesrvAddr(namesrv);
        nameServerEndpointsListLock.writeLock().lock();
        try {
            this.nameServerEndpointsList.clear();

            boolean httpPatternMatched = false;
            String httpPrefixMatched = "";
            if (namesrv.startsWith(MixAll.HTTP_PREFIX)) {
                httpPatternMatched = true;
                httpPrefixMatched = MixAll.HTTP_PREFIX;
            } else if (namesrv.startsWith(MixAll.HTTPS_PREFIX)) {
                httpPatternMatched = true;
                httpPrefixMatched = MixAll.HTTPS_PREFIX;
            }
            // for http pattern.
            if (httpPatternMatched) {
                final String domainName = namesrv.substring(httpPrefixMatched.length());
                final String[] domainNameSplit = domainName.split(":");
                String host = domainNameSplit[0].replace("_", "-").toLowerCase(UtilAll.LOCALE);
                final String[] hostSplit = host.split("\\.");
                if (hostSplit.length >= 2) {
                    this.setRegionId(hostSplit[1]);
                }
                int port = domainNameSplit.length >= 2 ? Integer.parseInt(domainNameSplit[1]) : 80;
                List<Address> addresses = new ArrayList<Address>();
                addresses.add(new Address(host, port));
                this.nameServerEndpointsList.add(new Endpoints(AddressScheme.DOMAIN_NAME, addresses));

                // arn is set before.
                if (StringUtils.isNotBlank(arn)) {
                    return;
                }
                if (Validators.NAME_SERVER_ENDPOINT_WITH_NAMESPACE_PATTERN.matcher(namesrv).matches()) {
                    this.arn = namesrv.substring(namesrv.lastIndexOf('/') + 1, namesrv.indexOf('.'));
                }
                return;
            }
            // for ip pattern.
            try {
                final String[] addressArray = namesrv.split(";");
                for (String address : addressArray) {
                    final String[] split = address.split(":");
                    String host = split[0];
                    int port = Integer.parseInt(split[1]);
                    List<Address> addresses = new ArrayList<Address>();
                    addresses.add(new Address(host, port));
                    this.nameServerEndpointsList.add(new Endpoints(AddressScheme.IPv4, addresses));
                }
            } catch (Throwable t) {
                log.error("Exception raises while parse name server address.", t);
                throw new ClientException(ErrorCode.ILLEGAL_FORMAT, t);
            }
        } finally {
            nameServerEndpointsListLock.writeLock().unlock();
        }
    }

    private Endpoints selectNameServerEndpoints() throws ClientException {
        nameServerEndpointsListLock.readLock().lock();
        try {
            if (nameServerEndpointsList.isEmpty()) {
                throw new ClientException(ErrorCode.NO_AVAILABLE_NAME_SERVER);
            }
            return nameServerEndpointsList.get(UtilAll.positiveMod(nameServerIndex.get(),
                                                                   nameServerEndpointsList.size()));
        } finally {
            nameServerEndpointsListLock.readLock().unlock();
        }
    }

    public Resource getPbGroup() {
        return Resource.newBuilder().setArn(arn).setName(group).build();
    }

    public ListenableFuture<TopicRouteData> fetchTopicRoute(final String topic) {
        final SettableFuture<TopicRouteData> future = SettableFuture.create();
        try {
            final Endpoints endpoints = selectNameServerEndpoints();
            Resource topicResource = Resource.newBuilder().setArn(arn).setName(topic).build();
            final QueryRouteRequest request =
                    QueryRouteRequest.newBuilder().setTopic(topicResource).setEndpoints(endpoints.toPbEndpoints())
                                     .build();
            final Metadata metadata = sign();
            final ListenableFuture<QueryRouteResponse> responseFuture =
                    clientManager.queryRoute(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
            Futures.addCallback(responseFuture, new FutureCallback<QueryRouteResponse>() {
                @Override
                public void onSuccess(QueryRouteResponse response) {
                }

                @Override
                public void onFailure(Throwable t) {
                    // select different name server endpoints for next time.
                    nameServerIndex.getAndIncrement();
                }
            });
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

    private void doHeartbeat(HeartbeatRequest request, final Endpoints endpoints) {
        try {
            Metadata metadata;
            try {
                metadata = sign();
            } catch (Throwable t) {
                return;
            }
            final ListenableFuture<HeartbeatResponse> future = clientManager
                    .heartbeat(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
            Futures.addCallback(future, new FutureCallback<HeartbeatResponse>() {
                @Override
                public void onSuccess(HeartbeatResponse response) {
                    final Status status = response.getCommon().getStatus();
                    final Code code = Code.forNumber(status.getCode());
                    if (Code.OK != code) {
                        log.warn("Failed to send heartbeat, code={}, status message=[{}], endpoints={}", code,
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
        } catch (Throwable e) {
            log.error("Unexpected exception raised while heartbeat, endpoints={}.", endpoints, e);
        }
    }

    private HeartbeatRequest wrapHeartbeatRequest() {
        final HeartbeatRequest.Builder builder = HeartbeatRequest.newBuilder();
        final HeartbeatEntry heartbeatEntry = prepareHeartbeatData();
        builder.addHeartbeats(heartbeatEntry);
        return builder.build();
    }

    @Override
    public void doHeartbeat() {
        final Set<Endpoints> routeEndpointsSet = getRouteEndpointsSet();
        final HeartbeatRequest request = wrapHeartbeatRequest();
        for (Endpoints endpoints : routeEndpointsSet) {
            doHeartbeat(request, endpoints);
        }
    }

    public abstract ClientResourceBundle wrapClientResourceBundle();


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

    private GenericPollingRequest wrapGenericPollingRequest() {
        final ClientResourceBundle bundle = wrapClientResourceBundle();
        return GenericPollingRequest.newBuilder().setClientResourceBundle(bundle).build();
    }

    private void onMultiplexingResponse(final Endpoints endpoints, final MultiplexingResponse response) {
        switch (response.getTypeCase()) {
            case PRINT_THREAD_STACK_REQUEST:
                String mid = response.getPrintThreadStackRequest().getMid();
                log.info("Receive thread stack request from remote, clientId={}", clientId);
                final String stackTrace = UtilAll.stackTrace();
                PrintThreadStackResponse printThreadStackResponse =
                        PrintThreadStackResponse.newBuilder().setStackTrace(stackTrace).setMid(mid).build();
                MultiplexingRequest multiplexingRequest = MultiplexingRequest
                        .newBuilder().setPrintThreadStackResponse(printThreadStackResponse).build();
                multiplexingCall(endpoints, multiplexingRequest);
                log.info("Send thread stack response to remote, clientId={}", clientId);
                break;
            case VERIFY_MESSAGE_CONSUMPTION_REQUEST:
                VerifyMessageConsumptionRequest verifyRequest = response.getVerifyMessageConsumptionRequest();
                final String messageId = verifyRequest.getMessage().getSystemAttribute().getMessageId();
                log.info("Receive verify message consumption request from remote, clientId={}, messageId={}",
                         clientId, messageId);
                ListenableFuture<VerifyMessageConsumptionResponse> future = verifyConsumption(verifyRequest);

                ScheduledExecutorService scheduler = clientManager.getScheduler();
                // in case of message consumption takes too long.
                Futures.withTimeout(future, VERIFY_CONSUMPTION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS, scheduler);
                Futures.addCallback(future, new FutureCallback<VerifyMessageConsumptionResponse>() {
                    @Override
                    public void onSuccess(VerifyMessageConsumptionResponse response) {
                        MultiplexingRequest multiplexingRequest = MultiplexingRequest
                                .newBuilder().setVerifyMessageConsumptionResponse(response).build();
                        log.info("Send verify message consumption response to remote, clientId={}, messageId={}",
                                 clientId, messageId);
                        multiplexingCall(endpoints, multiplexingRequest);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        Status status = Status.newBuilder().setCode(Code.ABORTED_VALUE).setMessage(t.getMessage())
                                              .build();
                        ResponseCommon common = ResponseCommon.newBuilder().setStatus(status).build();
                        final String mid = response.getVerifyMessageConsumptionRequest().getMid();
                        final VerifyMessageConsumptionResponse verifyResponse =
                                VerifyMessageConsumptionResponse.newBuilder().setCommon(common).setMid(mid).build();
                        MultiplexingRequest multiplexingRequest =
                                MultiplexingRequest.newBuilder()
                                                   .setVerifyMessageConsumptionResponse(verifyResponse).build();
                        log.info("Send verify message consumption response to remote, clientId={}, messageId={}",
                                 clientId, messageId);
                        multiplexingCall(endpoints, multiplexingRequest);
                    }
                });
                break;
            case RESOLVE_ORPHANED_TRANSACTION_REQUEST:
                ResolveOrphanedTransactionRequest orphanedRequest = response.getResolveOrphanedTransactionRequest();
                log.debug("Receive resolve orphaned transaction request from remote, clientId={}, messageId={}",
                          clientId, orphanedRequest.getOrphanedTransactionalMessage().getSystemAttribute()
                                                   .getMessageId());
                resolveOrphanedTransaction(endpoints, orphanedRequest);
                /* fall through on purpose. */
            case POLLING_RESPONSE:
                /* fall through on purpose. */
            default:
                dispatchGenericPollRequest(endpoints);
                break;
        }
    }

    private void dispatchGenericPollRequest(Endpoints endpoints) {
        final GenericPollingRequest genericPollingRequest = wrapGenericPollingRequest();
        MultiplexingRequest multiplexingRequest =
                MultiplexingRequest.newBuilder().setPollingRequest(genericPollingRequest).build();
        log.debug("Start to dispatch generic poll request, endpoints={}", endpoints);
        multiplexingCall(endpoints, multiplexingRequest);
    }

    private void multiplexingCall(final Endpoints endpoints, final MultiplexingRequest request) {
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
                        // should never reach here.
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

    private ListenableFuture<MultiplexingResponse> multiplexingCall0(Endpoints endpoints, MultiplexingRequest request) {
        final SettableFuture<MultiplexingResponse> future = SettableFuture.create();
        try {
            final Metadata metadata = sign();
            return clientManager.multiplexingCall(endpoints, metadata, request, MULTIPLEXING_CALL_TIMEOUT_MILLIS,
                                                  TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            // Failed to sign, set the future in advance.
            future.setException(t);
            return future;
        }
    }

    private void multiplexingCallLater(final Endpoints endpoints, final MultiplexingRequest request) {
        final ScheduledExecutorService scheduler = clientManager.getScheduler();
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
            if (scheduler.isShutdown()) {
                return;
            }
            log.error("[Bug] Failed to schedule multiplexing request", t);
            multiplexingCallLater(endpoints, request);
        }
    }

    private Set<Endpoints> getMessageTracingEndpointsSet() {
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

    private void updateMessageTracer() {
        synchronized (this) {
            try {
                log.debug("Start to update message tracer.");
                final Set<Endpoints> tracingEndpointsSet = getMessageTracingEndpointsSet();
                if (tracingEndpointsSet.isEmpty()) {
                    log.warn("No available message tracing endpoints.");
                    return;
                }
                if (null != messageTracingEndpoints && tracingEndpointsSet.contains(messageTracingEndpoints)) {
                    log.debug("message tracing target remains unchanged");
                    return;
                }
                List<Endpoints> tracingRpcTargetList = new ArrayList<Endpoints>(tracingEndpointsSet);
                Collections.shuffle(tracingRpcTargetList);
                // pick up tracing rpc target randomly.
                final Endpoints randomTracingEndpoints = tracingRpcTargetList.iterator().next();
                final SslContext sslContext =
                        GrpcSslContexts.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

                final NettyChannelBuilder channelBuilder =
                        NettyChannelBuilder.forTarget(randomTracingEndpoints.getFacade())
                                           .sslContext(sslContext)
                                           .intercept(new AuthInterceptor(this));

                final List<InetSocketAddress> socketAddresses = randomTracingEndpoints.toSocketAddresses();
                // if scheme is not domain.
                if (null != socketAddresses) {
                    IpNameResolverFactory tracingResolverFactory = new IpNameResolverFactory(socketAddresses);
                    channelBuilder.nameResolverFactory(tracingResolverFactory);
                }

                OtlpGrpcSpanExporter exporter =
                        OtlpGrpcSpanExporter.builder().setChannel(channelBuilder.build())
                                            .setTimeout(TRACE_EXPORTER_RPC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                                            .build();

                BatchSpanProcessor spanProcessor =
                        BatchSpanProcessor.builder(exporter)
                                          .setScheduleDelay(TRACE_EXPORTER_SCHEDULE_DELAY_MILLIS, TimeUnit.MILLISECONDS)
                                          .setMaxExportBatchSize(TRACE_EXPORTER_BATCH_SIZE)
                                          .setMaxQueueSize(TRACE_EXPORTER_MAX_QUEUE_SIZE)
                                          .build();
                if (null != messageTracerProvider) {
                    messageTracerProvider.shutdown();
                }
                messageTracerProvider = SdkTracerProvider.builder().addSpanProcessor(spanProcessor).build();
                OpenTelemetrySdk openTelemetry =
                        OpenTelemetrySdk.builder().setTracerProvider(messageTracerProvider).build();
                tracer = openTelemetry.getTracer(TRACER_INSTRUMENTATION_NAME);
                messageTracingEndpoints = randomTracingEndpoints;
            } catch (Throwable t) {
                log.error("Exception raised while updating tracer.", t);
            }
        }
    }

    public static MessageImpl wrapMessageImpl(Message message) {
        final org.apache.rocketmq.client.message.protocol.SystemAttribute mqSystemAttribute =
                new org.apache.rocketmq.client.message.protocol.SystemAttribute();
        final SystemAttribute systemAttribute = message.getSystemAttribute();
        // tag.
        mqSystemAttribute.setTag(systemAttribute.getTag());
        // keys.
        List<String> keys = new ArrayList<String>(systemAttribute.getKeysList());
        mqSystemAttribute.setKeys(keys);
        // message id.
        mqSystemAttribute.setMessageId(systemAttribute.getMessageId());
        // digest.
        final apache.rocketmq.v1.Digest bodyDigest = systemAttribute.getBodyDigest();
        byte[] body = message.getBody().toByteArray();
        boolean corrupted = false;
        String expectedCheckSum;
        DigestType digestType = DigestType.CRC32;
        final String checksum = bodyDigest.getChecksum();
        switch (bodyDigest.getType()) {
            case CRC32:
                expectedCheckSum = UtilAll.crc32CheckSum(body);
                if (!expectedCheckSum.equals(checksum)) {
                    corrupted = true;
                }
                break;
            case MD5:
                try {
                    expectedCheckSum = UtilAll.md5CheckSum(body);
                    if (!expectedCheckSum.equals(checksum)) {
                        corrupted = true;
                    }
                } catch (NoSuchAlgorithmException e) {
                    corrupted = true;
                    log.warn("MD5 is not supported unexpectedly, skip it.");
                }
                break;
            case SHA1:
                try {
                    expectedCheckSum = UtilAll.sha1CheckSum(body);
                    if (!expectedCheckSum.equals(checksum)) {
                        corrupted = true;
                    }
                } catch (NoSuchAlgorithmException e) {
                    corrupted = true;
                    log.warn("SHA-1 is not supported unexpectedly, skip it.");
                }
                break;
            default:
                log.warn("Unsupported message body digest algorithm.");
        }
        mqSystemAttribute.setDigest(new Digest(digestType, checksum));

        switch (systemAttribute.getBodyEncoding()) {
            case GZIP:
                try {
                    body = UtilAll.uncompressBytesGzip(body);
                    mqSystemAttribute.setBodyEncoding(Encoding.GZIP);
                } catch (IOException e) {
                    log.error("Failed to uncompress message body, messageId={}", systemAttribute.getMessageId());
                    corrupted = true;
                }
                break;
            case SNAPPY:
                // TODO
                mqSystemAttribute.setBodyEncoding(Encoding.SNAPPY);
                log.warn("SNAPPY encoding algorithm is not supported.");
                break;
            case IDENTITY:
                mqSystemAttribute.setBodyEncoding(Encoding.IDENTITY);
                break;
            default:
                log.warn("Unsupported message encoding algorithm.");
        }

        // message type.
        MessageType messageType;
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
        mqSystemAttribute.setMessageType(messageType);
        // born time millis.
        mqSystemAttribute.setBornTimeMillis(Timestamps.toMillis(systemAttribute.getBornTimestamp()));
        // born host.
        mqSystemAttribute.setBornHost(systemAttribute.getBornHost());
        // store time millis
        mqSystemAttribute.setStoreTimeMillis(Timestamps.toMillis(systemAttribute.getStoreTimestamp()));

        switch (systemAttribute.getTimedDeliveryCase()) {
            case DELAY_LEVEL:
                // delay level
                mqSystemAttribute.setDelayLevel(systemAttribute.getDelayLevel());
                break;
            case DELIVERY_TIMESTAMP:
                // delay timestamp
                mqSystemAttribute.setDeliveryTimeMillis(Timestamps.toMillis(systemAttribute.getDeliveryTimestamp()));
                break;
            case TIMEDDELIVERY_NOT_SET:
            default:
                break;
        }
        // receipt handle.
        mqSystemAttribute.setReceiptHandle(systemAttribute.getReceiptHandle());
        // partition id.
        mqSystemAttribute.setPartitionId(systemAttribute.getPartitionId());
        // partition offset.
        mqSystemAttribute.setPartitionOffset(systemAttribute.getPartitionOffset());
        // invisible period.
        mqSystemAttribute.setInvisiblePeriod(Durations.toMillis(systemAttribute.getInvisiblePeriod()));
        // delivery attempt
        mqSystemAttribute.setDeliveryAttempt(systemAttribute.getDeliveryAttempt());
        // producer group.
        mqSystemAttribute.setProducerGroup(systemAttribute.getProducerGroup().getName());
        // message group.
        mqSystemAttribute.setMessageGroup(systemAttribute.getMessageGroup());
        // trace context.
        mqSystemAttribute.setTraceContext(systemAttribute.getTraceContext());
        // transaction resolve delay millis.
        mqSystemAttribute.setOrphanedTransactionRecoveryPeriodMillis(
                Durations.toMillis(systemAttribute.getOrphanedTransactionRecoveryPeriod()));
        // decoded timestamp.
        mqSystemAttribute.setDecodedTimestamp(System.currentTimeMillis());
        // user properties.
        final ConcurrentHashMap<String, String> mqUserAttribute =
                new ConcurrentHashMap<String, String>(message.getUserAttributeMap());

        final String topic = message.getTopic().getName();
        return new MessageImpl(topic, mqSystemAttribute, mqUserAttribute, body, corrupted);
    }

    public Tracer getTracer() {
        return this.tracer;
    }
}
