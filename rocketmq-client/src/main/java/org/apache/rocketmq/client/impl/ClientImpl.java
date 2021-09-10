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

import apache.rocketmq.v1.GenericPollingRequest;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.MultiplexingRequest;
import apache.rocketmq.v1.MultiplexingResponse;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.PrintThreadStackResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.RecoverOrphanedTransactionRequest;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.ResponseCommon;
import apache.rocketmq.v1.VerifyMessageConsumptionRequest;
import apache.rocketmq.v1.VerifyMessageConsumptionResponse;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Metadata;
import java.util.ArrayList;
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
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.misc.TopAddressing;
import org.apache.rocketmq.client.misc.Validators;
import org.apache.rocketmq.client.route.Address;
import org.apache.rocketmq.client.route.AddressScheme;
import org.apache.rocketmq.client.route.Broker;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.Permission;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.client.trace.MessageTracer;
import org.apache.rocketmq.client.trace.TraceEndpointsProvider;
import org.apache.rocketmq.utility.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings(value = {"UnstableApiUsage", "NullableProblems"})
public abstract class ClientImpl extends Client implements MessageInterceptor, TraceEndpointsProvider {
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

    protected volatile ClientManager clientManager;

    protected final ClientService clientService;

    private final MessageTracer messageTracer;

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

        this.clientService = new ClientService();

        this.messageTracer = new MessageTracer(this);

        this.topAddressing = new TopAddressing();
        this.nameServerIndex = new AtomicInteger(RandomUtils.nextInt());

        this.messageInterceptors = new ArrayList<MessageInterceptor>();
        this.messageInterceptorsLock = new ReentrantReadWriteLock();

        this.nameServerEndpointsList = new ArrayList<Endpoints>();
        this.nameServerEndpointsListLock = new ReentrantReadWriteLock();

        this.inflightRouteFutureTable = new HashMap<String, Set<SettableFuture<TopicRouteData>>>();
        this.inflightRouteFutureLock = new ReentrantLock();

        this.topicRouteCache = new ConcurrentHashMap<String, TopicRouteData>();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                clientService.stopAsync().awaitTerminated();
            }
        });
    }

    /**
     * Underlying implement could intercept the {@link TopicRouteData}.
     *
     * @param topic          topic's name
     * @param topicRouteData route data of specified topic.
     */
    public abstract void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData);

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isRunning() {
        return clientService.isRunning();
    }

    public class ClientService extends AbstractIdleService {
        @Override
        protected void startUp() throws Exception {
            ClientImpl.this.setUp();
        }

        @Override
        protected void shutDown() throws Exception {
            ClientImpl.this.tearDown();
        }
    }

    public void registerMessageInterceptor(MessageInterceptor messageInterceptor) {
        messageInterceptorsLock.writeLock().lock();
        try {
            messageInterceptors.add(messageInterceptor);
        } finally {
            messageInterceptorsLock.writeLock().unlock();
        }
    }

    protected void setUp() throws ClientException {
        log.info("Begin to start the rocketmq client, clientId={}", id);
        if (null == clientManager) {
            clientManager = ClientManagerFactory.getInstance().registerClient(namespace, this);
        }

        messageTracer.init();

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
        log.info("The rocketmq client starts successfully, clientId={}", id);
    }

    private void notifyClientTermination() {
        log.info("Notify that client is terminated, clientId={}", id);
        final Set<Endpoints> routeEndpointsSet = getRouteEndpointsSet();
        final NotifyClientTerminationRequest notifyClientTerminationRequest =
                NotifyClientTerminationRequest.newBuilder().setClientId(id).setGroup(getPbGroup()).build();
        try {
            final Metadata metadata = sign();
            for (Endpoints endpoints : routeEndpointsSet) {
                clientManager.notifyClientTermination(endpoints, metadata, notifyClientTerminationRequest,
                                                      ioTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        } catch (Throwable t) {
            log.error("Exception raised while notifying client's termination, clientId={}", id, t);
        }
    }

    protected void tearDown() throws InterruptedException {
        log.info("Begin to shutdown the rocketmq client, clientId={}", id);
        notifyClientTermination();
        if (null != renewNameServerListFuture) {
            renewNameServerListFuture.cancel(false);
        }
        if (null != updateRouteCacheFuture) {
            updateRouteCacheFuture.cancel(false);
        }
        ClientManagerFactory.getInstance().unregisterClient(namespace, this);
        log.info("Shutdown the rocketmq client successfully, clientId={}", id);
    }

    public void intercept(MessageHookPoint hookPoint, MessageInterceptorContext context) {
        intercept(hookPoint, null, context);
    }

    @Override
    public void intercept(MessageHookPoint hookPoint, MessageExt messageExt, MessageInterceptorContext context) {
        messageInterceptorsLock.readLock().lock();
        try {
            for (MessageInterceptor interceptor : messageInterceptors) {
                try {
                    interceptor.intercept(hookPoint, messageExt, context);
                } catch (Throwable t) {
                    log.warn("Exception raised while intercepting message, hookPoint={}, messageId={}", hookPoint,
                             messageExt.getMsgId());
                }
            }
        } finally {
            messageInterceptorsLock.readLock().unlock();
        }
    }

    public ScheduledExecutorService getScheduler() {
        return clientManager.getScheduler();
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

    private synchronized Set<Endpoints> updateTopicRouteCache(String topic, TopicRouteData topicRouteData) {
        final Set<Endpoints> before = getRouteEndpointsSet();
        topicRouteCache.put(topic, topicRouteData);
        final Set<Endpoints> after = getRouteEndpointsSet();
        return new HashSet<Endpoints>(Sets.difference(after, before));
    }

    private void onTopicRouteDataUpdate(String topic, TopicRouteData topicRouteData) {
        onTopicRouteDataUpdate0(topic, topicRouteData);
        final Set<Endpoints> diff = updateTopicRouteCache(topic, topicRouteData);
        for (Endpoints endpoints : diff) {
            log.info("Start multiplexing call for new endpoints={}", endpoints);
            dispatchGenericPollRequest(endpoints);
        }
        messageTracer.refresh();
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
                    log.error("Failed to fetch topic route, namespace={}, topic={}", namespace, topic, t);
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
                        log.error("[Bug] in-flight route futures was empty, namespace={}, topic={}", namespace, topic);
                        return;
                    }
                    log.debug("Fetch topic route successfully, namespace={}, topic={}, in-flight route future size={}",
                              namespace, topic, newFutureSet.size());
                    for (SettableFuture<TopicRouteData> newFuture : newFutureSet) {
                        newFuture.set(newTopicRouteData);
                    }
                } catch (Throwable t) {
                    // should never reach here.
                    log.error("[Bug] Exception raises while update route data, clientId={}, namespace={}, topic={}",
                              id, namespace, topic, t);
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
                        log.error("[Bug] in-flight route futures was empty, namespace={}, topic={}", namespace, topic);
                        return;
                    }
                    log.error("Failed to fetch topic route, namespace={}, topic={}, in-flight route future size={}",
                              namespace, topic, newFutureSet.size(), t);
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

    public void setNamesrvAddr(String nameServerStr) throws ClientException {
        Validators.checkNamesrvAddr(nameServerStr);
        nameServerEndpointsListLock.writeLock().lock();
        try {
            this.nameServerEndpointsList.clear();

            boolean httpPatternMatched = false;
            String httpPrefixMatched = "";
            if (nameServerStr.startsWith(MixAll.HTTP_PREFIX)) {
                httpPatternMatched = true;
                httpPrefixMatched = MixAll.HTTP_PREFIX;
            } else if (nameServerStr.startsWith(MixAll.HTTPS_PREFIX)) {
                httpPatternMatched = true;
                httpPrefixMatched = MixAll.HTTPS_PREFIX;
            }
            // for http pattern.
            if (httpPatternMatched) {
                final String domainName = nameServerStr.substring(httpPrefixMatched.length());
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

                // namespace is set before.
                if (StringUtils.isNotBlank(namespace)) {
                    return;
                }
                if (Validators.NAME_SERVER_ENDPOINT_WITH_NAMESPACE_PATTERN.matcher(nameServerStr).matches()) {
                    this.namespace = nameServerStr.substring(nameServerStr.lastIndexOf('/') + 1,
                                                             nameServerStr.indexOf('.'));
                }
                return;
            }
            // for ip pattern.
            try {
                final String[] addressArray = nameServerStr.split(";");
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
            this.nameServerStr = nameServerStr;
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
            return nameServerEndpointsList.get(IntMath.mod(nameServerIndex.get(), nameServerEndpointsList.size()));
        } finally {
            nameServerEndpointsListLock.readLock().unlock();
        }
    }

    public Resource getPbGroup() {
        return Resource.newBuilder().setResourceNamespace(namespace).setName(group).build();
    }

    private ListenableFuture<TopicRouteData> fetchTopicRoute(final String topic) {
        final SettableFuture<TopicRouteData> future = SettableFuture.create();
        try {
            final Endpoints endpoints = selectNameServerEndpoints();
            Resource topicResource = Resource.newBuilder().setResourceNamespace(namespace).setName(topic).build();
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
                    // TODO: consider to remove defensive programming here.
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

    public abstract HeartbeatRequest wrapHeartbeatRequest();

    protected ListenableFuture<HeartbeatResponse> doHeartbeat(HeartbeatRequest request, final Endpoints endpoints) {
        try {
            Metadata metadata = sign();
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
            return future;
        } catch (Throwable e) {
            log.error("Exception raised while heartbeat, endpoints={}", endpoints, e);
            SettableFuture<HeartbeatResponse> future0 = SettableFuture.create();
            future0.setException(e);
            return future0;
        }
    }

    @Override
    public void doHeartbeat() {
        final Set<Endpoints> routeEndpointsSet = getRouteEndpointsSet();
        final HeartbeatRequest request = wrapHeartbeatRequest();
        for (Endpoints endpoints : routeEndpointsSet) {
            doHeartbeat(request, endpoints);
        }
    }

    public abstract GenericPollingRequest wrapGenericPollingRequest();


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
    public void recoverOrphanedTransaction(Endpoints endpoints, RecoverOrphanedTransactionRequest request) {
    }

    private void onMultiplexingResponse(final Endpoints endpoints, final MultiplexingResponse response) {
        switch (response.getTypeCase()) {
            case PRINT_THREAD_STACK_REQUEST:
                String mid = response.getPrintThreadStackRequest().getMid();
                log.info("Receive thread stack request from remote, clientId={}", id);
                final String stackTrace = UtilAll.stackTrace();
                PrintThreadStackResponse printThreadStackResponse =
                        PrintThreadStackResponse.newBuilder().setStackTrace(stackTrace).setMid(mid).build();
                MultiplexingRequest multiplexingRequest = MultiplexingRequest
                        .newBuilder().setPrintThreadStackResponse(printThreadStackResponse).build();
                multiplexingCall(endpoints, multiplexingRequest);
                log.info("Send thread stack response to remote, clientId={}", id);
                break;
            case VERIFY_MESSAGE_CONSUMPTION_REQUEST:
                VerifyMessageConsumptionRequest verifyRequest = response.getVerifyMessageConsumptionRequest();
                final String messageId = verifyRequest.getMessage().getSystemAttribute().getMessageId();
                log.info("Receive verify message consumption request from remote, clientId={}, messageId={}",
                         id, messageId);
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
                                 id, messageId);
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
                                 id, messageId);
                        multiplexingCall(endpoints, multiplexingRequest);
                    }
                });
                break;
            case RECOVER_ORPHANED_TRANSACTION_REQUEST:
                RecoverOrphanedTransactionRequest orphanedRequest = response.getRecoverOrphanedTransactionRequest();
                log.debug("Receive resolve orphaned transaction request from remote, clientId={}, messageId={}",
                          id, orphanedRequest.getOrphanedTransactionalMessage().getSystemAttribute()
                                             .getMessageId());
                recoverOrphanedTransaction(endpoints, orphanedRequest);
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

    @Override
    public List<Endpoints> getTraceCandidates() {
        Set<Endpoints> set = new HashSet<Endpoints>();
        for (TopicRouteData topicRouteData : topicRouteCache.values()) {
            final List<Partition> partitions = topicRouteData.getPartitions();
            for (Partition partition : partitions) {
                final Broker broker = partition.getBroker();
                if (MixAll.MASTER_BROKER_ID != broker.getId()) {
                    continue;
                }
                if (Permission.NONE.equals(partition.getPermission())) {
                    continue;
                }
                set.add(broker.getEndpoints());
            }
        }
        return new ArrayList<Endpoints>(set);
    }
}
