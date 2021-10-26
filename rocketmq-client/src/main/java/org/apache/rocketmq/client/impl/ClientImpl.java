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

import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.PollCommandRequest;
import apache.rocketmq.v1.PollCommandResponse;
import apache.rocketmq.v1.PrintThreadStackTraceCommand;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v1.ReportThreadStackTraceRequest;
import apache.rocketmq.v1.ReportThreadStackTraceResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.VerifyMessageConsumptionCommand;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.apache.rocketmq.utility.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings(value = {"UnstableApiUsage", "NullableProblems"})
public abstract class ClientImpl extends Client implements MessageInterceptor, TraceEndpointsProvider {
    private static final Logger log = LoggerFactory.getLogger(ClientImpl.class);

    /**
     * Delay interval while polling command encounters failure.
     */
    private static final long POLL_COMMAND_LATER_DELAY_MILLIS = 1000L;

    /**
     * Maximum time allowed client to execute polling call before being cancelled.
     */
    private static final long POLL_COMMAND_TIMEOUT_MILLIS = 60 * 1000L;

    protected volatile ClientManager clientManager;

    protected final ClientService clientService;

    protected final ThreadPoolExecutor commandExecutor;

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

        this.commandExecutor = new ThreadPoolExecutor(
                1,
                1,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("CommandExecutor"));

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Shutdown hook is invoked, clientId={}, status={}", id, clientService.state());
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

    public abstract NotifyClientTerminationRequest wrapNotifyClientTerminationRequest();

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

            log.info("Name server list was not set, schedule a task to fetch and renew periodically, clientId={}", id);
            renewNameServerListFuture = scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                renewNameServerList();
                            } catch (Throwable t) {
                                log.error("Exception raised while updating nameserver from top addressing, clientId={}",
                                          id, t);
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
                            log.error("Exception raised while updating topic route cache, clientId={}", id, t);
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
        final NotifyClientTerminationRequest notifyClientTerminationRequest = wrapNotifyClientTerminationRequest();
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
        messageTracer.shutdown();
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
                    log.warn("Exception raised while intercepting message, hookPoint={}, messageId={}, clientId={}",
                             hookPoint, messageExt.getMsgId(), id);
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
            log.error("Failed to calculate signature, clientId={}", id, t);
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
        log.info("Start to renew name server list for a new round, clientId={}", id);
        List<Endpoints> newNameServerEndpointsList;
        try {
            newNameServerEndpointsList = topAddressing.fetchNameServerAddresses();
        } catch (Throwable t) {
            log.error("Failed to fetch name server list from top addressing", t);
            return;
        }
        if (newNameServerEndpointsList.isEmpty()) {
            log.warn("Got an empty name server list, clientId={}", id);
            return;
        }
        nameServerEndpointsListLock.writeLock().lock();
        try {
            if (nameServerEndpointsList.equals(newNameServerEndpointsList)) {
                log.debug("Name server list remains the same, name server list={}, clientId={}",
                          nameServerEndpointsList, id);
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
        final TopicRouteData oldTopicRouteData = topicRouteCache.put(topic, topicRouteData);
        // topicRouteData here is never be null.
        if (topicRouteData.equals(oldTopicRouteData)) {
            log.info("Topic route remains the same, namespace={}, topic={}, clientId={}", namespace, topic, id);
        } else {
            log.info("Topic route is updated, namespace={}, topic={}, clientId={}, {} => {}", namespace, topic,
                     id, oldTopicRouteData, topicRouteData);
        }
        final Set<Endpoints> after = getRouteEndpointsSet();
        return new HashSet<Endpoints>(Sets.difference(after, before));
    }

    private void onTopicRouteDataUpdate(String topic, TopicRouteData topicRouteData) {
        onTopicRouteDataUpdate0(topic, topicRouteData);
        final Set<Endpoints> diff = updateTopicRouteCache(topic, topicRouteData);
        for (Endpoints endpoints : diff) {
            log.info("Start polling command for new endpoints={}, clientId={}", endpoints, id);
            pollCommand(endpoints);
        }
        messageTracer.refresh();
    }

    private void updateRouteCache() {
        log.info("Start to update route cache for a new round, clientId={}", id);
        for (final String topic : topicRouteCache.keySet()) {
            final ListenableFuture<TopicRouteData> future = fetchTopicRoute(topic);
            Futures.addCallback(future, new FutureCallback<TopicRouteData>() {
                @Override
                public void onSuccess(TopicRouteData topicRouteData) {
                    onTopicRouteDataUpdate(topic, topicRouteData);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Failed to fetch topic route for update cache, namespace={}, topic={}, clientId={}",
                              namespace, topic, id, t);
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
                        log.error("[Bug] in-flight route futures was empty, namespace={}, topic={}, clientId={}",
                                  namespace, topic, id);
                        return;
                    }
                    log.debug("Fetch topic route successfully, namespace={}, topic={}, in-flight route future "
                              + "size={}, clientId={}", namespace, topic, newFutureSet.size(), id);
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
                        log.error("[Bug] in-flight route futures was empty, namespace={}, topic={}, clientId={}",
                                  namespace, topic, id);
                        return;
                    }
                    log.error("Failed to fetch topic route, namespace={}, topic={}, in-flight route future size={}, "
                              + "clientId={}", namespace, topic, newFutureSet.size(), id, t);
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
        this.nameServerStr = nameServerStr;
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
                log.error("Exception raises while parse name server address, clientId={}", id, t);
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
                    // Response is processed by caller.
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Exception raised while fetch topic route from name server endpoints={}, namespace={}, "
                              + "topic={}, clientId={}, choose the another one for the next round.", endpoints,
                              namespace, topic, id, t);
                    nameServerIndex.getAndIncrement();
                }
            });
            return Futures.transformAsync(responseFuture, new AsyncFunction<QueryRouteResponse, TopicRouteData>() {
                @Override
                public ListenableFuture<TopicRouteData> apply(QueryRouteResponse response) throws Exception {
                    final Status status = response.getCommon().getStatus();
                    final Code code = Code.forNumber(status.getCode());
                    if (Code.NOT_FOUND.equals(code)) {
                        log.error("Topic not found, namespace={}, topic={}, clientId={}, endpoints={}, status "
                                  + "message=[{}]", namespace, topic, id, endpoints, status.getMessage());
                        future.set(TopicRouteData.EMPTY);
                        return future;
                    }
                    if (!Code.OK.equals(code)) {
                        throw new ClientException(ErrorCode.FETCH_TOPIC_ROUTE_FAILURE, status.toString());
                    }
                    final TopicRouteData topicRouteData = new TopicRouteData(response.getPartitionsList());
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
                    if (!Code.OK.equals(code)) {
                        log.warn("Failed to send heartbeat, code={}, status message=[{}], endpoints={}, clientId={}",
                                 code, status.getMessage(), endpoints, id);
                        return;
                    }
                    log.info("Send heartbeat successfully, endpoints={}, clientId={}", endpoints, id);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.warn("Failed to send heartbeat, endpoints={}, clientId={}", endpoints, id, t);
                }
            });
            return future;
        } catch (Throwable e) {
            log.error("Exception raised while heartbeat, endpoints={}, clientId={}", endpoints, id, e);
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

    public abstract PollCommandRequest wrapPollCommandRequest();

    /**
     * Verify message consumption , only be implemented by push consumer.
     *
     * <p>SHOULD NEVER THROW ANY EXCEPTION.</p>
     *
     * @param endpoints remote endpoints.
     * @param command   verify message consumption command.
     */
    public void verifyMessageConsumption(final Endpoints endpoints, final VerifyMessageConsumptionCommand command) {
    }

    /**
     * Recover orphaned transaction message, only be implemented by producer.
     *
     * <p>SHOULD NEVER THROW ANY EXCEPTION.</p>
     *
     * @param endpoints remote endpoints.
     * @param command   orphaned transaction command.
     */
    public void recoverOrphanedTransaction(final Endpoints endpoints, final RecoverOrphanedTransactionCommand command) {
    }

    public void printThreadStackTrace(final Endpoints endpoints, final PrintThreadStackTraceCommand command) {
        final String commandId = command.getCommandId();
        final Runnable task = new Runnable() {
            @Override
            public void run() {
                ListenableFuture<ReportThreadStackTraceResponse> future;
                try {
                    final String threadStackTrace = UtilAll.stackTrace();
                    ReportThreadStackTraceRequest request =
                            ReportThreadStackTraceRequest.newBuilder().setThreadStackTrace(threadStackTrace)
                                                         .setCommandId(commandId).build();
                    final Metadata metadata = sign();
                    future = clientManager.reportThreadStackTrace(endpoints, metadata, request, ioTimeoutMillis,
                                                                  TimeUnit.MILLISECONDS);
                } catch (Throwable t) {
                    SettableFuture<ReportThreadStackTraceResponse> future0 = SettableFuture.create();
                    future0.setException(t);
                    future = future0;
                }
                Futures.addCallback(future, new FutureCallback<ReportThreadStackTraceResponse>() {
                    @Override
                    public void onSuccess(ReportThreadStackTraceResponse response) {
                        final Status status = response.getCommon().getStatus();
                        final Code code = Code.forNumber(status.getCode());
                        if (!Code.OK.equals(code)) {
                            log.error("Failed to report thread stack trace, clientId={}, commandId={}, code={}, status "
                                      + "message=[{}]", id, commandId, code, status.getMessage());
                            return;
                        }
                        log.info("Report thread stack trace response, clientId={}, commandId={}", id, commandId);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.error("Exception raised while reporting thread stack trace, clientId={}, commandId={}",
                                  id, commandId, t);
                    }
                });
            }
        };
        try {
            commandExecutor.submit(task);
        } catch (Throwable t) {
            // should never reach here.
            log.error("[Bug] Exception raised while submitting task to print thread stack trace, clientId={}, "
                      + "commandId={}", id, commandId, t);
        }
    }

    private void onPollCommandResponse(final Endpoints endpoints, final PollCommandResponse response) {
        switch (response.getTypeCase()) {
            case PRINT_THREAD_STACK_TRACE_COMMAND:
                log.info("Receive command to print thread stack trace, clientId={}, commandId={}", id,
                         response.getPrintThreadStackTraceCommand().getCommandId());
                printThreadStackTrace(endpoints, response.getPrintThreadStackTraceCommand());
                break;
            case VERIFY_MESSAGE_CONSUMPTION_COMMAND:
                log.info("Receive command to verify message consumption, clientId={}, commandId={}", id,
                         response.getVerifyMessageConsumptionCommand().getCommandId());
                verifyMessageConsumption(endpoints, response.getVerifyMessageConsumptionCommand());
                break;
            case RECOVER_ORPHANED_TRANSACTION_COMMAND:
                log.info("Receive command to recover orphaned transaction, clientId={}", id);
                recoverOrphanedTransaction(endpoints, response.getRecoverOrphanedTransactionCommand());
                break;
            case NOOP_COMMAND:
                log.debug("Receive noop command, clientId={}", id);
                break;
            default:
                break;
        }
        pollCommand(endpoints);
    }

    private void pollCommand(final Endpoints endpoints) {
        try {
            final PollCommandRequest request = wrapPollCommandRequest();
            final Set<Endpoints> routeEndpointsSet = getRouteEndpointsSet();
            if (!routeEndpointsSet.contains(endpoints)) {
                log.info("Endpoints was removed, no need to poll command, endpoints={}, clientId={}", endpoints, id);
                return;
            }
            final ListenableFuture<PollCommandResponse> future = pollCommand0(endpoints, request);
            Futures.addCallback(future, new FutureCallback<PollCommandResponse>() {
                @Override
                public void onSuccess(PollCommandResponse response) {
                    try {
                        onPollCommandResponse(endpoints, response);
                    } catch (Throwable t) {
                        // should never reach here.
                        log.error("[Bug] Exception raised while handling polling response, would call later, "
                                  + "endpoints={}, clientId={}", endpoints, id, t);
                        pollCommandLater(endpoints);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Exception raised while polling command, would call later, endpoints={}, clientId={}",
                              endpoints, id, t);
                    pollCommandLater(endpoints);
                }
            });
        } catch (Throwable t) {
            log.error("Exception raised while polling command, would call later, endpoints={}, clientId={}",
                      endpoints, id, t);
            pollCommandLater(endpoints);
        }
    }

    private ListenableFuture<PollCommandResponse> pollCommand0(Endpoints endpoints, PollCommandRequest request) {
        final SettableFuture<PollCommandResponse> future = SettableFuture.create();
        try {
            final Metadata metadata = sign();
            return clientManager.pollCommand(endpoints, metadata, request, POLL_COMMAND_TIMEOUT_MILLIS,
                                             TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            // Failed to sign, set the future in advance.
            future.setException(t);
            return future;
        }
    }

    private void pollCommandLater(final Endpoints endpoints) {
        final ScheduledExecutorService scheduler = clientManager.getScheduler();
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        pollCommand(endpoints);
                    } catch (Throwable t) {
                        pollCommandLater(endpoints);
                    }
                }
            }, POLL_COMMAND_LATER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            log.error("[Bug] Failed to schedule polling command, clientId={}", id, t);
            pollCommandLater(endpoints);
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
