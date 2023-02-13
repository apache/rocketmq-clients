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

package org.apache.rocketmq.client.java.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.ThreadStackTrace;
import apache.rocketmq.v2.VerifyMessageCommand;
import apache.rocketmq.v2.VerifyMessageResult;
import com.google.common.base.Function;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.exception.InternalErrorException;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.hook.CompositedMessageInterceptor;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContext;
import org.apache.rocketmq.client.java.impl.producer.ClientSessionHandler;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.metrics.ClientMeterManager;
import org.apache.rocketmq.client.java.metrics.MessageMeterInterceptor;
import org.apache.rocketmq.client.java.metrics.Metric;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ExecutorServices;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.apache.rocketmq.client.java.rpc.Signature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
public abstract class ClientImpl extends AbstractIdleService implements Client, ClientSessionHandler,
    MessageInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ClientImpl.class);
    /**
     * The telemetry timeout should not be too long, otherwise
     * <a href="https://github.com/grpc/grpc-java/issues/7351">this issue</a> may be triggered in JDK8 + macOS.
     */
    private static final Duration TELEMETRY_TIMEOUT = Duration.ofDays(60 * 365);

    protected final ClientConfiguration clientConfiguration;
    protected final Endpoints endpoints;
    protected final Set<String> topics;
    // Thread-safe set.
    protected final Set<Endpoints> isolated;
    protected final ExecutorService clientCallbackExecutor;
    protected final ClientMeterManager clientMeterManager;
    /**
     * Telemetry command executor, which aims to execute commands from the remote.
     */
    protected final ThreadPoolExecutor telemetryCommandExecutor;
    protected final ClientId clientId;

    private final ClientManager clientManager;
    private volatile ScheduledFuture<?> updateRouteCacheFuture;
    private final ConcurrentMap<String, TopicRouteData> topicRouteCache;

    @GuardedBy("inflightRouteFutureLock")
    private final Map<String /* topic */, Set<SettableFuture<TopicRouteData>>> inflightRouteFutureTable;
    private final Lock inflightRouteFutureLock;

    @GuardedBy("sessionsLock")
    private final Map<Endpoints, ClientSessionImpl> sessionsTable;
    private final ReadWriteLock sessionsLock;

    private final CompositedMessageInterceptor compositedMessageInterceptor;

    public ClientImpl(ClientConfiguration clientConfiguration, Set<String> topics) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        this.endpoints = new Endpoints(clientConfiguration.getEndpoints());
        this.topics = topics;
        // Generate client id firstly.
        this.clientId = new ClientId();

        this.topicRouteCache = new ConcurrentHashMap<>();

        this.inflightRouteFutureTable = new ConcurrentHashMap<>();
        this.inflightRouteFutureLock = new ReentrantLock();

        this.sessionsTable = new HashMap<>();
        this.sessionsLock = new ReentrantReadWriteLock();

        this.isolated = Collections.newSetFromMap(new ConcurrentHashMap<>());

        this.clientManager = new ClientManagerImpl(this);

        final long clientIdIndex = clientId.getIndex();
        this.clientCallbackExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("ClientCallbackWorker", clientIdIndex));

        this.clientMeterManager = new ClientMeterManager(clientId, clientConfiguration);

        this.compositedMessageInterceptor =
            new CompositedMessageInterceptor(Collections.singletonList(new MessageMeterInterceptor(this,
                clientMeterManager)));

        this.telemetryCommandExecutor = new ThreadPoolExecutor(
            1,
            1,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("CommandExecutor", clientIdIndex));
    }


    /**
     * Start the rocketmq client and do some preparatory work.
     */
    @Override
    protected void startUp() throws Exception {
        log.info("Begin to start the rocketmq client, clientId={}", clientId);
        this.clientManager.startAsync().awaitRunning();
        // Fetch topic route from remote.
        log.info("Begin to fetch topic(s) route data from remote during client startup, clientId={}, topics={}",
            clientId, topics);
        for (String topic : topics) {
            final ListenableFuture<TopicRouteData> future = fetchTopicRoute(topic);
            future.get();
        }
        log.info("Fetch topic route data from remote successfully during startup, clientId={}, topics={}",
            clientId, topics);
        // Update route cache periodically.
        final ScheduledExecutorService scheduler = clientManager.getScheduler();
        this.updateRouteCacheFuture = scheduler.scheduleWithFixedDelay(() -> {
            try {
                updateRouteCache();
            } catch (Throwable t) {
                log.error("Exception raised while updating topic route cache, clientId={}", clientId, t);
            }
        }, 10, 30, TimeUnit.SECONDS);
        log.info("The rocketmq client starts successfully, clientId={}", clientId);
    }

    /**
     * Shutdown the rocketmq client and release related resources.
     */
    @Override
    protected void shutDown() throws InterruptedException {
        log.info("Begin to shutdown the rocketmq client, clientId={}", clientId);
        notifyClientTermination();
        if (null != this.updateRouteCacheFuture) {
            updateRouteCacheFuture.cancel(false);
        }
        telemetryCommandExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(telemetryCommandExecutor)) {
            log.error("[Bug] Timeout to shutdown the telemetry command executor, clientId={}", clientId);
        } else {
            log.info("Shutdown the telemetry command executor successfully, clientId={}", clientId);
        }
        log.info("Begin to release all telemetry sessions, clientId={}", clientId);
        releaseClientSessions();
        log.info("Release all telemetry sessions successfully, clientId={}", clientId);
        clientManager.stopAsync().awaitTerminated();
        clientCallbackExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(clientCallbackExecutor)) {
            log.error("[Bug] Timeout to shutdown the client callback executor, clientId={}", clientId);
        }
        clientMeterManager.shutdown();
        log.info("Shutdown the rocketmq client successfully, clientId={}", clientId);
    }

    @Override
    public void doBefore(MessageInterceptorContext context, List<GeneralMessage> generalMessages) {
        try {
            compositedMessageInterceptor.doBefore(context, generalMessages);
        } catch (Throwable t) {
            // Should never reach here.
            log.error("[Bug] Exception raised while handling messages, clientId={}", clientId, t);
        }
    }

    @Override
    public void doAfter(MessageInterceptorContext context, List<GeneralMessage> generalMessages) {
        try {
            compositedMessageInterceptor.doAfter(context, generalMessages);
        } catch (Throwable t) {
            // Should never reach here.
            log.error("[Bug] Exception raised while handling messages, clientId={}", clientId, t);
        }
    }

    @Override
    public TelemetryCommand settingsCommand() {
        final apache.rocketmq.v2.Settings settings = this.getSettings().toProtobuf();
        return TelemetryCommand.newBuilder().setSettings(settings).build();
    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(Endpoints endpoints,
        StreamObserver<TelemetryCommand> observer) throws ClientException {
        try {
            return clientManager.telemetry(endpoints, TELEMETRY_TIMEOUT, observer);
        } catch (ClientException e) {
            throw e;
        } catch (Throwable t) {
            throw new InternalErrorException(t);
        }
    }

    @Override
    public boolean isEndpointsDeprecated(Endpoints endpoints) {
        final Set<Endpoints> totalRouteEndpoints = getTotalRouteEndpoints();
        return !totalRouteEndpoints.contains(endpoints);
    }

    /**
     * This method is invoked while request of printing thread stack trace is received from remote.
     *
     * @param endpoints remote endpoints.
     * @param command   request of printing thread stack trace from remote.
     */
    @Override
    public void onPrintThreadStackTraceCommand(Endpoints endpoints, PrintThreadStackTraceCommand command) {
        final String nonce = command.getNonce();
        Runnable task = () -> {
            try {
                final String stackTrace = Utilities.stackTrace();
                Status status = Status.newBuilder().setCode(Code.OK).build();
                ThreadStackTrace threadStackTrace = ThreadStackTrace.newBuilder().setThreadStackTrace(stackTrace)
                    .setNonce(command.getNonce()).build();
                TelemetryCommand telemetryCommand = TelemetryCommand.newBuilder()
                    .setThreadStackTrace(threadStackTrace)
                    .setStatus(status)
                    .build();
                telemetry(endpoints, telemetryCommand);
            } catch (Throwable t) {
                log.error("Failed to send thread stack trace to remote, endpoints={}, nonce={}, clientId={}",
                    endpoints, nonce, clientId, t);
            }
        };
        try {
            telemetryCommandExecutor.submit(task);
        } catch (Throwable t) {
            log.error("[Bug] Exception raised while submitting task to print thread stack trace, endpoints={}, "
                + "nonce={}, clientId={}", endpoints, nonce, clientId, t);
        }
    }

    public abstract Settings getSettings();

    /**
     * Apply setting from remote.
     *
     * @param endpoints remote endpoints.
     * @param settings  settings received from remote.
     */
    @Override
    public final void onSettingsCommand(Endpoints endpoints, apache.rocketmq.v2.Settings settings) {
        final Metric metric = new Metric(settings.getMetric());
        clientMeterManager.reset(metric);
        this.getSettings().sync(settings);
    }

    /**
     * @see Client#syncSettings()
     */
    @Override
    public void syncSettings() {
        final apache.rocketmq.v2.Settings settings = getSettings().toProtobuf();
        final TelemetryCommand command = TelemetryCommand.newBuilder().setSettings(settings).build();
        final Set<Endpoints> totalRouteEndpoints = getTotalRouteEndpoints();
        for (Endpoints endpoints : totalRouteEndpoints) {
            try {
                telemetry(endpoints, command);
            } catch (Throwable t) {
                log.error("Failed to telemeter settings, clientId={}, endpoints={}", clientId, endpoints, t);
            }
        }
    }

    public void telemetry(Endpoints endpoints, TelemetryCommand command) {
        try {
            final ClientSessionImpl clientSession = getClientSession(endpoints);
            clientSession.write(command);
        } catch (Throwable t) {
            log.error("Failed to fire write telemetry command, clientId={}, endpoints={}", clientId, endpoints, t);
        }
    }

    private void releaseClientSessions() {
        sessionsLock.readLock().lock();
        try {
            sessionsTable.values().forEach(ClientSessionImpl::release);
        } finally {
            sessionsLock.readLock().unlock();
        }
    }

    public void removeClientSession(Endpoints endpoints, ClientSessionImpl clientSession) {
        sessionsLock.writeLock().lock();
        try {
            log.info("Remove client session, clientId={}, endpoints={}", clientId, endpoints);
            sessionsTable.remove(endpoints, clientSession);
        } finally {
            sessionsLock.writeLock().unlock();
        }
    }

    private ClientSessionImpl getClientSession(Endpoints endpoints) throws ClientException {
        sessionsLock.readLock().lock();
        try {
            final ClientSessionImpl session = sessionsTable.get(endpoints);
            if (null != session) {
                return session;
            }
        } finally {
            sessionsLock.readLock().unlock();
        }
        sessionsLock.writeLock().lock();
        try {
            ClientSessionImpl session = sessionsTable.get(endpoints);
            if (null != session) {
                return session;
            }
            session = new ClientSessionImpl(this, clientConfiguration.getRequestTimeout(), endpoints);
            sessionsTable.put(endpoints, session);
            return session;
        } finally {
            sessionsLock.writeLock().unlock();
        }
    }

    /**
     * Triggered when {@link TopicRouteData} is fetched from remote.
     */
    public ListenableFuture<TopicRouteData> onTopicRouteDataFetched(String topic,
    TopicRouteData topicRouteData) throws ClientException {
        final Set<Endpoints> routeEndpoints = topicRouteData
            .getMessageQueues().stream()
            .map(mq -> mq.getBroker().getEndpoints())
            .collect(Collectors.toSet());
        final Set<Endpoints> existRouteEndpoints = getTotalRouteEndpoints();
        final Set<Endpoints> newEndpoints = new HashSet<>(Sets.difference(routeEndpoints, existRouteEndpoints));
        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (Endpoints endpoints : newEndpoints) {
            final ClientSessionImpl clientSession = getClientSession(endpoints);
            futures.add(clientSession.syncSettings());
        }
        final ListenableFuture<?> future = Futures.allAsList(futures);
        return Futures.transform(future, (Function<Object, TopicRouteData>) input -> {
            topicRouteCache.put(topic, topicRouteData);
            onTopicRouteDataUpdate0(topic, topicRouteData);
            return topicRouteData;
        }, MoreExecutors.directExecutor());
    }

    public void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData) {
    }

    /**
     * This method is invoked while request of message consume verification is received from remote.
     *
     * @param endpoints remote endpoints.
     * @param command   request of message consume verification from remote.
     */
    @Override
    public void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand command) {
        log.warn("Ignore verify message command from remote, which is not expected, clientId={}, command={}",
            clientId, command);
        final String nonce = command.getNonce();
        final Status status = Status.newBuilder().setCode(Code.NOT_IMPLEMENTED).build();
        VerifyMessageResult verifyMessageResult = VerifyMessageResult.newBuilder().setNonce(nonce).build();
        TelemetryCommand telemetryCommand = TelemetryCommand.newBuilder()
            .setVerifyMessageResult(verifyMessageResult)
            .setStatus(status)
            .build();
        try {
            telemetry(endpoints, telemetryCommand);
        } catch (Throwable t) {
            log.warn("Failed to send message verification result, clientId={}", clientId, t);
        }
    }

    /**
     * This method is invoked while request of orphaned transaction recovery is received from remote.
     *
     * @param endpoints remote endpoints.
     * @param command   request of orphaned transaction recovery from remote.
     */
    @Override
    public void onRecoverOrphanedTransactionCommand(Endpoints endpoints, RecoverOrphanedTransactionCommand command) {
        log.warn("Ignore orphaned transaction recovery command from remote, which is not expected, clientId={}, "
            + "command={}", clientId, command);
    }

    private void updateRouteCache() {
        log.info("Start to update route cache for a new round, clientId={}", clientId);
        topicRouteCache.keySet().forEach(topic -> {
            final ListenableFuture<TopicRouteData> future = fetchTopicRoute(topic);
            Futures.addCallback(future, new FutureCallback<TopicRouteData>() {
                @Override
                public void onSuccess(TopicRouteData topicRouteData) {
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Failed to fetch topic route for update cache, topic={}, clientId={}", topic,
                        clientId, t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    /**
     * Wrap notify client termination request.
     */
    public abstract NotifyClientTerminationRequest wrapNotifyClientTerminationRequest();

    /**
     * Notify remote that current client is prepared to be terminated.
     */
    private void notifyClientTermination() {
        log.info("Notify remote that client is terminated, clientId={}", clientId);
        final Set<Endpoints> routeEndpointsSet = getTotalRouteEndpoints();
        final NotifyClientTerminationRequest notifyClientTerminationRequest = wrapNotifyClientTerminationRequest();
        try {
            for (Endpoints endpoints : routeEndpointsSet) {
                clientManager.notifyClientTermination(endpoints, notifyClientTerminationRequest,
                    clientConfiguration.getRequestTimeout());
            }
        } catch (Throwable t) {
            // Should never reach here.
            log.error("[Bug] Exception raised while notifying client's termination, clientId={}", clientId, t);
        }
    }

    public ClientManager getClientManager() {
        return clientManager;
    }

    @Override
    public Endpoints getEndpoints() {
        return endpoints;
    }

    /**
     * @see Client#getClientId()
     */
    @Override
    public ClientId getClientId() {
        return clientId;
    }

    /**
     * @see Client#doHeartbeat()
     */
    @Override
    public void doHeartbeat() {
        final Set<Endpoints> totalEndpoints = getTotalRouteEndpoints();
        final HeartbeatRequest request = wrapHeartbeatRequest();
        for (Endpoints endpoints : totalEndpoints) {
            doHeartbeat(request, endpoints);
        }
    }

    /**
     * Real-time signature generation
     */
    @Override
    public Metadata sign() throws NoSuchAlgorithmException, InvalidKeyException {
        return Signature.sign(clientConfiguration, clientId);
    }

    @Override
    public boolean isSslEnabled() {
        return clientConfiguration.isSslEnabled();
    }

    /**
     * Send heartbeat data to the appointed endpoint
     *
     * @param request   heartbeat data request
     * @param endpoints endpoint to send heartbeat data
     */
    private void doHeartbeat(HeartbeatRequest request, final Endpoints endpoints) {
        try {
            final RpcFuture<HeartbeatRequest, HeartbeatResponse> future = clientManager.heartbeat(endpoints,
                request, clientConfiguration.getRequestTimeout());
            Futures.addCallback(future, new FutureCallback<HeartbeatResponse>() {
                @Override
                public void onSuccess(HeartbeatResponse response) {
                    final Status status = response.getStatus();
                    final Code code = status.getCode();
                    if (Code.OK != code) {
                        log.warn("Failed to send heartbeat, code={}, status message=[{}], endpoints={}, clientId={}",
                            code, status.getMessage(), endpoints, clientId);
                        return;
                    }
                    log.info("Send heartbeat successfully, endpoints={}, clientId={}", endpoints, clientId);
                    final boolean removed = isolated.remove(endpoints);
                    if (removed) {
                        log.info("Rejoin endpoints which is isolated before, clientId={}, endpoints={}", clientId,
                            endpoints);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    log.warn("Failed to send heartbeat, endpoints={}, clientId={}", endpoints, clientId, t);
                }
            }, MoreExecutors.directExecutor());
        } catch (Throwable t) {
            // Should never reach here.
            log.error("[Bug] Exception raised while preparing heartbeat, endpoints={}, clientId={}", endpoints,
                clientId, t);
        }
    }

    /**
     * Wrap heartbeat request
     */
    public abstract HeartbeatRequest wrapHeartbeatRequest();

    /**
     * @see Client#doStats()
     */
    @Override
    public void doStats() {
    }

    private ListenableFuture<TopicRouteData> fetchTopicRoute(final String topic) {
        final ListenableFuture<TopicRouteData> future0 = fetchTopicRoute0(topic);
        final ListenableFuture<TopicRouteData> future = Futures.transformAsync(future0,
            topicRouteData -> onTopicRouteDataFetched(topic, topicRouteData), MoreExecutors.directExecutor());
        Futures.addCallback(future, new FutureCallback<TopicRouteData>() {
            @Override
            public void onSuccess(TopicRouteData topicRouteData) {
                log.info("Fetch topic route successfully, clientId={}, topic={}, topicRouteData={}", clientId,
                    topic, topicRouteData);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Failed to fetch topic route, clientId={}, topic={}", clientId, topic, t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    protected ListenableFuture<TopicRouteData> fetchTopicRoute0(final String topic) {
        Resource topicResource = Resource.newBuilder().setName(topic).build();
        final QueryRouteRequest request = QueryRouteRequest.newBuilder().setTopic(topicResource)
            .setEndpoints(endpoints.toProtobuf()).build();
        final RpcFuture<QueryRouteRequest, QueryRouteResponse> future =
            clientManager.queryRoute(endpoints, request, clientConfiguration.getRequestTimeout());
        return Futures.transformAsync(future, response -> {
            final Status status = response.getStatus();
            StatusChecker.check(status, future);
            final List<MessageQueue> messageQueuesList = response.getMessageQueuesList();
            final TopicRouteData topicRouteData = new TopicRouteData(messageQueuesList);
            return Futures.immediateFuture(topicRouteData);
        }, MoreExecutors.directExecutor());
    }

    protected Set<Endpoints> getTotalRouteEndpoints() {
        Set<Endpoints> totalRouteEndpoints = new HashSet<>();
        for (TopicRouteData topicRouteData : topicRouteCache.values()) {
            totalRouteEndpoints.addAll(topicRouteData.getTotalEndpoints());
        }
        return totalRouteEndpoints;
    }

    protected ListenableFuture<TopicRouteData> getRouteData(final String topic) {
        SettableFuture<TopicRouteData> future0 = SettableFuture.create();
        TopicRouteData topicRouteData = topicRouteCache.get(topic);
        // If route result was cached before, get it directly.
        if (null != topicRouteData) {
            future0.set(topicRouteData);
            return future0;
        }
        inflightRouteFutureLock.lock();
        try {
            // If route was fetched by last in-flight request, get it directly.
            topicRouteData = topicRouteCache.get(topic);
            if (null != topicRouteData) {
                future0.set(topicRouteData);
                return future0;
            }
            Set<SettableFuture<TopicRouteData>> inflightFutures = inflightRouteFutureTable.get(topic);
            // Request is in-flight, return future directly.
            if (null != inflightFutures) {
                inflightFutures.add(future0);
                return future0;
            }
            inflightFutures = new HashSet<>();
            inflightFutures.add(future0);
            inflightRouteFutureTable.put(topic, inflightFutures);
        } finally {
            inflightRouteFutureLock.unlock();
        }
        final ListenableFuture<TopicRouteData> future = fetchTopicRoute(topic);
        Futures.addCallback(future, new FutureCallback<TopicRouteData>() {
            @Override
            public void onSuccess(TopicRouteData topicRouteData) {
                inflightRouteFutureLock.lock();
                try {
                    final Set<SettableFuture<TopicRouteData>> newFutureSet =
                        inflightRouteFutureTable.remove(topic);
                    if (null == newFutureSet) {
                        // Should never reach here.
                        log.error("[Bug] in-flight route futures was empty, topic={}, clientId={}", topic,
                            clientId);
                        return;
                    }
                    log.debug("Fetch topic route successfully, topic={}, in-flight route future "
                        + "size={}, clientId={}", topic, newFutureSet.size(), clientId);
                    for (SettableFuture<TopicRouteData> newFuture : newFutureSet) {
                        newFuture.set(topicRouteData);
                    }
                } catch (Throwable t) {
                    // Should never reach here.
                    log.error("[Bug] Exception raised while update route data, topic={}, clientId={}", topic,
                        clientId, t);
                } finally {
                    inflightRouteFutureLock.unlock();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                inflightRouteFutureLock.lock();
                try {
                    final Set<SettableFuture<TopicRouteData>> newFutureSet =
                        inflightRouteFutureTable.remove(topic);
                    if (null == newFutureSet) {
                        // Should never reach here.
                        log.error("[Bug] in-flight route futures was empty, topic={}, clientId={}", topic, clientId);
                        return;
                    }
                    log.debug("Failed to fetch topic route, topic={}, in-flight route future " +
                        "size={}, clientId={}", topic, newFutureSet.size(), clientId, t);
                    for (SettableFuture<TopicRouteData> future : newFutureSet) {
                        future.setException(t);
                    }
                } finally {
                    inflightRouteFutureLock.unlock();
                }
            }
        }, MoreExecutors.directExecutor());
        return future0;
    }

    public ScheduledExecutorService getScheduler() {
        return clientManager.getScheduler();
    }

    protected <T> T handleClientFuture(ListenableFuture<T> future) throws ClientException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof ClientException) {
                throw (ClientException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new ClientException(null == cause ? e : cause);
        }
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    @Override
    protected String serviceName() {
        return super.serviceName() + "-" + clientId.getIndex();
    }
}
