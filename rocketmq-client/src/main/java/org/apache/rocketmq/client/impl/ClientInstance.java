package org.apache.rocketmq.client.impl;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.MultiplexingRequest;
import apache.rocketmq.v1.MultiplexingResponse;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.grpc.Metadata;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.net.ssl.SSLException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.remoting.RpcClient;
import org.apache.rocketmq.client.remoting.RpcClientImpl;
import org.apache.rocketmq.client.remoting.RpcTarget;
import org.apache.rocketmq.utility.ThreadFactoryImpl;

@Slf4j
public class ClientInstance {
    @Getter
    private final String id;

    private final ConcurrentMap<RpcTarget, RpcClient> rpcClientTable;

    @GuardedBy("unhealthyEndpointsLock")
    private final Set<Endpoints> unhealthyEndpointsSet;
    private final ReadWriteLock unhealthyEndpointsLock;

    private final ConcurrentMap<String/* ClientId */, ClientObserver> clientObserverTable;

    @Getter
    private final ScheduledExecutorService scheduler;
    /**
     * Public executor for all async rpc, <strong>should never submit heavy task.</strong>
     */
    @Getter
    private final ThreadPoolExecutor asyncExecutor;

    private final AtomicReference<ServiceState> state;

    private volatile ScheduledFuture<?> healthCheckFuture;

    private volatile ScheduledFuture<?> heartbeatFuture;

    private volatile ScheduledFuture<?> logStatsFuture;

    public ClientInstance(String id) {
        this.id = id;
        this.rpcClientTable = new ConcurrentHashMap<RpcTarget, RpcClient>();

        this.unhealthyEndpointsLock = new ReentrantReadWriteLock();
        this.unhealthyEndpointsSet = new HashSet<Endpoints>();

        this.clientObserverTable = new ConcurrentHashMap<String, ClientObserver>();

        this.scheduler = new ScheduledThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryImpl("ClientInstanceScheduler"));

        this.asyncExecutor = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("ClientAsyncThread"));

        this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);
    }

    public void registerClientObserver(ClientObserver clientObserver) {
        synchronized (this) {
            clientObserverTable.put(clientObserver.getClientId(), clientObserver);
        }
    }

    public void unregisterClientObserver(ClientObserver clientObserver) {
        synchronized (this) {
            clientObserverTable.remove(clientObserver.getClientId(), clientObserver);
            if (clientObserverTable.isEmpty()) {
                shutdown();
            }
        }
    }

    private void doHealthCheck() {
        cleanOfflineRpcClients();
        clientHealthCheck();
    }

    private void cleanOfflineRpcClients() {
        // Collect all endpoints that needs to be reserved.
        Set<Endpoints> rpcClientsNeedReserve = new HashSet<Endpoints>();
        for (ClientObserver observer : clientObserverTable.values()) {
            rpcClientsNeedReserve.addAll(observer.getEndpointsNeedHeartbeat());
        }
        for (RpcTarget target : rpcClientTable.keySet()) {
            // Target is name server, skip it.
            if (!target.isNeedHeartbeat()) {
                continue;
            }
            // Target is offline, clean it.
            if (!rpcClientsNeedReserve.contains(target.getEndpoints())) {
                rpcClientTable.remove(target);
            }
        }
    }

    // TODO: not implemented yet.
    private void clientHealthCheck() {
        return;
    }

    private Set<Endpoints> getAllUnhealthyEndpoints() {
        unhealthyEndpointsLock.readLock().lock();
        try {
            return new HashSet<Endpoints>(unhealthyEndpointsSet);
        } finally {
            unhealthyEndpointsLock.readLock().unlock();
        }
    }

    private void doHeartbeat() {
        log.info("Start to send heartbeat for a new round");
        for (Map.Entry<String, ClientObserver> entry : clientObserverTable.entrySet()) {
            final String clientId = entry.getKey();
            final ClientObserver observer = entry.getValue();
            log.info("Start to send heartbeat for clientId={}", clientId);
            observer.doHeartbeat();
        }
    }

    private void doLogStats() {
        log.info("Start to log stats for a new round");
        for (Map.Entry<String, ClientObserver> entry : clientObserverTable.entrySet()) {
            final String clientId = entry.getKey();
            final ClientObserver observer = entry.getValue();
            log.info("Log stats for clientId={}", clientId);
            observer.logStats();
        }
    }

    /**
     * Start the instance.
     *
     * @throws MQClientException
     */
    public void start() {
        synchronized (this) {
            log.info("Begin to start the client instance.");
            if (!state.compareAndSet(ServiceState.CREATED, ServiceState.READY)) {
                log.warn("The client instance has been started before.");
                return;
            }
            healthCheckFuture = scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                doHealthCheck();
                            } catch (Throwable t) {
                                log.error("Exception occurs while health check.", t);
                            }
                        }
                    },
                    5,
                    15,
                    TimeUnit.SECONDS
            );

            heartbeatFuture = scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                doHeartbeat();
                            } catch (Throwable t) {
                                log.error("Exception occurs while heartbeat.", t);
                            }
                        }
                    },
                    1,
                    10,
                    TimeUnit.SECONDS
            );

            logStatsFuture = scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                doLogStats();
                            } catch (Throwable t) {
                                log.error("Exception occurs while log stats", t);
                            }
                        }
                    },
                    1,
                    1,
                    TimeUnit.SECONDS
            );
            state.compareAndSet(ServiceState.READY, ServiceState.STARTED);
            log.info("The client instance starts successfully.");
        }
    }

    public void shutdown() {
        synchronized (this) {
            log.info("Begin to shutdown the client instance.");
            if (!state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING)) {
                log.warn("ClientInstance has not been started before");
                return;
            }
            ClientManager.getInstance().removeClientInstance(id);
            if (null != healthCheckFuture) {
                healthCheckFuture.cancel(false);
            }
            if (null != heartbeatFuture) {
                heartbeatFuture.cancel(false);
            }
            if (null != logStatsFuture) {
                logStatsFuture.cancel(false);
            }
            scheduler.shutdown();
            asyncExecutor.shutdown();
            state.compareAndSet(ServiceState.STOPPING, ServiceState.READY);
            log.info("Shutdown the client instance successfully.");
        }
    }

    /**
     * Get rpc client by remote address, would create client automatically if it does not exist.
     *
     * @param target remote address.
     * @return rpc client.
     */
    private RpcClient getRpcClient(RpcTarget target) throws MQClientException {
        RpcClient rpcClient = rpcClientTable.get(target);
        if (null != rpcClient) {
            return rpcClient;
        }
        RpcClientImpl newRpcClient;
        try {
            newRpcClient = new RpcClientImpl(target);
        } catch (SSLException e) {
            log.error("Failed to get rpc client, endpoints={}", target.getEndpoints());
            throw new MQClientException("Failed to get rpc client");
        }
        rpcClientTable.put(target, newRpcClient);

        return newRpcClient;
    }

    // TODO
    public Set<Endpoints> getAllIsolatedEndpoints() {
        return new HashSet<Endpoints>();
    }

    public ListenableFuture<QueryRouteResponse> queryRoute(RpcTarget target, Metadata metadata,
                                                           QueryRouteRequest request, long duration,
                                                           TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(target);
            return rpcClient.queryRoute(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<QueryRouteResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<HeartbeatResponse> heartbeat(RpcTarget target, Metadata metadata,
                                                         HeartbeatRequest request, long duration, TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(target);
            return rpcClient.heartbeat(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<HeartbeatResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<HealthCheckResponse> healthCheck(RpcTarget target, Metadata metadata,
                                                             HealthCheckRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(target);
            return rpcClient.healthCheck(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<HealthCheckResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<SendMessageResponse> sendMessage(RpcTarget target, Metadata metadata,
                                                             SendMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(target);
            return rpcClient.sendMessage(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<SendMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<QueryAssignmentResponse> queryAssignment(RpcTarget target, Metadata metadata,
                                                                     QueryAssignmentRequest request, long duration,
                                                                     TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(target);
            return rpcClient.queryAssignment(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<QueryAssignmentResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<ReceiveMessageResponse> receiveMessage(RpcTarget target, Metadata metadata,
                                                                   ReceiveMessageRequest request, long duration,
                                                                   TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(target);
            return rpcClient.receiveMessage(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<ReceiveMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<AckMessageResponse> ackMessage(RpcTarget target, Metadata metadata,
                                                           AckMessageRequest request, long duration,
                                                           TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(target);
            return rpcClient.ackMessage(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<AckMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<NackMessageResponse> nackMessage(RpcTarget target, Metadata metadata,
                                                             NackMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(target);
            return rpcClient.nackMessage(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<NackMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<EndTransactionResponse> endTransaction(RpcTarget target, Metadata metadata,
                                                                   EndTransactionRequest request, long duration,
                                                                   TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(target);
            return rpcClient.endTransaction(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            SettableFuture<EndTransactionResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<PullMessageResponse> pullMessage(RpcTarget target, Metadata metadata,
                                                             PullMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(target);
            return rpcClient.pullMessage(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<PullMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<MultiplexingResponse> multiplexingCall(RpcTarget target, Metadata metadata,
                                                                   MultiplexingRequest request, long duration,
                                                                   TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(target);
            return rpcClient.multiplexingCall(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<MultiplexingResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }
}
