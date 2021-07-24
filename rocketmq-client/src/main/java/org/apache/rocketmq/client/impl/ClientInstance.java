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
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
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
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.remoting.RpcClient;
import org.apache.rocketmq.client.remoting.RpcClientImpl;
import org.apache.rocketmq.utility.ThreadFactoryImpl;

@Slf4j
public class ClientInstance {
    private static final long RPC_CLIENT_MAX_IDLE_SECONDS = 30 * 60;

    @Getter
    private final String id;

    private final ConcurrentMap<Endpoints, RpcClient> rpcClientTable;

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

    private volatile ScheduledFuture<?> cleanIdleRpcClientsFuture;

    private volatile ScheduledFuture<?> heartbeatFuture;

    private volatile ScheduledFuture<?> logStatsFuture;

    public ClientInstance(String id) {
        this.id = id;
        this.rpcClientTable = new ConcurrentHashMap<Endpoints, RpcClient>();

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

        this.state = new AtomicReference<ServiceState>(ServiceState.READY);
    }

    public void registerClientObserver(ClientObserver clientObserver) {
        synchronized (this) {
            clientObserverTable.put(clientObserver.getClientId(), clientObserver);
        }
    }

    public void unregisterClientObserver(ClientObserver clientObserver) {
        synchronized (this) {
            clientObserverTable.remove(clientObserver.getClientId());
            if (clientObserverTable.isEmpty()) {
                shutdown();
            }
        }
    }

    private void doHealthCheck() {
        log.info("Start to do health check for a new round.");
        for (Map.Entry<String, ClientObserver> entry : clientObserverTable.entrySet()) {
            final ClientObserver observer = entry.getValue();
            observer.doHealthCheck();
        }
    }

    private void clearIdleRpcClients() {
        log.info("Start to clear idle rpc clients for a new round.");
        for (Map.Entry<Endpoints, RpcClient> entry : rpcClientTable.entrySet()) {
            final Endpoints endpoints = entry.getKey();
            final RpcClient client = entry.getValue();

            final long idleSeconds = client.idleSeconds();
            if (idleSeconds > RPC_CLIENT_MAX_IDLE_SECONDS) {
                rpcClientTable.remove(endpoints);
                log.info("Rpc client has been idle too long, endpoints={}, idleSeconds={}, maxIdleSeconds={}",
                         endpoints, idleSeconds, RPC_CLIENT_MAX_IDLE_SECONDS);
            }
        }
    }

    private void doHeartbeat() {
        log.info("Start to send heartbeat for a new round.");
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
            observer.doStats();
        }
    }

    /**
     * Start the instance.
     *
     * @throws ClientException
     */
    public void start() {
        synchronized (this) {
            log.info("Begin to start the client instance.");
            if (!state.compareAndSet(ServiceState.READY, ServiceState.STARTING)) {
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

            cleanIdleRpcClientsFuture = scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                clearIdleRpcClients();
                            } catch (Throwable t) {
                                log.error("Exception occurs while clear idle rpc clients.", t);
                            }
                        }
                    },
                    5,
                    60,
                    TimeUnit.SECONDS);

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
            state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
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
                heartbeatFuture.cancel(false);
            }
            if (null != cleanIdleRpcClientsFuture) {
                cleanIdleRpcClientsFuture.cancel(false);
            }
            if (null != heartbeatFuture) {
                heartbeatFuture.cancel(false);
            }
            if (null != logStatsFuture) {
                logStatsFuture.cancel(false);
            }
            scheduler.shutdown();
            asyncExecutor.shutdown();
            state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED);
            log.info("Shutdown the client instance successfully.");
        }
    }

    /**
     * Get rpc client by remote address, would create client automatically if it does not exist.
     *
     * @param target remote address.
     * @return rpc client.
     */
    private RpcClient getRpcClient(Endpoints endpoints) throws ClientException {
        RpcClient rpcClient = rpcClientTable.get(endpoints);
        if (null != rpcClient) {
            return rpcClient;
        }
        RpcClientImpl newRpcClient;
        try {
            newRpcClient = new RpcClientImpl(endpoints);
        } catch (SSLException e) {
            log.error("Failed to get rpc client, endpoints={}", endpoints);
            throw new ClientException("Failed to get rpc client");
        }
        rpcClientTable.put(endpoints, newRpcClient);

        return newRpcClient;
    }

    // TODO
    public Set<Endpoints> getAllIsolatedEndpoints() {
        return new HashSet<Endpoints>();
    }

    public ListenableFuture<QueryRouteResponse> queryRoute(Endpoints endpoints, Metadata metadata,
                                                           QueryRouteRequest request, long duration,
                                                           TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.queryRoute(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<QueryRouteResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<HeartbeatResponse> heartbeat(Endpoints endpoints, Metadata metadata,
                                                         HeartbeatRequest request, long duration, TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.heartbeat(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<HeartbeatResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<HealthCheckResponse> healthCheck(Endpoints endpoints, Metadata metadata,
                                                             HealthCheckRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.healthCheck(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<HealthCheckResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<SendMessageResponse> sendMessage(Endpoints endpoints, Metadata metadata,
                                                             SendMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.sendMessage(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<SendMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<QueryAssignmentResponse> queryAssignment(Endpoints endpoints, Metadata metadata,
                                                                     QueryAssignmentRequest request, long duration,
                                                                     TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.queryAssignment(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<QueryAssignmentResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<ReceiveMessageResponse> receiveMessage(Endpoints endpoints, Metadata metadata,
                                                                   ReceiveMessageRequest request, long duration,
                                                                   TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.receiveMessage(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<ReceiveMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<AckMessageResponse> ackMessage(Endpoints endpoints, Metadata metadata,
                                                           AckMessageRequest request, long duration,
                                                           TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.ackMessage(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<AckMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<NackMessageResponse> nackMessage(Endpoints endpoints, Metadata metadata,
                                                             NackMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.nackMessage(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<NackMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<EndTransactionResponse> endTransaction(Endpoints endpoints, Metadata metadata,
                                                                   EndTransactionRequest request, long duration,
                                                                   TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.endTransaction(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            SettableFuture<EndTransactionResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<QueryOffsetResponse> queryOffset(Endpoints endpoints, Metadata metadata,
                                                             QueryOffsetRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.queryOffset(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<QueryOffsetResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<PullMessageResponse> pullMessage(Endpoints endpoints, Metadata metadata,
                                                             PullMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.pullMessage(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<PullMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    public ListenableFuture<MultiplexingResponse> multiplexingCall(Endpoints endpoints, Metadata metadata,
                                                                   MultiplexingRequest request, long duration,
                                                                   TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.multiplexingCall(metadata, request, asyncExecutor, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<MultiplexingResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }
}
