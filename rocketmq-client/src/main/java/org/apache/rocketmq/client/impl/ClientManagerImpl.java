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

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
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
import io.grpc.Metadata;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.remoting.RpcClient;
import org.apache.rocketmq.client.remoting.RpcClientImpl;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.utility.ThreadFactoryImpl;

@Slf4j
public class ClientManagerImpl implements ClientManager {
    private static final long RPC_CLIENT_MAX_IDLE_SECONDS = 30 * 60;

    @Getter
    private final String id;

    private final ConcurrentMap<Endpoints, RpcClient> rpcClientTable;

    private final ConcurrentMap<String/* ClientId */, ClientObserver> observerTable;

    @Getter
    private final ScheduledExecutorService scheduler;

    /**
     * Public executor for all async rpc, <strong>should never submit heavy task.</strong>
     */
    private final ThreadPoolExecutor asyncWorker;

    private final AtomicReference<ServiceState> state;

    public ClientManagerImpl(String id) {
        this.id = id;
        this.rpcClientTable = new ConcurrentHashMap<Endpoints, RpcClient>();

        this.observerTable = new ConcurrentHashMap<String, ClientObserver>();

        this.scheduler = new ScheduledThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryImpl("ClientScheduler"));

        this.asyncWorker = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("ClientAsyncWorker"));

        this.state = new AtomicReference<ServiceState>(ServiceState.READY);
    }

    @Override
    public void registerObserver(ClientObserver observer) {
        synchronized (this) {
            observerTable.put(observer.getClientId(), observer);
        }
    }

    @Override
    public void unregisterObserver(ClientObserver observer) {
        synchronized (this) {
            observerTable.remove(observer.getClientId());
            if (observerTable.isEmpty()) {
                shutdown();
            }
        }
    }

    private void doHealthCheck() {
        log.info("Start to do health check for a new round.");
        for (Map.Entry<String, ClientObserver> entry : observerTable.entrySet()) {
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
        for (Map.Entry<String, ClientObserver> entry : observerTable.entrySet()) {
            final String clientId = entry.getKey();
            final ClientObserver observer = entry.getValue();
            log.info("Start to send heartbeat for clientId={}", clientId);
            observer.doHeartbeat();
        }
    }

    private void doLogStats() {
        log.info("Start to log stats for a new round");
        for (Map.Entry<String, ClientObserver> entry : observerTable.entrySet()) {
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
    @Override
    public void start() {
        synchronized (this) {
            log.info("Begin to start the client manager.");
            if (!state.compareAndSet(ServiceState.READY, ServiceState.STARTING)) {
                log.warn("The client manager has been started before.");
                return;
            }
            scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                doHealthCheck();
                            } catch (Throwable t) {
                                log.error("Exception raised while health check.", t);
                            }
                        }
                    },
                    5,
                    15,
                    TimeUnit.SECONDS
            );

            scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                clearIdleRpcClients();
                            } catch (Throwable t) {
                                log.error("Exception raised while clear idle rpc clients.", t);
                            }
                        }
                    },
                    5,
                    60,
                    TimeUnit.SECONDS);

            scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                doHeartbeat();
                            } catch (Throwable t) {
                                log.error("Exception raised while heartbeat.", t);
                            }
                        }
                    },
                    1,
                    10,
                    TimeUnit.SECONDS
            );

            scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                doLogStats();
                            } catch (Throwable t) {
                                log.error("Exception raised while log stats", t);
                            }
                        }
                    },
                    1,
                    1,
                    TimeUnit.SECONDS
            );
            state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
            log.info("The client manager starts successfully.");
        }
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            log.info("Begin to shutdown the client manager.");
            if (!state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING)) {
                log.warn("ClientInstance has not been started before");
                return;
            }
            ClientManagerFactory.getInstance().removeClientInstance(id);
            scheduler.shutdown();
            asyncWorker.shutdown();
            state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED);
            log.info("Shutdown the client manager successfully.");
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

    @Override
    public ListenableFuture<QueryRouteResponse> queryRoute(Endpoints endpoints, Metadata metadata,
                                                           QueryRouteRequest request, long duration,
                                                           TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.queryRoute(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<QueryRouteResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<HeartbeatResponse> heartbeat(Endpoints endpoints, Metadata metadata,
                                                         HeartbeatRequest request, long duration, TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.heartbeat(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<HeartbeatResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<HealthCheckResponse> healthCheck(Endpoints endpoints, Metadata metadata,
                                                             HealthCheckRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.healthCheck(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<HealthCheckResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<SendMessageResponse> sendMessage(Endpoints endpoints, Metadata metadata,
                                                             SendMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.sendMessage(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<SendMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<QueryAssignmentResponse> queryAssignment(Endpoints endpoints, Metadata metadata,
                                                                     QueryAssignmentRequest request, long duration,
                                                                     TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.queryAssignment(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<QueryAssignmentResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<ReceiveMessageResponse> receiveMessage(Endpoints endpoints, Metadata metadata,
                                                                   ReceiveMessageRequest request, long duration,
                                                                   TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.receiveMessage(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<ReceiveMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<AckMessageResponse> ackMessage(Endpoints endpoints, Metadata metadata,
                                                           AckMessageRequest request, long duration,
                                                           TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.ackMessage(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<AckMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<NackMessageResponse> nackMessage(Endpoints endpoints, Metadata metadata,
                                                             NackMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.nackMessage(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<NackMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(
            Endpoints endpoints, Metadata metadata, ForwardMessageToDeadLetterQueueRequest request, long duration,
            TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.forwardMessageToDeadLetterQueue(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<ForwardMessageToDeadLetterQueueResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<EndTransactionResponse> endTransaction(Endpoints endpoints, Metadata metadata,
                                                                   EndTransactionRequest request, long duration,
                                                                   TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.endTransaction(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            SettableFuture<EndTransactionResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<QueryOffsetResponse> queryOffset(Endpoints endpoints, Metadata metadata,
                                                             QueryOffsetRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.queryOffset(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<QueryOffsetResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<PullMessageResponse> pullMessage(Endpoints endpoints, Metadata metadata,
                                                             PullMessageRequest request, long duration,
                                                             TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.pullMessage(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<PullMessageResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }

    @Override
    public ListenableFuture<MultiplexingResponse> multiplexingCall(Endpoints endpoints, Metadata metadata,
                                                                   MultiplexingRequest request, long duration,
                                                                   TimeUnit timeUnit) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.multiplexingCall(metadata, request, asyncWorker, duration, timeUnit);
        } catch (Throwable t) {
            final SettableFuture<MultiplexingResponse> future = SettableFuture.create();
            future.setException(t);
            return future;
        }
    }
}
