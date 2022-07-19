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

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.misc.ExecutorServices;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.RpcClient;
import org.apache.rocketmq.client.java.rpc.RpcClientImpl;
import org.apache.rocketmq.client.java.rpc.RpcInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see ClientManager
 */
public class ClientManagerImpl extends ClientManager {
    public static final Duration RPC_CLIENT_MAX_IDLE_DURATION = Duration.ofMinutes(30);

    public static final Duration RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY = Duration.ofSeconds(5);
    public static final Duration RPC_CLIENT_IDLE_CHECK_PERIOD = Duration.ofMinutes(1);

    public static final Duration HEART_BEAT_INITIAL_DELAY = Duration.ofSeconds(1);
    public static final Duration HEART_BEAT_PERIOD = Duration.ofSeconds(10);

    public static final Duration LOG_STATS_INITIAL_DELAY = Duration.ofSeconds(60);
    public static final Duration LOG_STATS_PERIOD = Duration.ofSeconds(60);

    public static final Duration SYNC_SETTINGS_DELAY = Duration.ofSeconds(1);
    public static final Duration SYNC_SETTINGS_PERIOD = Duration.ofMinutes(5);

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientManagerImpl.class);

    private final Client client;
    @GuardedBy("rpcClientTableLock")
    private final Map<Endpoints, RpcClient> rpcClientTable;
    private final ReadWriteLock rpcClientTableLock;

    /**
     * In charge of all scheduled tasks.
     */
    private final ScheduledExecutorService scheduler;

    /**
     * Public executor for all async RPCs, <strong>should never submit a heavy task.</strong>
     */
    private final ExecutorService asyncWorker;

    public ClientManagerImpl(Client client) {
        this.client = client;
        this.rpcClientTable = new HashMap<>();
        this.rpcClientTableLock = new ReentrantReadWriteLock();
        this.scheduler = new ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryImpl("ClientScheduler"));

        this.asyncWorker = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("ClientAsyncWorker"));
    }

    /**
     * It is well-founded that a {@link RpcClient} is deprecated if it is idle for a long time, so it is essential to
     * clear it.
     *
     * @throws InterruptedException if the thread has been interrupted
     */
    private void clearIdleRpcClients() throws InterruptedException {
        rpcClientTableLock.writeLock().lock();
        try {
            final Iterator<Map.Entry<Endpoints, RpcClient>> it = rpcClientTable.entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<Endpoints, RpcClient> entry = it.next();
                final Endpoints endpoints = entry.getKey();
                final RpcClient client = entry.getValue();

                final Duration idleDuration = client.idleDuration();
                if (idleDuration.compareTo(RPC_CLIENT_MAX_IDLE_DURATION) > 0) {
                    it.remove();
                    client.shutdown();
                    LOGGER.info("Rpc client has been idle for a long time, endpoints={}, idleDuration={}, " +
                        "rpcClientMaxIdleDuration={}", endpoints, idleDuration, RPC_CLIENT_MAX_IDLE_DURATION);
                }
            }
        } finally {
            rpcClientTableLock.writeLock().unlock();
        }
    }

    /**
     * Return the RPC client by remote {@link Endpoints}, would create the client automatically if it does not exist.
     *
     * <p>In case of the occasion that {@link RpcClient} is garbage collected before shutdown when invoked
     * concurrently, lock here is essential.
     *
     * @param endpoints remote endpoints.
     * @return RPC client.
     */
    private RpcClient getRpcClient(Endpoints endpoints) throws ClientException {
        RpcClient rpcClient;
        rpcClientTableLock.readLock().lock();
        try {
            rpcClient = rpcClientTable.get(endpoints);
            if (null != rpcClient) {
                return rpcClient;
            }
        } finally {
            rpcClientTableLock.readLock().unlock();
        }
        rpcClientTableLock.writeLock().lock();
        try {
            rpcClient = rpcClientTable.get(endpoints);
            if (null != rpcClient) {
                return rpcClient;
            }
            try {
                rpcClient = new RpcClientImpl(endpoints);
            } catch (SSLException e) {
                LOGGER.error("Failed to get rpc client, endpoints={}", endpoints);
                throw new ClientException("Failed to generate RPC client", e);
            }
            rpcClientTable.put(endpoints, rpcClient);
            return rpcClient;
        } finally {
            rpcClientTableLock.writeLock().unlock();
        }
    }

    @Override
    public ListenableFuture<RpcInvocation<QueryRouteResponse>> queryRoute(Endpoints endpoints, Metadata metadata,
        QueryRouteRequest request, Duration duration) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.queryRoute(metadata, request, asyncWorker, duration);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<RpcInvocation<HeartbeatResponse>> heartbeat(Endpoints endpoints, Metadata metadata,
        HeartbeatRequest request, Duration duration) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.heartbeat(metadata, request, asyncWorker, duration);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<RpcInvocation<SendMessageResponse>> sendMessage(Endpoints endpoints, Metadata metadata,
        SendMessageRequest request, Duration duration) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.sendMessage(metadata, request, asyncWorker, duration);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<RpcInvocation<QueryAssignmentResponse>> queryAssignment(Endpoints endpoints,
        Metadata metadata, QueryAssignmentRequest request, Duration duration) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.queryAssignment(metadata, request, asyncWorker, duration);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<RpcInvocation<Iterator<ReceiveMessageResponse>>> receiveMessage(Endpoints endpoints,
        Metadata metadata, ReceiveMessageRequest request, Duration duration) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.receiveMessage(metadata, request, asyncWorker, duration);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<RpcInvocation<AckMessageResponse>> ackMessage(Endpoints endpoints, Metadata metadata,
        AckMessageRequest request, Duration duration) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.ackMessage(metadata, request, asyncWorker, duration);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<RpcInvocation<ChangeInvisibleDurationResponse>> changeInvisibleDuration(
        Endpoints endpoints, Metadata metadata, ChangeInvisibleDurationRequest request, Duration duration) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.changeInvisibleDuration(metadata, request, asyncWorker, duration);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<RpcInvocation<ForwardMessageToDeadLetterQueueResponse>> forwardMessageToDeadLetterQueue(
        Endpoints endpoints, Metadata metadata, ForwardMessageToDeadLetterQueueRequest request, Duration duration) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.forwardMessageToDeadLetterQueue(metadata, request, asyncWorker, duration);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<RpcInvocation<EndTransactionResponse>> endTransaction(Endpoints endpoints,
        Metadata metadata, EndTransactionRequest request, Duration duration) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.endTransaction(metadata, request, asyncWorker, duration);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<RpcInvocation<NotifyClientTerminationResponse>> notifyClientTermination(
        Endpoints endpoints, Metadata metadata, NotifyClientTerminationRequest request, Duration duration) {
        try {
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.notifyClientTermination(metadata, request, asyncWorker, duration);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(Endpoints endpoints, Metadata metadata, Duration duration,
        StreamObserver<TelemetryCommand> responseObserver) throws ClientException {
        final RpcClient rpcClient = getRpcClient(endpoints);
        return rpcClient.telemetry(metadata, asyncWorker, duration, responseObserver);
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return this.scheduler;
    }

    @Override
    protected void startUp() {
        LOGGER.info("Begin to start the client manager");
        scheduler.scheduleWithFixedDelay(
            () -> {
                try {
                    clearIdleRpcClients();
                } catch (Throwable t) {
                    LOGGER.error("Exception raised while clear idle rpc clients.", t);
                }
            },
            RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY.toNanos(),
            RPC_CLIENT_IDLE_CHECK_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        );

        scheduler.scheduleWithFixedDelay(
            () -> {
                try {
                    client.doHeartbeat();
                } catch (Throwable t) {
                    LOGGER.error("Exception raised while heartbeat.", t);
                }
            },
            HEART_BEAT_INITIAL_DELAY.toNanos(),
            HEART_BEAT_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        );

        scheduler.scheduleWithFixedDelay(
            () -> {
                try {
                    client.doStats();
                } catch (Throwable t) {
                    LOGGER.error("Exception raised while log stats.", t);
                }
            },
            LOG_STATS_INITIAL_DELAY.toNanos(),
            LOG_STATS_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        );

        scheduler.scheduleWithFixedDelay(
            () -> {
                try {
                    client.syncSettings();
                } catch (Throwable t) {
                    LOGGER.error("Exception raised during the setting synchronizing.", t);
                }
            },
            SYNC_SETTINGS_DELAY.toNanos(),
            SYNC_SETTINGS_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        );
        LOGGER.info("The client manager starts successfully");
    }

    @Override
    protected void shutDown() throws IOException {
        LOGGER.info("Begin to shutdown the client manager");
        scheduler.shutdown();
        try {
            if (!ExecutorServices.awaitTerminated(scheduler)) {
                LOGGER.error("[Bug] Timeout to shutdown the client scheduler");
            } else {
                LOGGER.info("Shutdown the client scheduler successfully");
            }
            rpcClientTableLock.writeLock().lock();
            try {
                final Iterator<Map.Entry<Endpoints, RpcClient>> it = rpcClientTable.entrySet().iterator();
                while (it.hasNext()) {
                    final Map.Entry<Endpoints, RpcClient> entry = it.next();
                    final RpcClient rpcClient = entry.getValue();
                    it.remove();
                    rpcClient.shutdown();
                }
            } finally {
                rpcClientTableLock.writeLock().unlock();
            }
            LOGGER.info("Shutdown all rpc client(s) successfully");
            asyncWorker.shutdown();
            if (!ExecutorServices.awaitTerminated(asyncWorker)) {
                LOGGER.error("[Bug] Timeout to shutdown the client async worker");
            } else {
                LOGGER.info("Shutdown the client async worker successfully");
            }
        } catch (InterruptedException e) {
            LOGGER.error("[Bug] Unexpected exception raised while shutdown client manager", e);
            throw new IOException(e);
        }
        LOGGER.info("Shutdown the client manager successfully");
    }
}
