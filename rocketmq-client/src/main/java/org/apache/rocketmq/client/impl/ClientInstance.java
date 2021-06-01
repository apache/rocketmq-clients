package org.apache.rocketmq.client.impl;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.Digest;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.ResponseCommon;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SystemAttribute;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.Status;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.CommunicationMode;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.consumer.ConsumerObserver;
import org.apache.rocketmq.client.impl.consumer.TopicAssignmentInfo;
import org.apache.rocketmq.client.impl.producer.ProducerObserver;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.message.protocol.TransactionPhase;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.misc.TopAddressing;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.remoting.RPCClient;
import org.apache.rocketmq.client.remoting.RPCClientImpl;
import org.apache.rocketmq.client.remoting.RpcTarget;
import org.apache.rocketmq.client.remoting.SendMessageResponseCallback;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.apache.rocketmq.utility.UtilAll;

@Slf4j
public class ClientInstance {
    private static final int MAX_ASYNC_QUEUE_TASK_NUM = 1024;
    private static final AtomicInteger nameServerIndex = new AtomicInteger(0);

    private static final long RPC_DEFAULT_TIMEOUT_MILLIS = 3 * 1000;
    /**
     * Usage of {@link RPCClientImpl#queryRoute(QueryRouteRequest, long, TimeUnit)} are
     * usually invokes the first call of gRPC, which need warm up in most case.
     */
    private static final long FETCH_TOPIC_ROUTE_INFO_TIMEOUT_MILLIS = 15 * 1000;

    private final ClientInstanceConfig clientInstanceConfig;
    @Setter
    private String tenantId = "";

    @Getter
    private final ScheduledExecutorService scheduler =
            new ScheduledThreadPoolExecutor(4, new ThreadFactoryImpl("ClientInstanceScheduler_"));

    private final ConcurrentMap<MqRpcTarget, RPCClient> clientTable;

    private final ThreadPoolExecutor callbackExecutor;
    private final Semaphore callbackSemaphore;

    private ThreadPoolExecutor sendCallbackExecutor;
    private Semaphore sendCallbackSemaphore;

    private final List<String> nameServerList;
    private final ReadWriteLock nameServerLock;

    private final TopAddressing topAddressing;

    private final ConcurrentMap<String, ProducerObserver> producerObserverTable;
    private final ConcurrentMap<String, ConsumerObserver> consumerObserverTable;

    private final ConcurrentHashMap<String /* Topic */, TopicRouteData> topicRouteTable;

    private final AtomicReference<ServiceState> state;


    public ClientInstance(ClientInstanceConfig clientInstanceConfig, List<String> nameServerList) {
        this.clientInstanceConfig = clientInstanceConfig;
        this.clientTable = new ConcurrentHashMap<MqRpcTarget, RPCClient>();
        this.callbackExecutor =
                new ThreadPoolExecutor(
                        Runtime.getRuntime().availableProcessors(),
                        Runtime.getRuntime().availableProcessors(),
                        60,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>(MAX_ASYNC_QUEUE_TASK_NUM),
                        new ThreadFactoryImpl("ClientCallbackThread_"));
        this.callbackSemaphore = new Semaphore(UtilAll.getThreadParallelCount(callbackExecutor));

        this.sendCallbackExecutor = null;
        this.sendCallbackSemaphore = null;

        this.nameServerList = nameServerList;
        this.nameServerLock = new ReentrantReadWriteLock();

        this.topAddressing = new TopAddressing();

        this.producerObserverTable = new ConcurrentHashMap<String, ProducerObserver>();
        this.consumerObserverTable = new ConcurrentHashMap<String, ConsumerObserver>();

        this.topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();

        this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);
    }

    public void setSendCallbackExecutor(ThreadPoolExecutor sendCallbackExecutor) {
        this.sendCallbackExecutor = sendCallbackExecutor;
        this.sendCallbackSemaphore =
                new Semaphore(UtilAll.getThreadParallelCount(sendCallbackExecutor));
    }

    private void updateNameServerListFromTopAddressing() throws IOException {
        final List<String> nameServerList = topAddressing.fetchNameServerAddresses();
        this.setNameServerList(nameServerList);
    }

    public synchronized void start() throws MQClientException {
        if (ServiceState.STARTED == state.get()) {
            return;
        }
        if (!state.compareAndSet(ServiceState.CREATED, ServiceState.STARTING)) {
            throw new MQClientException(
                    "The client instance has attempted to be stared before, state=" + state.get());
        }

        // Only for internal usage of Alibaba group.
        if (nameServerListIsEmpty()) {
            try {
                updateNameServerListFromTopAddressing();
            } catch (Throwable t) {
                throw new MQClientException(
                        "Failed to fetch name server list from top address while starting.", t);
            }
            scheduler.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                updateNameServerListFromTopAddressing();
                            } catch (Throwable t) {
                                log.error(
                                        "Exception occurs while updating name server list from top addressing", t);
                            }
                        }
                    },
                    3 * 1000,
                    30 * 1000,
                    TimeUnit.MILLISECONDS);
        }

        scheduler.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            updateRouteInfo();
                        } catch (Throwable t) {
                            log.error("Exception occurs while updating route info.", t);
                        }
                    }
                },
                10 * 1000,
                60 * 1000,
                TimeUnit.MILLISECONDS);

        scheduler.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            scanConsumersLoadAssignments();
                        } catch (Throwable t) {
                            log.error("Exception occurs while scanning load assignments of consumers.", t);
                        }
                    }
                },
                1000,
                5 * 1000,
                TimeUnit.MILLISECONDS);

        scheduler.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            restoreIsolatedTarget();
                        } catch (Throwable t) {
                            log.error("Exception occurs while restoring isolated target.", t);
                        }
                    }
                },
                5 * 1000,
                15 * 1000,
                TimeUnit.MILLISECONDS);

        scheduler.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            cleanOutdatedClient();
                        } catch (Throwable t) {
                            log.error("Exception occurs while cleaning outdated client.", t);
                        }
                    }
                },
                30 * 1000,
                60 * 1000,
                TimeUnit.MILLISECONDS);

        scheduler.scheduleWithFixedDelay(
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
                0,
                30 * 1000,
                TimeUnit.MILLISECONDS);

        scheduler.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            logStats();
                        } catch (Throwable t) {
                            log.error("Exception occurs while logging stats.", t);
                        }
                    }
                },
                1000,
                1000,
                TimeUnit.MILLISECONDS);

        state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
    }

    public synchronized void shutdown() throws MQClientException {
        if (ServiceState.STOPPED == state.get()) {
            return;
        }
        if (!producerObserverTable.isEmpty()) {
            log.info(
                    "Not all producerObserver has been unregistered, producerObserver num={}",
                    producerObserverTable.size());
            return;
        }
        if (!consumerObserverTable.isEmpty()) {
            log.info(
                    "Not all consumerObserver has been unregistered, consumerObserver num={}",
                    consumerObserverTable.size());
            return;
        }
        state.compareAndSet(ServiceState.STARTING, ServiceState.STOPPING);
        state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING);
        final ServiceState serviceState = state.get();
        if (ServiceState.STOPPING == serviceState) {
            scheduler.shutdown();
            callbackExecutor.shutdown();
            if (state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED)) {
                log.info("Shutdown ClientInstance successfully");
                return;
            }
        }
        throw new MQClientException("Failed to shutdown consumer, state=" + state.get());
    }

    private void logStats() {
        final ServiceState serviceState = state.get();
        if (ServiceState.STARTED != serviceState && ServiceState.STARTING != serviceState) {
            log.warn("Unexpected client instance state={}", serviceState);
            return;
        }
        for (ProducerObserver producerObserver : producerObserverTable.values()) {
            producerObserver.logStats();
        }
        for (ConsumerObserver consumerObserver : consumerObserverTable.values()) {
            consumerObserver.logStats();
        }
    }

    private void doHeartbeat() {
        final ServiceState serviceState = state.get();
        if (ServiceState.STARTED != serviceState && ServiceState.STARTING != serviceState) {
            log.warn("Unexpected client instance state={}", serviceState);
            return;
        }

        log.debug("Start to send heartbeat for a new round.");

        final HeartbeatRequest.Builder builder = HeartbeatRequest.newBuilder();

        for (ProducerObserver producerObserver : producerObserverTable.values()) {
            builder.addHeartbeats(producerObserver.prepareHeartbeatData());
        }

        for (ConsumerObserver consumerObserver : consumerObserverTable.values()) {
            builder.addHeartbeats(consumerObserver.prepareHeartbeatData());
        }

        final HeartbeatRequest request = builder.build();

        Set<MqRpcTarget> filteredTarget = new HashSet<MqRpcTarget>();
        for (MqRpcTarget rpcTarget : clientTable.keySet()) {
            if (!rpcTarget.needHeartbeat) {
                return;
            }
            filteredTarget.add(rpcTarget);
        }

        for (MqRpcTarget rpcTarget : filteredTarget) {
            final RPCClient rpcClient = clientTable.get(rpcTarget);
            if (null == rpcClient) {
                continue;
            }
            final HeartbeatResponse response =
                    rpcClient.heartbeat(request, RPC_DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            final Status status = response.getCommon().getStatus();
            final Code code = Code.forNumber(status.getCode());
            final String target = rpcTarget.getTarget();
            if (Code.OK != code) {
                log.warn("Failed to send heartbeat to target, responseCode={}, target={}", code, target);
                continue;
            }
            log.debug("Send heartbeat to target successfully, target={}", target);
        }
    }

    private void restoreIsolatedTarget() {
        final ServiceState serviceState = state.get();
        if (ServiceState.STARTED != serviceState && ServiceState.STARTING != serviceState) {
            log.warn("Unexpected client instance state={}", serviceState);
            return;
        }

        for (Map.Entry<MqRpcTarget, RPCClient> entry : clientTable.entrySet()) {
            final MqRpcTarget rpcTarget = entry.getKey();
            final RPCClient rpcClient = entry.getValue();
            if (!rpcClient.isIsolated()) {
                continue;
            }
            final String target = rpcTarget.getTarget();
            final HealthCheckRequest request =
                    HealthCheckRequest.newBuilder().setClientHost(target).build();
            final HealthCheckResponse response =
                    rpcClient.healthCheck(request, RPC_DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            final Status status = response.getCommon().getStatus();
            final Code code = Code.forNumber(status.getCode());
            if (Code.OK != code) {
                rpcTarget.setIsolated(false);
                log.info("Isolated target={} has been restored", target);
                continue;
            }
            log.debug("Isolated target={} was not restored", target);
        }
    }

    private void cleanOutdatedClient() {
        final ServiceState serviceState = state.get();
        if (ServiceState.STARTED != serviceState && ServiceState.STARTING != serviceState) {
            log.warn("Unexpected client instance state={}", serviceState);
            return;
        }

        Set<String> currentEndpoints = new HashSet<String>();
        nameServerLock.readLock().lock();
        try {
            currentEndpoints.addAll(nameServerList);
        } finally {
            nameServerLock.readLock().unlock();
        }
        for (TopicRouteData topicRouteData : topicRouteTable.values()) {
            final Set<String> endpoints = topicRouteData.getAllEndpoints();
            currentEndpoints.addAll(endpoints);
        }

        for (MqRpcTarget rpcTarget : clientTable.keySet()) {
            if (!currentEndpoints.contains(rpcTarget.getTarget())) {
                clientTable.remove(rpcTarget);
            }
        }
    }

    private void setNameServerList(List<String> nameServerList) {
        nameServerLock.writeLock().lock();
        try {
            this.nameServerList.clear();
            this.nameServerList.addAll(nameServerList);
        } finally {
            nameServerLock.writeLock().unlock();
        }
    }

    public boolean nameServerListIsEmpty() {
        nameServerLock.readLock().lock();
        try {
            return nameServerList.isEmpty();
        } finally {
            nameServerLock.readLock().unlock();
        }
    }

    /**
     * Update topic route info from name server and notify observer if changed.
     */
    private void updateRouteInfo() {
        final ServiceState serviceState = state.get();
        if (ServiceState.STARTED != serviceState && ServiceState.STARTING != serviceState) {
            log.warn("Unexpected client instance state={}", serviceState);
            return;
        }
        final Set<String> topics = new HashSet<String>(topicRouteTable.keySet());
        if (topics.isEmpty()) {
            return;
        }
        for (String topic : topics) {
            boolean needNotify = false;
            TopicRouteData after;

            try {
                after = fetchTopicRouteData(topic);
            } catch (Throwable t) {
                log.warn("Failed to fetch topic route from name server, topic={}", topic);
                continue;
            }

            final TopicRouteData before = topicRouteTable.get(topic);
            if (!after.equals(before)) {
                topicRouteTable.put(topic, after);
                needNotify = true;
            }

            if (needNotify) {
                log.info("Topic route changed, topic={}, before={}, after={}", topic, before, after);
            } else {
                log.debug("Topic route remains unchanged, topic={}", topic);
            }

            if (needNotify) {
                for (ProducerObserver producerObserver : producerObserverTable.values()) {
                    producerObserver.onTopicRouteChanged(topic, after);
                }
            }
        }
    }

    /**
     * Scan load assignments for all consumers.
     */
    private void scanConsumersLoadAssignments() {
        final ServiceState serviceState = state.get();
        if (ServiceState.STARTED != serviceState && ServiceState.STARTING != serviceState) {
            log.warn("Unexpected client instance state={}", serviceState);
            return;
        }
        for (ConsumerObserver consumerObserver : consumerObserverTable.values()) {
            consumerObserver.scanLoadAssignments();
        }
    }

    /**
     * Register producer observer.
     *
     * @param producerGroup group of producer, caller must ensure that it is not blank.
     * @param observer      producer observer.
     * @return result of register.
     */
    public boolean registerProducerObserver(String producerGroup, ProducerObserver observer) {
        final ProducerObserver prev = producerObserverTable.putIfAbsent(producerGroup, observer);
        if (null != prev) {
            log.warn("The producer group exists already, producerGroup={}", producerGroup);
            return false;
        }
        return true;
    }

    /**
     * Unregister producer observer.
     *
     * @param producerGroup the producer group
     */
    public void unregisterProducerObserver(String producerGroup) {
        producerObserverTable.remove(producerGroup);
    }

    /**
     * Register consumer observer.
     *
     * @param consumerGroup group of consumer, caller must ensure that it is not blank.
     * @param observer      consumer observer.
     * @return result of register.
     */
    public boolean registerConsumerObserver(String consumerGroup, ConsumerObserver observer) {
        final ConsumerObserver prev = consumerObserverTable.putIfAbsent(consumerGroup, observer);
        if (null != prev) {
            log.warn("The consumer group exists already, producerGroup={}", consumerGroup);
            return false;
        }
        return true;
    }

    /**
     * Unregister consumer observer.
     *
     * @param consumerGroup the consumer group
     */
    public void unregisterConsumerObserver(String consumerGroup) {
        consumerObserverTable.remove(consumerGroup);
    }

    private RPCClient getRPCClient(String target) {
        return getRPCClient(target, true);
    }

    /**
     * Get rpc client by remote address, would create client automatically if it does not exist.
     *
     * @param target remote address.
     * @return rpc client.
     */
    private RPCClient getRPCClient(String target, boolean needHeartbeat) {
        final MqRpcTarget rpcTarget = new MqRpcTarget(target, needHeartbeat);

        RPCClient rpcClient = clientTable.get(rpcTarget);
        if (null != rpcClient) {
            return rpcClient;
        }

        rpcClient = new RPCClientImpl(rpcTarget);
        rpcClient.setArn(clientInstanceConfig.getArn());
        rpcClient.setTenantId(tenantId);
        rpcClient.setAccessCredential(clientInstanceConfig.getAccessCredential());
        clientTable.put(rpcTarget, rpcClient);

        return rpcClient;
    }

    /**
     * Mark the remote address as isolated or not.
     *
     * @param target   remote address.
     * @param isolated is isolated or not.
     */
    public void setTargetIsolated(String target, boolean isolated) {
        final RPCClient client = clientTable.get(new MqRpcTarget(target, true));
        if (null != client) {
            client.setIsolated(isolated);
        }
    }

    public Set<String> getAvailableTargets() {
        Set<String> targetSet = new HashSet<String>();
        for (RpcTarget rpcTarget : clientTable.keySet()) {
            if (rpcTarget.isIsolated()) {
                continue;
            }
            targetSet.add(rpcTarget.getTarget());
        }
        return targetSet;
    }

    public Set<String> getIsolatedTargets() {
        Set<String> targetSet = new HashSet<String>();
        for (RpcTarget rpcTarget : clientTable.keySet()) {
            if (!rpcTarget.isIsolated()) {
                continue;
            }
            targetSet.add(rpcTarget.getTarget());
        }
        return targetSet;
    }

    public boolean isTargetIsolated(String target) {
        return getIsolatedTargets().contains(target);
    }

    SendMessageResponse send(
            String target, SendMessageRequest request, long duration, TimeUnit unit) {
        RPCClient rpcClient = this.getRPCClient(target);
        return rpcClient.sendMessage(request, duration, unit);
    }

    void sendAsync(
            String target,
            SendMessageRequest request,
            SendCallback sendCallback,
            long duration,
            TimeUnit unit) {
        final SendMessageResponseCallback callback =
                new SendMessageResponseCallback(request, state, sendCallback);

        boolean customizedExecutor = null != sendCallbackExecutor && null != sendCallbackSemaphore;
        final ThreadPoolExecutor executor =
                customizedExecutor ? sendCallbackExecutor : callbackExecutor;
        final Semaphore semaphore = customizedExecutor ? sendCallbackSemaphore : callbackSemaphore;

        try {
            semaphore.acquire();
            final ListenableFuture<SendMessageResponse> future =
                    getRPCClient(target).sendMessage(request, executor, duration, unit);
            Futures.addCallback(
                    future,
                    new FutureCallback<SendMessageResponse>() {
                        @Override
                        public void onSuccess(@Nullable SendMessageResponse response) {
                            try {
                                callback.onSuccess(response);
                            } finally {
                                semaphore.release();
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            try {
                                callback.onException(t);
                            } finally {
                                semaphore.release();
                            }
                        }
                    },
                    MoreExecutors.directExecutor());

        } catch (Throwable t) {
            try {
                callback.onException(t);
            } finally {
                semaphore.release();
            }
        }
    }

    public SendMessageResponse sendClientAPI(
            String target,
            CommunicationMode mode,
            SendMessageRequest request,
            SendCallback sendCallback,
            long duration,
            TimeUnit unit) {
        switch (mode) {
            case SYNC:
            case ONE_WAY:
                return send(target, request, duration, unit);
            case ASYNC:
            default:
                sendAsync(target, request, sendCallback, duration, unit);
                ResponseCommon common =
                        ResponseCommon.newBuilder().setStatus(Status.newBuilder().setCode(Code.OK_VALUE)).build();
                return SendMessageResponse.newBuilder().setCommon(common).build();
        }
    }

    public ListenableFuture<PopResult> receiveMessageAsync(
            final String target, final ReceiveMessageRequest request, long duration, TimeUnit unit) {
        final ListenableFuture<ReceiveMessageResponse> future =
                getRPCClient(target).receiveMessage(request, callbackExecutor, duration, unit);
        return Futures.transform(
                future,
                new Function<ReceiveMessageResponse, PopResult>() {
                    @Override
                    public PopResult apply(ReceiveMessageResponse response) {
                        return processReceiveMessageResponse(target, response);
                    }
                });
    }

    public void ackMessage(final String target, final AckMessageRequest request)
            throws MQClientException {
        final AckMessageResponse response =
                getRPCClient(target).ackMessage(request, RPC_DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        if (Code.OK != code) {
            log.error("Failed to ack message, target={}, status={}.", target, status);
            throw new MQClientException("Failed to ack message.");
        }
    }

    public void ackMessageAsync(final String target, final AckMessageRequest request) {
        final ListenableFuture<AckMessageResponse> future =
                getRPCClient(target)
                        .ackMessage(
                                request, callbackExecutor, RPC_DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Futures.addCallback(
                future,
                new FutureCallback<AckMessageResponse>() {
                    @Override
                    public void onSuccess(AckMessageResponse result) {
                        // TODO: check status here.
                        final Status status = result.getCommon().getStatus();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.warn(
                                "Failed to ack message asynchronously, target={}", target, t);
                    }
                });
    }

    public void nackMessage(final String target, final NackMessageRequest request)
            throws MQClientException {
        final NackMessageResponse response =
                getRPCClient(target)
                        .nackMessage(request, RPC_DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        final Status status = response.getCommon().getStatus();
        final int code = status.getCode();
        if (Code.OK_VALUE != code) {
            log.error("Failed to nack message, target={}, status={}.", target, status);
            throw new MQClientException("Failed to nack message.");
        }
    }

    private String selectNameServer(boolean roundRobin) throws MQClientException {
        nameServerLock.readLock().lock();
        try {
            int size = nameServerList.size();
            if (size <= 0) {
                throw new MQClientException("No name server is available");
            }
            int index = roundRobin ? nameServerIndex.getAndIncrement() : nameServerIndex.get();

            return nameServerList.get(index % size);
        } finally {
            nameServerLock.readLock().unlock();
        }
    }

    private int getNameServerNum() {
        nameServerLock.readLock().lock();
        try {
            return nameServerList.size();
        } finally {
            nameServerLock.readLock().unlock();
        }
    }

    private QueryRouteResponse queryRoute(String target, QueryRouteRequest request) {
        RPCClient rpcClient = this.getRPCClient(target, false);
        return rpcClient.queryRoute(
                request, FETCH_TOPIC_ROUTE_INFO_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    public TopicAssignmentInfo queryLoadAssignment(String target, QueryAssignmentRequest request)
            throws MQServerException {
        final RPCClient rpcClient = this.getRPCClient(target);
        QueryAssignmentResponse response = rpcClient.queryAssignment(request, RPC_DEFAULT_TIMEOUT_MILLIS,
                                                                     TimeUnit.MILLISECONDS);
        final Status status = response.getCommon().getStatus();
        final int code = status.getCode();
        if (Code.OK_VALUE != code) {
            throw new MQServerException(
                    "Failed to query load assignment from remote target, target="
                    + target + ", code=" + code);
        }
        return new TopicAssignmentInfo(response.getLoadAssignmentsList());
    }

    /**
     * Get topic route info from remote,
     *
     * @param topic the requested topic.
     * @return topic route into.
     * @throws MQClientException throw exception when failed to fetch topic route info from remote.
     *                           e.g. topic does not exist.
     */
    private TopicRouteData fetchTopicRouteData(String topic) throws MQClientException {
        int retryTimes = getNameServerNum();
        Resource topicResource = Resource.newBuilder().setArn(clientInstanceConfig.getArn()).setName(topic).build();
        boolean roundRobin = false;
        for (int time = 1; time <= retryTimes; time++) {
            final QueryRouteRequest request =
                    QueryRouteRequest.newBuilder().setTopic(topicResource).build();

            String target = selectNameServer(roundRobin);
            QueryRouteResponse response = queryRoute(target, request);
            final Status status = response.getCommon().getStatus();
            final Code code = Code.forNumber(status.getCode());
            if (Code.OK != code) {
                log.warn(
                        "Failed to fetch topic route, topic={}, time={}, responseCode={}, nameServerAddress={}",
                        topic, time, code, target);
                roundRobin = true;
                continue;
            }
            log.debug("Fetch topic route successfully, topic={}, time={}, nameServerAddress={} ", topic, time, target);

            final List<Partition> partitionsList = response.getPartitionsList();

            if (partitionsList.isEmpty()) {
                log.warn(
                        "Topic route is empty unexpectedly , topic={}, time={}, nameServerAddress={}",
                        topic, time, target);
                throw new MQClientException("Topic does not exist.");
            }
            return new TopicRouteData(partitionsList);
        }
        log.error("Failed to fetch topic route finally, topic={}", topic);
        throw new MQClientException("Failed to fetch topic route.");
    }

    /**
     * Get topic route info, would fetch topic route info from remote only when it does not exist in
     * local cache.
     *
     * @param topic the requested topic.
     * @return topic route info.
     * @throws MQClientException throw exception when failed to fetch topic route info from remote.
     *                           e.g. topic does not exist.
     */
    public TopicRouteData getTopicRouteInfo(String topic) throws MQClientException {
        TopicRouteData topicRouteData = topicRouteTable.get(topic);
        if (null != topicRouteData) {
            return topicRouteData;
        }
        topicRouteData = fetchTopicRouteData(topic);
        topicRouteTable.put(topic, topicRouteData);
        return topicRouteTable.get(topic);
    }

    public String resolveEndpoint(String topic, String brokerName) throws MQClientException {
        final TopicRouteData topicRouteInfo = getTopicRouteInfo(topic);
        final List<org.apache.rocketmq.client.route.Partition> partitions = topicRouteInfo.getPartitions();
        for (org.apache.rocketmq.client.route.Partition partition : partitions) {
            if (!partition.getBrokerName().equals(brokerName)) {
                continue;
            }
            if (MixAll.MASTER_BROKER_ID != partition.getBrokerId()) {
                continue;
            }
            return partition.getTarget();
        }
        log.error("Failed to resolve endpoint, brokerName does not exist, topic={}, brokerName={}", topic, brokerName);
        throw new MQClientException("Failed to resolve master node from brokerName.");
    }

    public static SendResult processSendResponse(SendMessageResponse response)
            throws MQServerException {
        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        if (null == code) {
            throw new MQServerException("Unrecognized code=" + status.getCode());
        }
        if (Code.OK == code) {
            return new SendResult(response.getMessageId());
        }
        log.debug("Response indicates failure of sending message, information={}", status.getMessage());
        throw new MQServerException(status.getMessage());
    }

    // TODO: handle the case that the topic does not exist.
    public static PopResult processReceiveMessageResponse(String target, ReceiveMessageResponse response) {
        PopStatus popStatus;

        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        switch (code != null ? code : Code.UNKNOWN) {
            case OK:
                popStatus = PopStatus.FOUND;
                break;
            case RESOURCE_EXHAUSTED:
                popStatus = PopStatus.POLLING_FULL;
                log.warn("Too may pop request in broker, brokerAddress={}", target);
                break;
            case DEADLINE_EXCEEDED:
                popStatus = PopStatus.NO_NEW_MSG;
                break;
            case NOT_FOUND:
                popStatus = PopStatus.POLLING_NOT_FOUND;
                break;
            default:
                popStatus = PopStatus.SERVICE_UNSTABLE;
                log.warn(
                        "Pop response indicated server-side error, brokerAddress={}, code={}, status message={}",
                        target, code, status.getMessage());
        }

        List<MessageExt> msgFoundList = new ArrayList<MessageExt>();
        if (PopStatus.FOUND == popStatus) {
            final List<Message> msgList = response.getMessagesList();
            for (Message msg : msgList) {
                try {
                    MessageImpl impl = new MessageImpl(msg.getTopic().getName());
                    final SystemAttribute systemAttribute = msg.getSystemAttribute();
                    // Target
                    impl.getSystemAttribute().setTargetEndpoint(target);
                    // Tag
                    impl.getSystemAttribute().setTag(systemAttribute.getTag());
                    // Key
                    List<String> keys = new ArrayList<String>(systemAttribute.getKeysList());
                    impl.getSystemAttribute().setKeys(keys);
                    // Message Id
                    impl.getSystemAttribute().setMessageId(systemAttribute.getMessageId());
                    // Check digest.
                    final Digest bodyDigest = systemAttribute.getBodyDigest();
                    byte[] body = msg.getBody().toByteArray();
                    boolean bodyDigestMatch = false;
                    String expectedCheckSum;
                    switch (bodyDigest.getType()) {
                        case CRC32:
                            expectedCheckSum = UtilAll.getCrc32CheckSum(body);
                            if (expectedCheckSum.equals(bodyDigest.getChecksum())) {
                                bodyDigestMatch = true;
                            }
                            break;
                        case MD5:
                            try {
                                expectedCheckSum = UtilAll.getMd5CheckSum(body);
                                if (expectedCheckSum.equals(bodyDigest.getChecksum())) {
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
                                if (expectedCheckSum.equals(bodyDigest.getChecksum())) {
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
                        log.warn("Message body checksum failed.");
                        // Need NACK immediately ?
                        continue;
                    }

                    switch (systemAttribute.getBodyEncoding()) {
                        case GZIP:
                            body = UtilAll.uncompressByteArray(body);
                            break;
                        case SNAPPY:
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

                    TransactionPhase transactionPhase;
                    switch (systemAttribute.getTransactionPhase()) {
                        case NOT_APPLICABLE:
                            transactionPhase = TransactionPhase.NOT_APPLICABLE;
                            break;
                        case PREPARE:
                            transactionPhase = TransactionPhase.PREPARE;
                            break;
                        case COMMIT:
                            transactionPhase = TransactionPhase.COMMIT;
                            break;
                        case ROLLBACK:
                            transactionPhase = TransactionPhase.ROLLBACK;
                            break;
                        default:
                            transactionPhase = TransactionPhase.NOT_APPLICABLE;
                            log.warn("Unknown transaction phase, fall through to N/A");
                    }
                    // TransactionPhase
                    impl.getSystemAttribute().setTransactionPhase(transactionPhase);

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
                    // TraceContext
                    impl.getSystemAttribute().setTraceContext(systemAttribute.getTraceContext());
                    // UserProperties
                    impl.getUserAttribute().putAll(msg.getUserAttributeMap());

                    MessageExt messageExt = new MessageExt(impl);
                    msgFoundList.add(messageExt);
                } catch (Throwable t) {
                    log.error("Failed to parse messageExt from protocol buffer, msgId={}",
                              msg.getSystemAttribute().getMessageId());
                }
            }
        }

        return new PopResult(
                target,
                popStatus,
                Timestamps.toMillis(response.getDeliveryTimestamp()),
                Durations.toMillis(response.getInvisibleDuration()),
                msgFoundList);
    }

    static class MqRpcTarget extends RpcTarget {
        private final boolean needHeartbeat;

        public MqRpcTarget(String target, boolean needHeartbeat) {
            super(target);
            this.needHeartbeat = needHeartbeat;
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }
}
