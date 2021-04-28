package org.apache.rocketmq.client.impl;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
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
import org.apache.rocketmq.client.message.MessageConst;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.MessageSystemFlag;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.misc.TopAddressing;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.remoting.RPCClient;
import org.apache.rocketmq.client.remoting.RPCClientImpl;
import org.apache.rocketmq.client.remoting.RPCTarget;
import org.apache.rocketmq.client.remoting.SendMessageResponseCallback;
import org.apache.rocketmq.client.route.BrokerData;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.proto.AckMessageRequest;
import org.apache.rocketmq.proto.AckMessageResponse;
import org.apache.rocketmq.proto.ChangeInvisibleTimeRequest;
import org.apache.rocketmq.proto.ChangeInvisibleTimeResponse;
import org.apache.rocketmq.proto.HealthCheckRequest;
import org.apache.rocketmq.proto.HealthCheckResponse;
import org.apache.rocketmq.proto.HeartbeatRequest;
import org.apache.rocketmq.proto.HeartbeatResponse;
import org.apache.rocketmq.proto.Message;
import org.apache.rocketmq.proto.PopMessageRequest;
import org.apache.rocketmq.proto.PopMessageResponse;
import org.apache.rocketmq.proto.QueryAssignmentRequest;
import org.apache.rocketmq.proto.QueryAssignmentResponse;
import org.apache.rocketmq.proto.ResponseCode;
import org.apache.rocketmq.proto.RouteInfoRequest;
import org.apache.rocketmq.proto.RouteInfoResponse;
import org.apache.rocketmq.proto.SendMessageRequest;
import org.apache.rocketmq.proto.SendMessageResponse;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.apache.rocketmq.utility.UtilAll;

@Slf4j
public class ClientInstance {
  private static final int MAX_ASYNC_QUEUE_TASK_NUM = 1024;
  private static final AtomicInteger nameServerIndex = new AtomicInteger(0);

  private static final long RPC_DEFAULT_TIMEOUT_MILLIS = 3 * 1000;
  /**
   * Usage of {@link RPCClientImpl#fetchTopicRouteInfo(RouteInfoRequest, long, TimeUnit)} are
   * usually invokes the first call of gRPC, which need warm up in most case.
   */
  private static final long FETCH_TOPIC_ROUTE_INFO_TIMEOUT_MILLIS = 15 * 1000;

  @Getter
  private final ScheduledExecutorService scheduler =
      new ScheduledThreadPoolExecutor(4, new ThreadFactoryImpl("ClientInstanceScheduler_"));

  private final ClientConfig clientConfig;
  private final ConcurrentMap<MQRPCTarget, RPCClient> clientTable;

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

  @Getter private final String clientId;
  private final AtomicReference<ServiceState> state;

  public ClientInstance(ClientConfig clientConfig, String clientId) {
    this.clientConfig = clientConfig;
    this.clientTable = new ConcurrentHashMap<MQRPCTarget, RPCClient>();
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

    this.nameServerList = clientConfig.getNameServerList();
    this.nameServerLock = new ReentrantReadWriteLock();

    this.topAddressing = new TopAddressing();

    this.producerObserverTable = new ConcurrentHashMap<String, ProducerObserver>();
    this.consumerObserverTable = new ConcurrentHashMap<String, ConsumerObserver>();

    this.topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();

    this.clientId = clientId;
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

  public void start() throws MQClientException {
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
        clientConfig.getRouteUpdatePeriodMillis(),
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
        clientConfig.getHeartbeatPeriodMillis(),
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

  public void shutdown() {
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
      state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED);
      return;
    }
    log.warn("Failed to shutdown client instance, unexpected state={}.", serviceState);
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
    builder.setClientId(clientId);
    builder.setLanguageCode(HeartbeatRequest.LanguageCode.JAVA);

    for (ProducerObserver producerObserver : producerObserverTable.values()) {
      builder.addProducerDataSet(producerObserver.prepareHeartbeatData());
    }

    for (ConsumerObserver consumerObserver : consumerObserverTable.values()) {
      builder.addConsumeDataSet(consumerObserver.prepareHeartbeatData());
    }

    final HeartbeatRequest request = builder.build();

    Set<MQRPCTarget> filteredTarget = new HashSet<MQRPCTarget>();
    for (MQRPCTarget rpcTarget : clientTable.keySet()) {
      if (!rpcTarget.needHeartbeat) {
        return;
      }
      filteredTarget.add(rpcTarget);
    }

    for (MQRPCTarget rpcTarget : filteredTarget) {
      final RPCClient rpcClient = clientTable.get(rpcTarget);
      if (null == rpcClient) {
        continue;
      }
      final HeartbeatResponse response =
          rpcClient.heartbeat(request, RPC_DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      final ResponseCode code = response.getCode();
      final String target = rpcTarget.getTarget();
      if (ResponseCode.SUCCESS != code) {
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

    for (Map.Entry<MQRPCTarget, RPCClient> entry : clientTable.entrySet()) {
      final MQRPCTarget rpcTarget = entry.getKey();
      final RPCClient rpcClient = entry.getValue();
      if (!rpcClient.isIsolated()) {
        continue;
      }
      final String target = rpcTarget.getTarget();
      final HealthCheckRequest request =
          HealthCheckRequest.newBuilder().setClientHost(target).build();
      final HealthCheckResponse response =
          rpcClient.healthCheck(request, RPC_DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      if (ResponseCode.SUCCESS == response.getCode()) {
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

    Set<String> currentTargets = new HashSet<String>();
    nameServerLock.readLock().lock();
    try {
      currentTargets.addAll(nameServerList);
    } finally {
      nameServerLock.readLock().unlock();
    }
    for (TopicRouteData topicRouteData : topicRouteTable.values()) {
      final List<BrokerData> brokerDataList = topicRouteData.getBrokerDataList();
      for (BrokerData brokerData : brokerDataList) {
        final HashMap<Long, String> brokerAddressTable = brokerData.getBrokerAddressTable();
        for (String target : brokerAddressTable.values()) {
          currentTargets.add(UtilAll.shiftTargetPort(target, MixAll.SHIFT_PORT));
        }
      }
    }

    for (MQRPCTarget rpcTarget : clientTable.keySet()) {
      if (!currentTargets.contains(rpcTarget.getTarget())) {
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

  /** Update topic route info from name server and notify observer if changed. */
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

  /** Scan load assignments for all consumers. */
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
   * @param observer producer observer.
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
   * @param observer consumer observer.
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
    final MQRPCTarget rpcTarget = new MQRPCTarget(target, needHeartbeat);
    RPCClient rpcClient = clientTable.get(rpcTarget);
    if (null != rpcClient) {
      return rpcClient;
    }

    rpcClient = new RPCClientImpl(rpcTarget);
    clientTable.put(rpcTarget, rpcClient);
    return rpcClient;
  }

  /**
   * Mark the remote address as isolated or not.
   *
   * @param target remote address.
   * @param isolated is isolated or not.
   */
  public void setTargetIsolated(String target, boolean isolated) {
    final RPCClient client = clientTable.get(new MQRPCTarget(target, true));
    if (null != client) {
      client.setIsolated(isolated);
    }
  }

  public Set<String> getAvailableTargets() {
    Set<String> targetSet = new HashSet<String>();
    for (RPCTarget RPCTarget : clientTable.keySet()) {
      if (RPCTarget.isIsolated()) {
        continue;
      }
      targetSet.add(RPCTarget.getTarget());
    }
    return targetSet;
  }

  public Set<String> getIsolatedTargets() {
    Set<String> targetSet = new HashSet<String>();
    for (RPCTarget RPCTarget : clientTable.keySet()) {
      if (!RPCTarget.isIsolated()) {
        continue;
      }
      targetSet.add(RPCTarget.getTarget());
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
        return SendMessageResponse.newBuilder().setCode(ResponseCode.SUCCESS).build();
    }
  }

  public ListenableFuture<PopResult> popMessageAsync(
      final String target, final PopMessageRequest request, long duration, TimeUnit unit) {
    final ListenableFuture<PopMessageResponse> future =
        getRPCClient(target).popMessage(request, callbackExecutor, duration, unit);
    return Futures.transform(
        future,
        new Function<PopMessageResponse, PopResult>() {
          @Override
          public PopResult apply(PopMessageResponse response) {
            return processPopResponse(target, response);
          }
        });
  }

  public void ackMessage(final String target, final AckMessageRequest request)
      throws MQClientException {
    final AckMessageResponse response =
        getRPCClient(target).ackMessage(request, RPC_DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    final ResponseCode code = response.getCode();
    if (ResponseCode.SUCCESS != code) {
      log.error("Failed to ack message, target={}, request={}.", target, request);
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
          public void onSuccess(@Nullable AckMessageResponse result) {}

          @Override
          public void onFailure(Throwable t) {
            log.warn(
                "Failed to ack message asynchronously,target={}, request={}", target, request, t);
          }
        });
  }

  public void changeInvisibleTime(final String target, final ChangeInvisibleTimeRequest request)
      throws MQClientException {
    final ChangeInvisibleTimeResponse response =
        getRPCClient(target)
            .changeInvisibleTime(request, RPC_DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    final ResponseCode code = response.getCode();
    if (ResponseCode.SUCCESS != code) {
      log.error("Failed to change invisible time, target={}, request={}.", target, request);
      throw new MQClientException("Failed to change invisible time.");
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

      final String target = nameServerList.get(index % size);
      return UtilAll.shiftTargetPort(target, MixAll.SHIFT_PORT);
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

  private RouteInfoResponse fetchTopicRouteInfo(String target, RouteInfoRequest request) {
    RPCClient rpcClient = this.getRPCClient(target, false);
    return rpcClient.fetchTopicRouteInfo(
        request, FETCH_TOPIC_ROUTE_INFO_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  public TopicAssignmentInfo queryLoadAssignment(String target, QueryAssignmentRequest request)
      throws MQServerException {
    final RPCClient rpcClient = this.getRPCClient(target);
    final QueryAssignmentResponse response =
        rpcClient.queryAssignment(request, RPC_DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    final ResponseCode code = response.getCode();
    if (ResponseCode.SUCCESS != code) {
      throw new MQServerException(
          "Failed to query load assignment from remote target, target="
              + target
              + ", code="
              + code);
    }
    return new TopicAssignmentInfo(response.getMessageQueueAssignmentsList());
  }

  /**
   * Get topic route info from remote,
   *
   * @param topic the requested topic.
   * @return topic route into.
   * @throws MQClientException throw exception when failed to fetch topic route info from remote.
   *     e.g. topic does not exist.
   */
  private TopicRouteData fetchTopicRouteData(String topic) throws MQClientException {
    int retryTimes = getNameServerNum();
    RouteInfoRequest request = RouteInfoRequest.newBuilder().addTopic(topic).build();
    boolean roundRobin = false;
    for (int i = 0; i < retryTimes; i++) {
      String target = selectNameServer(roundRobin);
      RouteInfoResponse response = fetchTopicRouteInfo(target, request);
      if (ResponseCode.SUCCESS != response.getCode()) {
        roundRobin = true;
        continue;
      }
      Map<String, org.apache.rocketmq.proto.TopicRouteData> routeEntries = response.getRouteMap();
      org.apache.rocketmq.proto.TopicRouteData topicRouteData = routeEntries.get(topic);
      if (null == topicRouteData) {
        throw new MQClientException("Topic does not exist.");
      }
      return new TopicRouteData(topicRouteData);
    }
    throw new MQClientException("Failed to fetch topic route.");
  }

  /**
   * Get topic route info, would fetch topic route info from remote only when it does not exist in
   * local cache.
   *
   * @param topic the requested topic.
   * @return topic route info.
   * @throws MQClientException throw exception when failed to fetch topic route info from remote.
   *     e.g. topic does not exist.
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

  public String resolveTarget(String topic, String brokerName) throws MQClientException {
    final TopicRouteData topicRouteInfo = getTopicRouteInfo(topic);
    final List<BrokerData> brokerDataList = topicRouteInfo.getBrokerDataList();
    for (BrokerData brokerData : brokerDataList) {
      if (!brokerData.getBrokerName().equals(brokerName)) {
        continue;
      }
      final HashMap<Long, String> brokerAddressTable = brokerData.getBrokerAddressTable();
      String target = brokerAddressTable.get(MixAll.MASTER_BROKER_ID);
      if (null == target) {
        log.error(
            "Failed to resolve target address for master node, topic={}, brokerName={}",
            topic,
            brokerName);
        throw new MQClientException("Failed to resolve master node.");
      }
      target = UtilAll.shiftTargetPort(target, MixAll.SHIFT_PORT);
      return target;
    }
    log.error(
        "Failed to resolve target address, brokerName does not exist, topic={}, brokerName={}",
        topic,
        brokerName);
    throw new MQClientException("Failed to resolve master node from brokerName.");
  }

  public static SendResult processSendResponse(MessageQueue mq, SendMessageResponse response)
      throws MQServerException {
    SendStatus sendStatus;
    final ResponseCode code = response.getCode();
    switch (code) {
      case SUCCESS:
        sendStatus = SendStatus.SEND_OK;
        break;
      case FLUSH_DISK_TIMEOUT:
        sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
        break;
      case FLUSH_SLAVE_TIMEOUT:
        sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
        break;
      case SLAVE_NOT_AVAILABLE:
        sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
        break;
      default:
        throw new MQServerException(code, "Unknown server error while sending message.");
    }
    return new SendResult(
        sendStatus,
        response.getMessageId(),
        mq,
        response.getQueueOffset(),
        response.getTransactionId());
  }

  // TODO: handle the case that the topic does not exist.
  public static PopResult processPopResponse(String target, PopMessageResponse response) {
    PopStatus popStatus;
    final ResponseCode code = response.getCode();
    switch (code) {
      case SUCCESS:
        popStatus = PopStatus.FOUND;
        break;
      case POLLING_FULL:
        popStatus = PopStatus.POLLING_FULL;
        break;
      case POLLING_TIMEOUT:
        popStatus = PopStatus.NO_NEW_MSG;
        break;
      case PULL_NOT_FOUND:
        popStatus = PopStatus.POLLING_NOT_FOUND;
        break;
      default:
        popStatus = PopStatus.SERVICE_UNSTABLE;
        log.warn(
            "Pop response indicated server-side error, brokerAddress={}, code={}, remark={}",
            target,
            response.getCode(),
            response.getRemark());
    }

    List<MessageExt> messageList = new ArrayList<MessageExt>();
    if (PopStatus.FOUND == popStatus) {
      final List<org.apache.rocketmq.proto.MessageExt> msgList = response.getMessagesList();
      for (org.apache.rocketmq.proto.MessageExt msg : msgList) {
        try {
          MessageExt message = new MessageExt();
          final Message base = msg.getBase();

          message.setTopic(base.getTopic());
          message.setFlag(base.getFlag());

          byte[] body = base.getBody().toByteArray();
          final org.apache.rocketmq.proto.MessageExt.Extension extension = msg.getExtension();
          int systemFlag = extension.getSysFlag();
          if (MessageSystemFlag.isBodyCompressed(systemFlag)) {
            body = UtilAll.uncompressByteArray(body);
            systemFlag = MessageSystemFlag.clearBodyCompressedFlag(systemFlag);
          }
          message.setBody(body);
          message.setSysFlag(systemFlag);

          for (Map.Entry<String, String> entry : base.getPropertiesMap().entrySet()) {
            message.putProperty(entry.getKey(), entry.getValue());
          }
          message.putProperty(MessageConst.PROPERTY_ACK_HOST_ADDRESS, target);

          message.setQueueOffset(extension.getQueueOffset());
          message.setCommitLogOffset(extension.getCommitLogOffset());
          message.setBornTimestamp(extension.getBornTimestamp());
          message.setStoreTimestamp(extension.getStoreTimestamp());
          message.setPreparedTransactionOffset(extension.getPreparedTransactionOffset());
          message.setQueueId(extension.getQueueId());
          message.setStoreSize(extension.getStoreSize());
          message.setBodyCRC(extension.getBodyCrc());
          message.setReconsumeTimes(extension.getReconsumeTimes());

          message.setDecodedTimestamp(System.currentTimeMillis());

          final long invisibleTime = response.getInvisibleTime();
          message.setExpiredTimestamp(System.currentTimeMillis() + invisibleTime);
          message.setBornHost(UtilAll.host2SocketAddress(extension.getBornHost()));
          message.setStoreHost(UtilAll.host2SocketAddress(extension.getStoreHost()));
          message.setMsgId(extension.getMessageId());

          messageList.add(message);
        } catch (Throwable t) {
          log.error(
              "Failed to parse message from protocol buffer, msgId={}",
              msg.getExtension().getMessageId(),
              t);
        }
      }
    }

    return new PopResult(
        target,
        popStatus,
        response.getTermId(),
        response.getPopTime(),
        response.getInvisibleTime(),
        response.getRestNumber(),
        messageList);
  }

  static class MQRPCTarget extends RPCTarget {
    private final boolean needHeartbeat;

    public MQRPCTarget(String target, boolean needHeartbeat) {
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
