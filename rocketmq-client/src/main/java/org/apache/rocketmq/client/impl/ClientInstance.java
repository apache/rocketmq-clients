package org.apache.rocketmq.client.impl;

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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.CommunicationMode;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.consumer.ConsumerObserver;
import org.apache.rocketmq.client.impl.consumer.TopicAssignmentInfo;
import org.apache.rocketmq.client.impl.producer.ProducerObserver;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.remoting.RPCClient;
import org.apache.rocketmq.client.remoting.RPCClientImpl;
import org.apache.rocketmq.client.remoting.RPCTarget;
import org.apache.rocketmq.client.remoting.SendMessageResponseCallback;
import org.apache.rocketmq.client.route.BrokerData;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.proto.*;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.apache.rocketmq.utility.UtilAll;

@Slf4j
public class ClientInstance {
  private static final int MAX_ASYNC_QUEUE_TASK_NUM = 1024;
  private static final AtomicInteger nameServerIndex = new AtomicInteger(0);

  @Getter
  private final ScheduledExecutorService scheduler =
      new ScheduledThreadPoolExecutor(2, new ThreadFactoryImpl("ClientInstanceScheduler_"));

  private final ClientConfig clientConfig;
  private final ConcurrentMap<MQRPCTarget, RPCClient> clientTable;

  private final ThreadPoolExecutor callbackExecutor;

  private final List<String> nameServerList;
  private final ReadWriteLock nameServerLock;

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

    this.nameServerList = clientConfig.getNameServerList();
    this.nameServerLock = new ReentrantReadWriteLock();
    this.topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();

    this.producerObserverTable = new ConcurrentHashMap<String, ProducerObserver>();
    this.consumerObserverTable = new ConcurrentHashMap<String, ConsumerObserver>();

    this.clientId = clientId;
    this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);
  }

  public void start() throws MQClientException {
    if (!state.compareAndSet(ServiceState.CREATED, ServiceState.STARTING)) {
      throw new MQClientException(
          "The client instance has attempted to be stared before, state=" + state.get());
    }

    if (nameServerListIsEmpty()) {
      //
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
        0,
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
        10 * 1000,
        60 * 1000,
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
      final HeartbeatResponse response = rpcClient.heartbeat(request);
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
      final HealthCheckResponse response = rpcClient.healthCheck(request);
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

  public void setNameServerList(List<String> nameServerList) {
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

    rpcClient = new RPCClientImpl(rpcTarget, callbackExecutor);
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
    final RPCClient client = clientTable.get(new RPCTarget(target));
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
    getRPCClient(target).sendMessage(request, callback, duration, unit);
  }

  public SendMessageResponse sendClientAPI(
      String target,
      CommunicationMode mode,
      SendMessageRequest request,
      SendCallback sendCallback) {
    return this.sendClientAPI(target, mode, request, sendCallback, 3, TimeUnit.SECONDS);
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
    return rpcClient.fetchTopicRouteInfo(request);
  }

  public TopicAssignmentInfo queryLoadAssignment(String target, QueryAssignmentRequest request)
      throws MQServerException {
    final RPCClient rpcClient = this.getRPCClient(target);
    final QueryAssignmentResponse response = rpcClient.queryAssignment(request);
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
        throw new MQServerException(code, "Unknown server error");
    }
    SendResult sendResult = new SendResult();
    sendResult.setSendStatus(sendStatus);
    sendResult.setMsgId(response.getMessageId());
    sendResult.setQueueOffset(response.getQueueOffset());
    sendResult.setMessageQueue(mq);
    sendResult.setTransactionId(response.getTransactionId());
    return sendResult;
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
