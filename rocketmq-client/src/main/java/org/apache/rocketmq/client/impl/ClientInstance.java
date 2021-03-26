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
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.CommunicationMode;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.consumer.ConsumerObserver;
import org.apache.rocketmq.client.impl.producer.ProducerObserver;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.remoting.InvocationContext;
import org.apache.rocketmq.client.remoting.RPCClient;
import org.apache.rocketmq.client.remoting.RPCClientImpl;
import org.apache.rocketmq.client.remoting.RPCTarget;
import org.apache.rocketmq.client.route.BrokerData;
import org.apache.rocketmq.proto.HealthCheckRequest;
import org.apache.rocketmq.proto.HealthCheckResponse;
import org.apache.rocketmq.proto.HeartbeatRequest;
import org.apache.rocketmq.proto.HeartbeatResponse;
import org.apache.rocketmq.proto.ResponseCode;
import org.apache.rocketmq.proto.RouteInfoRequest;
import org.apache.rocketmq.proto.RouteInfoResponse;
import org.apache.rocketmq.proto.SendMessageRequest;
import org.apache.rocketmq.proto.SendMessageResponse;
import org.apache.rocketmq.proto.TopicRouteData;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.apache.rocketmq.utility.UtilAll;

@Slf4j
public class ClientInstance {
  private static final int MAX_ASYNC_QUEUE_TASK_NUM = 1000;
  private static final AtomicInteger nameServerIndex = new AtomicInteger(0);

  private final ScheduledExecutorService scheduler =
      new ScheduledThreadPoolExecutor(2, new ThreadFactoryImpl("ClientInstanceScheduler_"));

  private final ClientConfig clientConfig;
  private final ConcurrentMap<MQRPCTarget, RPCClient> clientTable;
  private final ThreadPoolExecutor asyncExecutor;

  private final List<String> nameServerList;
  private final ReadWriteLock nameServerLock;

  private final ConcurrentMap<String, ProducerObserver> producerObserverTable;
  private final ConcurrentMap<String, ConsumerObserver> consumerObserverTable;

  private final ConcurrentHashMap<
          String /* Topic */, org.apache.rocketmq.client.route.TopicRouteData>
      topicRouteTable;

  private final String clientId;
  private final AtomicReference<ServiceState> state;

  public ClientInstance(ClientConfig clientConfig, String clientId) {
    this.clientConfig = clientConfig;
    this.clientTable = new ConcurrentHashMap<>();
    this.asyncExecutor =
        new ThreadPoolExecutor(
            1,
            Runtime.getRuntime().availableProcessors(),
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(MAX_ASYNC_QUEUE_TASK_NUM));

    this.nameServerList = clientConfig.getNameServerList();
    this.nameServerLock = new ReentrantReadWriteLock();
    this.topicRouteTable = new ConcurrentHashMap<>();

    this.producerObserverTable = new ConcurrentHashMap<>();
    this.consumerObserverTable = new ConcurrentHashMap<>();

    this.clientId = clientId;
    this.state = new AtomicReference<>(ServiceState.CREATED);
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
        () -> {
          try {
            updateRouteInfo();
          } catch (Throwable t) {
            log.error("Exception occurs while scheduling update route info.", t);
          }
        },
        10 * 1000,
        clientConfig.getRouteUpdatePeriodMillis(),
        TimeUnit.MILLISECONDS);

    scheduler.scheduleWithFixedDelay(
        () -> {
          try {
            restoreIsolatedTarget();
          } catch (Throwable t) {
            log.error("Exception occurs while scheduling health check.", t);
          }
        },
        5 * 1000,
        15 * 1000,
        TimeUnit.MILLISECONDS);

    scheduler.scheduleWithFixedDelay(
        () -> {
          try {
            cleanOutdatedClient();
          } catch (Throwable t) {
            log.error("Exception occurs while clean outdated client.", t);
          }
        },
        30 * 1000,
        60 * 1000,
        TimeUnit.MILLISECONDS);

    scheduler.scheduleWithFixedDelay(
        () -> {
          try {
            doHeartbeat();
          } catch (Throwable t) {
            log.error("Exception occurs while heartbeat.", t);
          }
        },
        0,
        clientConfig.getHeartbeatPeriodMillis(),
        TimeUnit.MILLISECONDS);

    scheduler.scheduleWithFixedDelay(
        () -> {
          try {
            logStats();
          } catch (Throwable t) {
            log.error("Exception occurs while stats", t);
          }
        },
        10 * 1000,
        60 * 1000,
        TimeUnit.MILLISECONDS);

    state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
  }

  private void logStats() {}

  private void doHeartbeat() {
    log.debug("Start to send heartbeat for a new round.");

    final HeartbeatRequest.Builder builder = HeartbeatRequest.newBuilder();
    builder.setClientId(clientId);
    builder.setLanguageCode(HeartbeatRequest.LanguageCode.JAVA);

    producerObserverTable
        .values()
        .forEach(
            producerObserver ->
                builder.addProducerDataSet(producerObserver.prepareHeartbeatData()));

    consumerObserverTable
        .values()
        .forEach(
            consumerObserver -> builder.addConsumeDataSet(consumerObserver.prepareHeartbeatData()));

    final HeartbeatRequest request = builder.build();

    Set<MQRPCTarget> filteredTarget = new HashSet<>();
    clientTable.forEach(
        (rpcTarget, rpcClient) -> {
          if (!rpcTarget.needHeartbeat) {
            return;
          }
          filteredTarget.add(rpcTarget);
        });
    filteredTarget.forEach(
        rpcTarget -> {
          final RPCClient rpcClient = clientTable.get(rpcTarget);
          if (null == rpcClient) {
            return;
          }
          final HeartbeatResponse response = rpcClient.heartbeat(request);
          final ResponseCode code = response.getCode();
          final String target = rpcTarget.getTarget();
          if (ResponseCode.SUCCESS != code) {
            log.warn(
                "Failed to send heartbeat to target, responseCode={}, target={}", code, target);
          } else {
            log.debug("Send heartbeat to target successfully, target={}", target);
          }
        });
  }

  private void restoreIsolatedTarget() {
    clientTable.forEach(
        (RPCTarget, rpcClient) -> {
          if (!rpcClient.isIsolated()) {
            return;
          }
          final String target = RPCTarget.getTarget();
          HealthCheckRequest request =
              HealthCheckRequest.newBuilder().setClientHost(target).build();
          final HealthCheckResponse response = rpcClient.healthCheck(request);
          if (ResponseCode.SUCCESS == response.getCode()) {
            RPCTarget.setIsolated(false);
            log.info("Isolated target[{}] has been restored", target);
          } else {
            log.debug("Isolated target[{}] was not restored", target);
          }
        });
  }

  private void cleanOutdatedClient() {
    Set<String> currentTargets = new HashSet<>();
    nameServerLock.readLock().lock();
    try {
      currentTargets.addAll(nameServerList);
    } finally {
      nameServerLock.readLock().unlock();
    }
    topicRouteTable.forEach(
        (topic, topicRouteData) -> {
          final List<BrokerData> brokerDataList = topicRouteData.getBrokerDataList();
          for (BrokerData brokerData : brokerDataList) {
            final HashMap<Long, String> brokerAddressTable = brokerData.getBrokerAddressTable();
            brokerAddressTable
                .values()
                .forEach(
                    target ->
                        currentTargets.add(UtilAll.shiftTargetPort(target, MixAll.SHIFT_PORT)));
          }
        });
    clientTable
        .keySet()
        .forEach(
            RPCTarget -> {
              if (!currentTargets.contains(RPCTarget.getTarget())) {
                clientTable.remove(RPCTarget);
              }
            });
  }

  public void shutdown() {}

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

  private void updateRouteInfo() {
    final Set<String> topics = new HashSet<>(topicRouteTable.keySet());
    if (topics.isEmpty()) {
      return;
    }
    for (String topic : topics) {
      boolean needNotify = false;
      org.apache.rocketmq.client.route.TopicRouteData after;

      try {
        after = fetchTopicRouteData(topic);
      } catch (Throwable t) {
        log.warn("Failed to fetch topic route from name server, topic={}", topic);
        continue;
      }

      final org.apache.rocketmq.client.route.TopicRouteData before = topicRouteTable.get(topic);
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
        producerObserverTable.forEach(
            (producerGroup, observer) -> observer.onTopicRouteChanged(topic, after));
      }
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

    rpcClient = new RPCClientImpl(rpcTarget, asyncExecutor);
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
    Set<String> targetSet = new HashSet<>();
    for (RPCTarget RPCTarget : clientTable.keySet()) {
      if (RPCTarget.isIsolated()) {
        continue;
      }
      targetSet.add(RPCTarget.getTarget());
    }
    return targetSet;
  }

  public Set<String> getIsolatedTargets() {
    Set<String> targetSet = new HashSet<>();
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
    BiConsumer<SendMessageResponse, Throwable> biConsumer =
        ((response, throwable) -> {
          ServiceState state = this.state.get();
          String messageId = response.getMessageId();

          if (ServiceState.STARTED != state) {
            log.info("Client instance has stopped, state={}, msgId={}", state, messageId);
          }

          if (null != throwable) {
            sendCallback.onException(throwable);
            return;
          }

          MessageQueue messageQueue = new MessageQueue();
          messageQueue.setQueueId(response.getQueueId());
          messageQueue.setTopic(request.getMessage().getTopic());
          messageQueue.setBrokerName(request.getBrokerName());
          SendResult sendResult;
          try {
            sendResult = processSendResponse(messageQueue, response);
          } catch (MQServerException e) {
            sendCallback.onException(e);
            return;
          }
          sendCallback.onSuccess(sendResult);
        });
    InvocationContext<SendMessageResponse> context = new InvocationContext<>(biConsumer);
    getRPCClient(target).sendMessage(request, context, duration, unit);
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

  /**
   * Get topic route info from remote,
   *
   * @param topic the requested topic.
   * @return topic route into.
   * @throws MQClientException throw exception when failed to fetch topic route info from remote.
   *     e.g. topic does not exist.
   */
  private org.apache.rocketmq.client.route.TopicRouteData fetchTopicRouteData(String topic)
      throws MQClientException {
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
      Map<String, TopicRouteData> routeEntries = response.getRouteMap();
      TopicRouteData topicRouteData = routeEntries.get(topic);
      if (null == topicRouteData) {
        throw new MQClientException("Topic does not exist.");
      }
      return new org.apache.rocketmq.client.route.TopicRouteData(topicRouteData);
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
  public org.apache.rocketmq.client.route.TopicRouteData getTopicRouteInfo(String topic)
      throws MQClientException {
    org.apache.rocketmq.client.route.TopicRouteData topicRouteData = topicRouteTable.get(topic);
    if (null != topicRouteData) {
      return topicRouteData;
    }
    topicRouteData = fetchTopicRouteData(topic);
    topicRouteTable.put(topic, topicRouteData);
    return topicRouteTable.get(topic);
  }

  public SendResult processSendResponse(MessageQueue mq, SendMessageResponse response)
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
