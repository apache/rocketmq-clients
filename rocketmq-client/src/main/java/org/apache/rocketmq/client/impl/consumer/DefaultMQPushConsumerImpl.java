package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.LoadBalanceStrategy;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.impl.ClientManager;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.BrokerData;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.proto.ConsumeData;
import org.apache.rocketmq.proto.MessageModel;
import org.apache.rocketmq.proto.QueryAssignmentRequest;
import org.apache.rocketmq.utility.UtilAll;

@Slf4j
public class DefaultMQPushConsumerImpl implements ConsumerObserver {
  private final DefaultMQPushConsumer defaultMQPushConsumer;

  private final ConcurrentMap<String /* topic */, FilterExpression> filterExpressionTable;
  private final ConcurrentMap<String /* topic */, TopicAssignmentInfo> lastTopicAssignmentTable;

  private MessageListenerConcurrently messageListenerConcurrently;
  private MessageListenerOrderly messageListenerOrderly;

  private ClientInstance clientInstance;
  private ConsumeService consumeService;
  private final AtomicReference<ServiceState> state;

  public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer) {
    this.defaultMQPushConsumer = defaultMQPushConsumer;
    this.filterExpressionTable = new ConcurrentHashMap<String, FilterExpression>();
    this.lastTopicAssignmentTable = new ConcurrentHashMap<String, TopicAssignmentInfo>();
    this.messageListenerConcurrently = null;
    this.messageListenerOrderly = null;
    this.consumeService = null;
    this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);
  }

  private ConsumeService generateConsumeService() {
    if (null != messageListenerConcurrently) {
      return new ConsumeConcurrentlyService(this, messageListenerConcurrently);
    }
    if (null != messageListenerOrderly) {
      return new ConsumeOrderlyService(this, messageListenerOrderly);
    }
    return null;
  }

  public void start() throws MQClientException {
    final String consumerGroup = defaultMQPushConsumer.getGroupName();

    if (!state.compareAndSet(ServiceState.CREATED, ServiceState.STARTING)) {
      throw new MQClientException(
          "The producer has attempted to be started before, consumerGroup=" + consumerGroup);
    }

    clientInstance = ClientManager.getClientInstance(defaultMQPushConsumer);

    consumeService = this.generateConsumeService();
    if (null != consumeService) {
      consumeService.start();
    }

    final boolean registerResult = clientInstance.registerConsumerObserver(consumerGroup, this);
    if (!registerResult) {
      throw new MQClientException(
          "The consumer group has been created already, please specify another one, consumerGroup="
              + consumerGroup);
    } else {
      log.debug("Registered consumer observer, consumerGroup={}", consumerGroup);
    }

    clientInstance.start();
    //    clientInstance.getScheduler().submit(this::scanLoadAssignments);

    state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
  }

  public void shutdown() {}

  @Override
  public void scanLoadAssignments() {
    try {
      final ServiceState serviceState = state.get();
      if (ServiceState.STARTED != serviceState && ServiceState.STARTING != serviceState) {
        log.warn(
            "Unexpected consumer state while scanning load assignments, state={}", serviceState);
        return;
      }
      log.debug("Start to scan load assignments periodically");
      //      filterExpressionTable.forEach(
      //          (topic, FilterExpression) -> {
      //            try {
      //              //            final TopicAssignmentInfo topicAssignmentInfo =
      //              // getTopicAssignmentInfo(topic);
      //              final TopicAssignmentInfo lastTopicAssignmentInfo =
      //                  lastTopicAssignmentTable.get(topic);
      //              final TopicAssignmentInfo currentTopicAssignmentInfo =
      // queryLoadAssignment(topic);
      //              if (null == lastTopicAssignmentInfo) {}
      //
      //            } catch (Throwable t) {
      //
      //            }
      //          });

    } catch (Throwable t) {

    }
  }

  private void syncProcessQueue(
      String topic, TopicAssignmentInfo topicAssignmentInfo, FilterExpression filterExpression) {}

  //  /**
  //   * Get {@link TopicAssignmentInfo} from remote if no local cache.
  //   *
  //   * @param topic
  //   * @return
  //   * @throws MQClientException
  //   * @throws MQServerException
  //   */
  //  TopicAssignmentInfo getTopicAssignmentInfo(String topic)
  //      throws MQClientException, MQServerException {
  //    TopicAssignmentInfo topicAssignmentInfo = lastTopicAssignmentTable.get(topic);
  //    if (null != topicAssignmentInfo) {
  //      return topicAssignmentInfo;
  //    }
  //    log.info(
  //        "Load assignment of topic={} is not cached, try to acquire it from load balancer",
  // topic);
  //    topicAssignmentInfo = queryLoadAssignment(topic);
  //    log.info(
  //        "Fetch load assignment of topic={} first time, load assignment={}",
  //        topic,
  //        topicAssignmentInfo);
  //    if (null != topicAssignmentInfo && !lastTopicAssignmentTable.isEmpty()) {
  //      lastTopicAssignmentTable.put(topic, topicAssignmentInfo);
  //    }
  //    return topicAssignmentInfo;
  //  }

  public void subscribe(final String topic, final String subscribeExpression) {
    FilterExpression filterExpression = new FilterExpression(subscribeExpression);
    filterExpressionTable.put(topic, filterExpression);
  }

  // Not yet implemented.
  public void subscribe(final String topic, final MessageSelector messageSelector) {}

  public void unsubscribe(final String topic) {
    filterExpressionTable.remove(topic);
  }

  public boolean hasBeenStarted() {
    final ServiceState serviceState = state.get();
    return ServiceState.CREATED != serviceState;
  }

  public void registerMessageListener(MessageListenerConcurrently messageListenerConcurrently) {
    this.messageListenerConcurrently = messageListenerConcurrently;
  }

  public void registerMessageListener(MessageListenerOrderly messageListenerOrderly) {
    this.messageListenerOrderly = messageListenerOrderly;
  }

  private String selectTargetForQuery(String topic) throws MQClientException, MQServerException {
    final TopicRouteData topicRouteData = clientInstance.getTopicRouteInfo(topic);
    final List<BrokerData> brokerDataList = topicRouteData.getBrokerDataList();
    if (brokerDataList.isEmpty()) {
      // Should never reach here.
      throw new MQServerException("No broker could be selected.");
    }

    final BrokerData brokerData =
        brokerDataList.get(TopicAssignmentInfo.getNextQueryBrokerIndex() % brokerDataList.size());
    String target = brokerData.getBrokerAddressTable().get(MixAll.MASTER_BROKER_ID);
    return UtilAll.shiftTargetPort(target, MixAll.SHIFT_PORT);
  }

  private TopicAssignmentInfo queryLoadAssignment(String topic)
      throws MQClientException, MQServerException {
    final String target = selectTargetForQuery(topic);

    QueryAssignmentRequest request =
        QueryAssignmentRequest.newBuilder()
            .setTopic(topic)
            .setConsumerGroup(defaultMQPushConsumer.getGroupName())
            .setClientId(clientInstance.getClientId())
            .setStrategyName(LoadBalanceStrategy.DEFAULT_STRATEGY)
            .setMessageModel(MessageModel.CLUSTERING)
            .build();

    return clientInstance.queryLoadAssignment(target, request);
  }

  @Override
  public ConsumeData prepareHeartbeatData() {
    return null;
  }
}
