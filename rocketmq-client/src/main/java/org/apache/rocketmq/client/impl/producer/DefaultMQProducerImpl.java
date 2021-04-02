package org.apache.rocketmq.client.impl.producer;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.CommunicationMode;
import org.apache.rocketmq.client.constant.MessageSysFlag;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.exception.RemotingException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.impl.ClientManager;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageBatch;
import org.apache.rocketmq.client.message.MessageClientIdUtils;
import org.apache.rocketmq.client.message.MessageConst;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.Validators;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.proto.ProducerData;
import org.apache.rocketmq.proto.SendMessageRequest;
import org.apache.rocketmq.proto.SendMessageResponse;

@Slf4j
public class DefaultMQProducerImpl implements ProducerObserver {
  private final DefaultMQProducer defaultMQProducer;
  private final ConcurrentMap<String /* topic */, TopicPublishInfo> topicPublishInfoTable;
  private ClientInstance clientInstance;
  private final AtomicReference<ServiceState> state;

  public DefaultMQProducerImpl(DefaultMQProducer defaultMQProducer) {
    this.defaultMQProducer = defaultMQProducer;
    this.topicPublishInfoTable = new ConcurrentHashMap<String, TopicPublishInfo>();

    this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);
  }

  // Not yet implemented.
  public Set<String> getPublishTopicList() {
    return null;
  }

  // Not yet implemented.
  public boolean isPublishTopicNeedUpdate(String s) {
    return false;
  }

  // Not yet implemented.
  public TransactionCheckListener checkListener() {
    return null;
  }

  // Not yet implemented.
  public void removeTopicPublishInfo(String s) {}

  // Not yet implemented.
  public boolean isUnitMode() {
    return false;
  }

  /**
   * Start the producer, please do not start producer repeatedly.
   *
   * @throws MQClientException the mq client exception.
   */
  public void start() throws MQClientException {
    final String producerGroup = defaultMQProducer.getGroupName();

    if (!state.compareAndSet(ServiceState.CREATED, ServiceState.STARTING)) {
      throw new MQClientException(
          "The producer has attempted to be started before, producerGroup=" + producerGroup);
    }

    clientInstance = ClientManager.getClientInstance(defaultMQProducer);
    final boolean registerResult = clientInstance.registerProducerObserver(producerGroup, this);
    if (!registerResult) {
      throw new MQClientException(
          "The producer group has been created already, please specify another one, producerGroup="
              + producerGroup);
    } else {
      log.debug("Registered producer observer, producerGroup={}", producerGroup);
    }

    clientInstance.start();
    state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
  }

  public void shutdown() {
    state.compareAndSet(ServiceState.STARTING, ServiceState.STOPPING);
    state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING);
    final ServiceState serviceState = state.get();
    if (ServiceState.STOPPING == serviceState) {
      clientInstance.unregisterProducerObserver(defaultMQProducer.getProducerGroup());
      clientInstance.shutdown();
      state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED);
      return;
    }
    log.warn("Failed to shutdown producer, unexpected state={}.", serviceState);
  }

  public boolean hasBeenStarted() {
    final ServiceState serviceState = state.get();
    return ServiceState.CREATED != serviceState;
  }

  // Not yet implemented.
  public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
    return null;
  }

  // TODO: compress message here.
  private void messagePretreated(Message message) {
    if (!(message instanceof MessageBatch)) {
      MessageClientIdUtils.setMessageId(message);
      if (this.defaultMQProducer.isAddExtendUniqInfo()) {
        // TODO: MessageClientIdUtils.setExtendUniqInfo(msg,this.defaultMQProducer.getRandomSign());
      }
    }
  }

  private SendMessageRequest wrapSendMessageRequest(Message message, MessageQueue mq) {
    final SendMessageRequest.Builder requestBuilder = SendMessageRequest.newBuilder();
    org.apache.rocketmq.proto.Message.Builder messageBuilder =
        org.apache.rocketmq.proto.Message.newBuilder()
            .setTopic(message.getTopic())
            .setBody(ByteString.copyFrom(message.getBody()));

    requestBuilder.setBornTimestamp(System.currentTimeMillis());
    requestBuilder.setProducerGroup(defaultMQProducer.getProducerGroup());
    if (defaultMQProducer.isUseDefaultTopicIfNotFound()) {
      requestBuilder.setDefaultTopic(defaultMQProducer.getCreateTopicKey());
      requestBuilder.setDefaultTopicQueueNumber(defaultMQProducer.getDefaultTopicQueueNums());
    }

    final Map<String, String> properties = message.getProperties();
    messageBuilder.putAllProperties(properties);
    messageBuilder.setFlag(message.getFlag());

    int sysFlag = MessageSysFlag.EMPTY_FLAG;
    final boolean transaction_flag =
        Boolean.parseBoolean(properties.get(MessageConst.PROPERTY_TRANSACTION_PREPARED));
    if (transaction_flag) {
      sysFlag = MessageSysFlag.setTransactionPreparedFlag(MessageSysFlag.EMPTY_FLAG);
    }
    requestBuilder.setSysFlag(sysFlag);
    requestBuilder.setQueueId(mq.getQueueId());
    requestBuilder.setBrokerName(mq.getBrokerName());

    return requestBuilder.setMessage(messageBuilder.build()).build();
  }

  private TopicPublishInfo getPublicInfo(String topic) throws MQClientException {
    TopicPublishInfo topicPublishInfo = topicPublishInfoTable.get(topic);
    if (null != topicPublishInfo) {
      return topicPublishInfo;
    }
    TopicRouteData topicRouteData = clientInstance.getTopicRouteInfo(topic);
    topicPublishInfo = new TopicPublishInfo(topic, topicRouteData);
    topicPublishInfoTable.put(topic, topicPublishInfo);
    return topicPublishInfo;
  }

  private MessageQueue selectOneMessageQueue(TopicPublishInfo topicPublishInfo) {
    final Set<String> isolatedTargets = clientInstance.getIsolatedTargets();
    return topicPublishInfo.selectOneMessageQueue(isolatedTargets);
  }

  private int getTotalSendTimes(CommunicationMode mode) {
    int totalSendTimes = 1;
    if (mode == CommunicationMode.SYNC) {
      totalSendTimes += defaultMQProducer.getRetryTimesWhenSendFailed();
    }
    return totalSendTimes;
  }

  boolean isRunning() {
    return ServiceState.STARTED == state.get();
  }

  void ensureRunning() throws MQClientException {
    if (!isRunning()) {
      throw new MQClientException("Producer is not started");
    }
  }

  public SendResult send(Message message)
      throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
    return send(message, defaultMQProducer.getSendMsgTimeout());
  }

  public SendResult send(Message message, long timeoutMillis)
      throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
    return sendDefaultImpl(message, CommunicationMode.SYNC, null, timeoutMillis);
  }

  public SendResult send(Message message, MessageQueue mq)
      throws MQClientException, RemotingException, MQBrokerException, InterruptedException,
          MQServerException {
    return send(message, mq, defaultMQProducer.getSendMsgTimeout());
  }

  public SendResult send(Message message, MessageQueue mq, long timeoutMillis)
      throws MQClientException, RemotingException, MQBrokerException, InterruptedException,
          MQServerException {
    ensureRunning();
    return sendKernelImpl(message, mq, CommunicationMode.SYNC, null, timeoutMillis);
  }

  public void send(Message message, SendCallback sendCallback)
      throws MQClientException, RemotingException, InterruptedException {
    send(message, sendCallback, defaultMQProducer.getSendMsgTimeout());
  }

  public void send(Message message, SendCallback sendCallback, long timeout)
      throws MQClientException, RemotingException, InterruptedException {
    sendDefaultImpl(message, CommunicationMode.ASYNC, sendCallback, timeout);
  }

  public void send(Message message, MessageQueue mq, SendCallback sendCallback)
      throws MQClientException, RemotingException, InterruptedException, MQServerException {
    send(message, mq, sendCallback, defaultMQProducer.getSendMsgTimeout());
  }

  public void send(Message message, MessageQueue mq, SendCallback sendCallback, long timeoutMillis)
      throws MQClientException, RemotingException, InterruptedException, MQServerException {
    ensureRunning();
    sendKernelImpl(message, mq, CommunicationMode.ASYNC, sendCallback, timeoutMillis);
  }

  public void sendOneway(Message message)
      throws MQClientException, RemotingException, InterruptedException {
    sendDefaultImpl(
        message, CommunicationMode.ONE_WAY, null, defaultMQProducer.getSendMsgTimeout());
  }

  public void sendOneway(Message message, MessageQueue mq)
      throws MQClientException, RemotingException, InterruptedException, MQServerException {
    ensureRunning();
    sendKernelImpl(
        message, mq, CommunicationMode.ONE_WAY, null, defaultMQProducer.getSendMsgTimeout());
  }

  public SendResult send(Message message, MessageQueueSelector selector, Object arg)
      throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
    return send(message, selector, arg, defaultMQProducer.getSendMsgTimeout());
  }

  public SendResult send(Message message, MessageQueueSelector selector, Object arg, long timeout)
      throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
    return sendSelectImpl(message, selector, arg, CommunicationMode.SYNC, null, timeout);
  }

  public void send(
      Message message, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
      throws MQClientException, RemotingException, InterruptedException {
    send(message, selector, arg, sendCallback, defaultMQProducer.getSendMsgTimeout());
  }

  public void send(
      Message message,
      MessageQueueSelector selector,
      Object arg,
      SendCallback sendCallback,
      long timeout)
      throws MQClientException, RemotingException, InterruptedException {
    sendSelectImpl(message, selector, arg, CommunicationMode.ASYNC, sendCallback, timeout);
  }

  public void sendOneway(Message message, MessageQueueSelector selector, Object arg)
      throws MQClientException, RemotingException, InterruptedException {
    sendSelectImpl(
        message,
        selector,
        arg,
        CommunicationMode.ONE_WAY,
        null,
        defaultMQProducer.getSendMsgTimeout());
  }

  public SendResult sendKernelImpl(
      Message message,
      MessageQueue mq,
      CommunicationMode mode,
      SendCallback sendCallback,
      long timeoutMillis)
      throws MQClientException, MQServerException {
    TopicPublishInfo publicInfo = getPublicInfo(message.getTopic());
    final String target = publicInfo.resolveTarget(mq.getBrokerName());
    if (null == target) {
      throw new MQClientException("Failed to parse target address from topic route");
    }
    messagePretreated(message);
    final SendMessageRequest request = wrapSendMessageRequest(message, mq);

    final SendMessageResponse response =
        clientInstance.sendClientAPI(
            target, mode, request, sendCallback, timeoutMillis, TimeUnit.MILLISECONDS);

    return clientInstance.processSendResponse(mq, response);
  }

  public SendResult sendDefaultImpl(
      Message message, CommunicationMode mode, SendCallback sendCallback, long timeoutMillis)
      throws MQClientException {
    ensureRunning();
    final TopicPublishInfo publicInfo = getPublicInfo(message.getTopic());
    SendResult sendResult;
    final int totalSendTimes = getTotalSendTimes(mode);
    for (int time = 0; time < totalSendTimes; time++) {
      final MessageQueue messageQueue = selectOneMessageQueue(publicInfo);
      if (null == messageQueue) {
        throw new MQClientException("Failed to select a message queue for sending.");
      }
      try {
        sendResult = sendKernelImpl(message, messageQueue, mode, sendCallback, timeoutMillis);
        final SendStatus sendStatus = sendResult.getSendStatus();
        if (sendStatus.isSuccess()) {
          return sendResult;
        }
      } catch (Throwable t) {
        log.warn("Exception occurs while sending message.", t);

        final String target = publicInfo.resolveTarget(messageQueue.getBrokerName());
        clientInstance.setTargetIsolated(target, true);
        log.debug(
            "Would isolate target for a while cause failed to send message, target={}", target);
      }
    }
    throw new MQClientException("Failed to send message");
  }

  private SendResult sendSelectImpl(
      Message message,
      MessageQueueSelector selector,
      Object arg,
      CommunicationMode mode,
      SendCallback sendCallback,
      long timeoutMillis)
      throws MQClientException {
    ensureRunning();
    Validators.messageCheck(message, defaultMQProducer.getMaxMessageSize());

    final TopicPublishInfo publicInfo = getPublicInfo(message.getTopic());

    SendResult sendResult;
    MessageQueue messageQueue;

    try {
      messageQueue = selector.select(publicInfo.getMessageQueueList(), message, arg);
    } catch (Throwable t) {
      throw new MQClientException("Exception occurs while select message queue.");
    }
    if (null == messageQueue) {
      throw new MQClientException("Message queue is unexpectedly null.");
    }

    final String target = publicInfo.resolveTarget(messageQueue.getBrokerName());
    if (!clientInstance.isTargetIsolated(target)) {
      try {
        sendResult = sendKernelImpl(message, messageQueue, mode, sendCallback, timeoutMillis);
        return sendResult;
      } catch (Throwable t) {
        log.warn("Exception occurs while sending message.", t);

        clientInstance.setTargetIsolated(target, true);
        log.debug(
            "Would isolate target for a while cause failed to send message, target={}", target);
      }
    } else {
      log.warn("Could not select a message queue cause target[{}] is isolated.", target);
    }

    return null;
  }

  // Not implemented yet.
  public void setCallbackExecutor(final ExecutorService callbackExecutor) {}

  @Override
  public ProducerData prepareHeartbeatData() {
    return ProducerData.newBuilder().setGroupName(defaultMQProducer.getGroupName()).build();
  }

  @Override
  public void onTopicRouteChanged(String topic, TopicRouteData topicRouteData) {
    final TopicPublishInfo topicPublishInfo = topicPublishInfoTable.get(topic);
    // Need filter topic in advance?
    if (null == topicPublishInfo) {
      log.info("No topic publish info, skip updating, topic={}", topic);
      return;
    }
    topicPublishInfo.refreshTopicRoute(topicRouteData);
  }
}
