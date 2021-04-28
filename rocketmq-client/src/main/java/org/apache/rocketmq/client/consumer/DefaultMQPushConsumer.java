package org.apache.rocketmq.client.consumer;

import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.constant.ConsumeFromWhere;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RemotingException;
import org.apache.rocketmq.client.impl.ClientConfig;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;

public class DefaultMQPushConsumer extends ClientConfig {

  /** Wrapping internal implementations for virtually all methods presented in this class. */
  protected final DefaultMQPushConsumerImpl impl;

  @Getter @Setter private ConsumeFromWhere consumeFromWhere;

  @Getter private int consumeThreadMin = 20;

  @Getter private int consumeThreadMax = 64;

  @Getter @Setter private int consumeMessageBatchMaxSize = 1;

  @Getter @Setter private boolean ackMessageAsync = true;

  public String getConsumerGroup() {
    return this.getGroupName();
  }

  public void setConsumerGroup(String consumerGroup) {
    if (impl.hasBeenStarted()) {
      throw new RuntimeException("Please set consumerGroup before consumer started.");
    }
    setGroupName(consumerGroup);
  }

  public void start() throws MQClientException {
    this.setGroupName(withNamespace(this.getGroupName()));
    this.impl.start();
  }

  public void shutdown() {
    this.impl.shutdown();
  }

  public DefaultMQPushConsumer(final String consumerGroup) {
    this.setGroupName(consumerGroup);
    this.consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    this.impl = new DefaultMQPushConsumerImpl(this);
  }

  public void registerMessageListener(MessageListenerConcurrently messageListenerConcurrently) {
    this.impl.registerMessageListener(messageListenerConcurrently);
  }

  public void registerMessageListener(MessageListenerOrderly messageListenerOrderly) {
    this.impl.registerMessageListener(messageListenerOrderly);
  }

  public void subscribe(String topic, String subscribeExpression) throws MQClientException {
    this.impl.subscribe(topic, subscribeExpression);
  }

  // Not yet implemented
  public void subscribe(String topic, String fullClassName, String filterClassSource)
      throws MQClientException {}

  // Not yet implemented
  public void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {}

  // Not yet implemented
  public void unsubscribe(String topic) {}

  // Not yet implemented
  public void updateCorePoolSize(int i) {}

  // Not yet implemented
  public void allowCoreThreadTimeOut(boolean b) {}

  // Not yet implemented
  public void suspend() {}

  // Not yet implemented
  public void resume() {}

  // Not yet implemented
  public void sendMessageBack(MessageExt messageExt, int i)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException {}

  // Not yet implemented
  public void sendMessageBack(MessageExt messageExt, int i, String s)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException {}

  // Not yet implemented
  public Set<MessageQueue> fetchSubscribeMessageQueues(String s) throws MQClientException {
    return null;
  }

  /**
   * Set maximum thread count of consume thread pool.
   *
   * <p>Also, if consumeThreadMax is less than consumeThreadMin, set consumeThreadMin equal to
   * consumeThreadMax.
   *
   * @param consumeThreadMax max consuming thread num.
   */
  public void setConsumeThreadMax(int consumeThreadMax) {
    this.consumeThreadMax = consumeThreadMax;
    if (this.consumeThreadMax < this.consumeThreadMin) {
      this.consumeThreadMin = consumeThreadMax;
    }
  }

  /**
   * Set minimum thread count of consume thread pool.
   *
   * <p>Also, if consumeThreadMax is less than consumeThreadMin, set consumeThreadMax equal to
   * consumeThreadMin.
   *
   * @param consumeThreadMin min consuming thread num.
   */
  public void setConsumeThreadMin(int consumeThreadMin) {
    this.consumeThreadMin = consumeThreadMin;
    if (this.consumeThreadMax < this.consumeThreadMin) {
      this.consumeThreadMax = consumeThreadMin;
    }
  }
}
