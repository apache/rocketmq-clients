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

  public void shutdown() {}

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

  public void subscribe(String topic, String subExpression) throws MQClientException {}

  public void subscribe(String topic, String fullClassName, String filterClassSource)
      throws MQClientException {}

  public void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {}

  public void unsubscribe(String topic) {}

  public void updateCorePoolSize(int i) {}

  public void allowCoreThreadTimeOut(boolean b) {}

  public void suspend() {}

  public void resume() {}

  public void sendMessageBack(MessageExt messageExt, int i)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException {}

  public void sendMessageBack(MessageExt messageExt, int i, String s)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException {}

  public Set<MessageQueue> fetchSubscribeMessageQueues(String s) throws MQClientException {
    return null;
  }
}
