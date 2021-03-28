package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListener;

public class DefaultMQPushConsumerImpl {
  private final DefaultMQPushConsumer defaultMQPushConsumer;
  private MessageListener messageListener;

  public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer) {
    this.defaultMQPushConsumer = defaultMQPushConsumer;
  }

  public void registerMessageListener(MessageListener messageListener) {
    this.messageListener = messageListener;
  }
}
