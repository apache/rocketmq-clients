package org.apache.rocketmq.client.consumer;

import java.util.Set;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RemotingException;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;

public class DefaultMQPushConsumer implements MQPushConsumer {
  @Override
  public void start() throws MQClientException {}

  @Override
  public void shutdown() {}

  @Override
  public void registerMessageListener(MessageListener messageListener) {}

  @Override
  public void registerMessageListener(MessageListenerConcurrently messageListenerConcurrently) {}

  @Override
  public void registerMessageListener(MessageListenerOrderly messageListenerOrderly) {}

  @Override
  public void subscribe(String s, String s1) throws MQClientException {}

  @Override
  public void subscribe(String s, String s1, String s2) throws MQClientException {}

  @Override
  public void subscribe(String s, MessageSelector messageSelector) throws MQClientException {}

  @Override
  public void unsubscribe(String s) {}

  @Override
  public void updateCorePoolSize(int i) {}

  @Override
  public void allowCoreThreadTimeOut(boolean b) {}

  @Override
  public void suspend() {}

  @Override
  public void resume() {}

  @Override
  public void sendMessageBack(MessageExt messageExt, int i)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException {}

  @Override
  public void sendMessageBack(MessageExt messageExt, int i, String s)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException {}

  @Override
  public Set<MessageQueue> fetchSubscribeMessageQueues(String s) throws MQClientException {
    return null;
  }
}
