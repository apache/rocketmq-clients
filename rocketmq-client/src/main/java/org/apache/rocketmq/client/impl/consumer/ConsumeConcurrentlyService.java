package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.RateLimiter;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;

public class ConsumeConcurrentlyService implements ConsumeService {
  private AtomicReference<ServiceState> state;
  private DefaultMQPushConsumerImpl impl;
  private MessageListenerConcurrently messageListenerConcurrently;

  public ConsumeConcurrentlyService(
      DefaultMQPushConsumerImpl impl, MessageListenerConcurrently messageListenerConcurrently) {
    this.impl = impl;
    this.messageListenerConcurrently = messageListenerConcurrently;
    this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);
  }

  @Override
  public void start() {}

  @Override
  public void shutdown() {}

  @Override
  public void submitConsumeTask(
      List<MessageExt> messageExtList, ProcessQueue processQueue, MessageQueue messageQueue) {}

  @Override
  public boolean hasConsumeRateLimiter(String topic) {
    return false;
  }

  @Override
  public RateLimiter rateLimiter(String topic) {
    return null;
  }
}
