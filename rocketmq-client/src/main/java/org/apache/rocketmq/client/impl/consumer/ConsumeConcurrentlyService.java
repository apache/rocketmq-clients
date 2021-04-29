package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.RateLimiter;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.utility.ThreadFactoryImpl;

@Slf4j
public class ConsumeConcurrentlyService implements ConsumeService {
  private final AtomicReference<ServiceState> state;

  private final DefaultMQPushConsumer consumer;
  @Getter private final MessageListenerConcurrently messageListenerConcurrently;
  private final ThreadPoolExecutor consumeExecutor;

  public ConsumeConcurrentlyService(
      DefaultMQPushConsumerImpl consumerImpl,
      MessageListenerConcurrently messageListenerConcurrently) {
    this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);
    this.messageListenerConcurrently = messageListenerConcurrently;
    this.consumer = consumerImpl.getDefaultMQPushConsumer();

    this.consumeExecutor =
        new ThreadPoolExecutor(
            consumer.getConsumeThreadMin(),
            consumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("ConsumeMessageThread_"));
  }

  @Override
  public void start() throws MQClientException {
    if (!state.compareAndSet(ServiceState.CREATED, ServiceState.STARTED)) {
      throw new MQClientException(
          "ConsumerConcurrentlyService has attempted to be stared before, state=" + state.get());
    }
  }

  @Override
  public void shutdown() throws MQClientException {
    state.compareAndSet(ServiceState.CREATED, ServiceState.STOPPING);
    state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING);
    final ServiceState serviceState = state.get();
    if (ServiceState.STOPPING == serviceState) {
      consumeExecutor.shutdown();
      if (state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED)) {
        log.info("Shutdown ConsumeConcurrentlyService successfully.");
        return;
      }
    }
    throw new MQClientException(
        "Failed to shutdown ConsumeConcurrentlyService, state=" + state.get());
  }

  @Override
  public void dispatch(ProcessQueue processQueue) {
    final List<MessageExt> cachedMessages = processQueue.getCachedMessages();
    final int batchMaxSize = consumer.getConsumeMessageBatchMaxSize();
    final int size = cachedMessages.size();
    for (int i = 0; i < size; i += batchMaxSize) {
      try {
        final List<MessageExt> spitedMessages =
            cachedMessages.subList(i, Math.min(size, i + batchMaxSize));
        consumeExecutor.submit(new ConsumeConcurrentlyTask(this, processQueue, spitedMessages));
      } catch (Throwable t) {
        log.error(
            "Exception occurs while submitting consumeTask for mq={}, cached msg count={}, batchMaxSize={}",
            processQueue.getMessageQueue().simpleName(),
            size,
            batchMaxSize,
            t);
      }
    }
  }

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
