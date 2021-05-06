package org.apache.rocketmq.benchmark.rocketmq;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.MessageExt;

@Slf4j
public class PushConsumerExample {
  public static AtomicLong CONSUME_TIMES = new AtomicLong(0);

  public static void main(String[] args) throws MQClientException, InterruptedException {
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TestGroup");
    consumer.setNamesrvAddr("11.167.164.105:9876");
    consumer.subscribe("TestTopicXXX", "*");
    consumer.registerMessageListener(
        new MessageListenerConcurrently() {
          @Override
          public ConsumeConcurrentlyStatus consumeMessage(
              List<MessageExt> messages, ConsumeConcurrentlyContext context) {
            for (MessageExt message : messages) {
              if (CONSUME_TIMES.incrementAndGet() % 64 == 0) {
                log.info("Received message, {}", message);
              }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
          }
        });
    consumer.start();
    log.info("Consumer started.");
    Thread.sleep(60 * 1000);
    consumer.shutdown();
    log.info("Consumer is shutdown.");
  }
}
