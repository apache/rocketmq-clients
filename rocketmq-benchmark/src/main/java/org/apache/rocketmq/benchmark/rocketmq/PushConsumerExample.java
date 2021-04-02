package org.apache.rocketmq.benchmark.rocketmq;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.MessageExt;

@Slf4j
public class PushConsumerExample {
  public static void main(String[] args) throws MQClientException {
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TestGroup");
    consumer.setNamesrvAddr("11.167.164.105:9876");
    consumer.subscribe("TestTopic", "*");
    consumer.registerMessageListener(
        new MessageListenerConcurrently() {
          @Override
          public ConsumeConcurrentlyStatus consumeMessage(
              List<MessageExt> messages, ConsumeConcurrentlyContext context) {
            for (MessageExt message : messages) {
              final String messageBody = new String(message.getBody());
              log.info("Received message, {}", message);
              log.info("Message body={}", messageBody);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
          }
        });
    consumer.start();
    log.info("Consumer started.");
  }
}
