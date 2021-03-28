package org.apache.rocketmq.benchmark;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;

@Slf4j
public class AsyncProducerExample {
  public static void main(String[] args) throws MQClientException, InterruptedException {
    DefaultMQProducer producer = new DefaultMQProducer("default_producer_group");
    producer.setNamesrvAddr("11.167.164.105:9876");
    producer.start();

    int messageNum = 32;
    CountDownLatch latch = new CountDownLatch(messageNum);

    for (int i = 0; i < messageNum; i++) {
      try {
        Message msg =
            new Message(
                "TestTopic" /* Topic */,
                "TagA" /* Tag */,
                ("Hello RocketMQ " + i).getBytes(StandardCharsets.UTF_8) /* Message body */);
        producer.send(
            msg,
            new SendCallback() {
              @Override
              public void onSuccess(SendResult sendResult) {
                log.info("{}", sendResult);
                latch.countDown();
              }

              @Override
              public void onException(Throwable e) {
                log.error("", e);
                latch.countDown();
              }
            });
      } catch (Exception e) {
        log.error("", e);
        latch.countDown();
        Thread.sleep(1000);
      }
    }
    latch.await();
    producer.shutdown();
  }
}
