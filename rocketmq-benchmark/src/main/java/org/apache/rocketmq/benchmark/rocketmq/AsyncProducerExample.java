package org.apache.rocketmq.benchmark.rocketmq;

import com.google.common.base.Stopwatch;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;

@Slf4j
public class AsyncProducerExample {
  public static void main(String[] args) throws MQClientException, InterruptedException {
    DefaultMQProducer producer = new DefaultMQProducer("TestGroup");
    producer.setNamesrvAddr("11.167.164.105:9876");
    producer.start();

    int messageNum = 100000;
    final Stopwatch started = Stopwatch.createStarted();
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
        log.error("Unexpected error", e);
        latch.countDown();
        Thread.sleep(1000);
      }
    }
    //    final boolean await = latch.await(15, TimeUnit.SECONDS);
    latch.await();
    final long elapsed = started.elapsed(TimeUnit.MILLISECONDS);
    log.info(
        "Sending {} message(s) costs {}ms, latch counter={}",
        messageNum,
        elapsed,
        latch.getCount());
    producer.shutdown();
  }
}
