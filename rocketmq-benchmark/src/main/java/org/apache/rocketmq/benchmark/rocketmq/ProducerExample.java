package org.apache.rocketmq.benchmark.rocketmq;

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;

@Slf4j
public class ProducerExample {
  public static void main(String[] args) throws MQClientException, InterruptedException {
    DefaultMQProducer producer = new DefaultMQProducer("TestGroup");
    producer.setNamesrvAddr("11.167.164.105:9876");
    producer.start();

    int messageNum = 1000;
    final Stopwatch started = Stopwatch.createStarted();
    for (int i = 0; i < messageNum; i++) {
      try {
        Message msg =
            new Message(
                "TestTopic" /* Topic */,
                "TagA" /* Tag */,
                ("Hello RocketMQ " + i).getBytes(MixAll.DEFAULT_CHARSET) /* Message body */);
        SendResult sendResult = producer.send(msg);
        log.info("{}", sendResult);
      } catch (Exception e) {
        e.printStackTrace();
        Thread.sleep(1000);
      }
    }
    final long elapsed = started.elapsed(TimeUnit.MILLISECONDS);
    log.info("Sending {} message(s) costs {}ms", messageNum, elapsed);
    producer.shutdown();
  }
}
