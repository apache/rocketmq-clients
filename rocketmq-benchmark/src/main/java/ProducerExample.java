import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;

@Slf4j
public class ProducerExample {
  public static void main(String[] args) throws MQClientException, InterruptedException {
    DefaultMQProducer producer = new DefaultMQProducer("default_producer_group");
    producer.setNamesrvAddr("11.167.164.105:9876");
    producer.start();

    for (int i = 0; i < 32; i++) {
      try {
        Message msg =
            new Message(
                "TestTopic" /* Topic */,
                "TagA" /* Tag */,
                ("Hello RocketMQ " + i).getBytes(StandardCharsets.UTF_8) /* Message body */);
        SendResult sendResult = producer.send(msg);
        log.info("{}", sendResult);
      } catch (Exception e) {
        e.printStackTrace();
        Thread.sleep(1000);
      }
    }
    producer.shutdown();
  }
}
