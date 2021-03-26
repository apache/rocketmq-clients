import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;

public class AsyncProducerExample {
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
        producer.send(
            msg,
            new SendCallback() {
              @Override
              public void onSuccess(SendResult sendResult) {
                System.out.printf("%s%n", sendResult);
              }

              @Override
              public void onException(Throwable e) {
                System.out.println(e);
              }
            });

        SendResult sendResult = producer.send(msg);

      } catch (Exception e) {
        e.printStackTrace();
        Thread.sleep(1000);
      }
    }
    producer.shutdown();
  }
}
