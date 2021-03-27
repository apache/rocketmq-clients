package org.apache.rocketmq.client.misc;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.constant.SystemTopic;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.Message;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ValidatorsTest {

  @Test
  public void testTopicCheck() throws MQClientException {
    Validators.topicCheck("abc");
    // Blank case 1.
    Assert.expectThrows(MQClientException.class, () -> Validators.topicCheck(" "));
    Assert.expectThrows(MQClientException.class, () -> Validators.topicCheck(" abc "));
    Assert.expectThrows(MQClientException.class, () -> Validators.topicCheck(" abc"));
    Assert.expectThrows(MQClientException.class, () -> Validators.topicCheck("abc "));
    // Blank case 2.
    Assert.expectThrows(MQClientException.class, () -> Validators.topicCheck("abc\t"));
    Assert.expectThrows(MQClientException.class, () -> Validators.topicCheck("abc\n"));
    Assert.expectThrows(MQClientException.class, () -> Validators.topicCheck("abc\f"));
    Assert.expectThrows(MQClientException.class, () -> Validators.topicCheck("abc\r"));
    // Illegal case.
    Assert.expectThrows(MQClientException.class, () -> Validators.topicCheck("[abc]"));
    // To long case.
    final String tooLongTopic = StringUtils.repeat("a", Validators.TOPIC_MAX_LENGTH + 1);
    Assert.expectThrows(MQClientException.class, () -> Validators.topicCheck(tooLongTopic));
    // Equals to default topic.
    Assert.expectThrows(
        MQClientException.class, () -> Validators.topicCheck(SystemTopic.DEFAULT_TOPIC.getTopic()));
  }

  @Test
  public void testConsumerGroupCheck() throws MQClientException {
    Validators.consumerGroupCheck("abc");
    // Blank case 1.
    Assert.expectThrows(MQClientException.class, () -> Validators.consumerGroupCheck(" "));
    Assert.expectThrows(MQClientException.class, () -> Validators.consumerGroupCheck(" abc "));
    Assert.expectThrows(MQClientException.class, () -> Validators.consumerGroupCheck(" abc"));
    Assert.expectThrows(MQClientException.class, () -> Validators.consumerGroupCheck("abc "));
    // Blank case 2.
    Assert.expectThrows(MQClientException.class, () -> Validators.consumerGroupCheck("abc\t"));
    Assert.expectThrows(MQClientException.class, () -> Validators.consumerGroupCheck("abc\n"));
    Assert.expectThrows(MQClientException.class, () -> Validators.consumerGroupCheck("abc\f"));
    Assert.expectThrows(MQClientException.class, () -> Validators.consumerGroupCheck("abc\r"));
    // Illegal case.
    Assert.expectThrows(MQClientException.class, () -> Validators.consumerGroupCheck("[abc]"));
    // To long case.
    final String tooLongConsumerGroup =
        StringUtils.repeat("a", Validators.CONSUMER_GROUP_MAX_LENGTH + 1);
    Assert.expectThrows(
        MQClientException.class, () -> Validators.consumerGroupCheck(tooLongConsumerGroup));
  }

  @Test
  public void testMessageCheck() throws MQClientException {
    int bodyMaxSize = 3;
    Assert.expectThrows(MQClientException.class, () -> Validators.messageCheck(null, bodyMaxSize));
    {
      final Message message = new Message();
      message.setTopic("abc");
      message.setBody(new byte[bodyMaxSize]);
      Validators.messageCheck(message, bodyMaxSize);
    }
    // Topic is blank.
    {
      final Message message = new Message();
      message.setTopic("");
      Assert.expectThrows(
          MQClientException.class, () -> Validators.messageCheck(message, bodyMaxSize));
    }
    // Body is null.
    {
      final Message message = new Message();
      message.setTopic("abc");
      Assert.expectThrows(
          MQClientException.class, () -> Validators.messageCheck(message, bodyMaxSize));
    }
    // Body length is zero.
    {
      final Message message = new Message();
      message.setTopic("abc");
      message.setBody(new byte[0]);
      Assert.expectThrows(
          MQClientException.class, () -> Validators.messageCheck(message, bodyMaxSize));
    }
    // Body length exceeds.
    {
      final Message message = new Message();
      message.setTopic("abc");
      message.setBody(new byte[bodyMaxSize + 1]);
      Assert.expectThrows(
          MQClientException.class, () -> Validators.messageCheck(message, bodyMaxSize));
    }
  }
}
