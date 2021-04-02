package org.apache.rocketmq.client.misc;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.constant.SystemTopic;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.Message;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ValidatorsTest {

  private void checkIllegalTopic(String topic) {
    try {
      Validators.topicCheck(topic);
      Assert.fail();
    } catch (MQClientException ignore) {
    }
  }

  @Test
  public void testTopicCheck() throws MQClientException {
    Validators.topicCheck("abc");
    // Blank case 1.
    checkIllegalTopic(" ");
    checkIllegalTopic(" abc ");
    checkIllegalTopic("abc ");
    // Blank case 2
    checkIllegalTopic("abc\t");
    checkIllegalTopic("abc\t");
    checkIllegalTopic("abc\n");
    checkIllegalTopic("abc\f");
    checkIllegalTopic("abc\r");
    // Illegal case.
    checkIllegalTopic("[abc]");
    // Too long case.
    final String tooLongTopic = StringUtils.repeat("a", Validators.TOPIC_MAX_LENGTH + 1);
    checkIllegalTopic(tooLongTopic);
    // Equals to default topic.
    checkIllegalTopic(SystemTopic.DEFAULT_TOPIC.getTopic());
  }

  private void checkIllegalConsumerGroup(String consumerGroup) {
    try {
      Validators.consumerGroupCheck(consumerGroup);
      Assert.fail();
    } catch (MQClientException ignore) {
    }
  }

  @Test
  public void testConsumerGroupCheck() throws MQClientException {
    Validators.consumerGroupCheck("abc");
    // Blank case 1.
    checkIllegalConsumerGroup(" ");
    checkIllegalConsumerGroup(" abc ");
    checkIllegalConsumerGroup("abc ");
    // Blank case 2
    checkIllegalConsumerGroup("abc\t");
    checkIllegalConsumerGroup("abc\t");
    checkIllegalConsumerGroup("abc\n");
    checkIllegalConsumerGroup("abc\f");
    checkIllegalConsumerGroup("abc\r");
    // Illegal case.
    checkIllegalConsumerGroup("[abc]");
    // Too long case.
    final String tooLongConsumerGroup =
        StringUtils.repeat("a", Validators.CONSUMER_GROUP_MAX_LENGTH + 1);
    checkIllegalConsumerGroup(tooLongConsumerGroup);
  }

  private void checkIllegalMessage(final Message message, final int bodyMaxSize) {
    try {
      Validators.messageCheck(message, bodyMaxSize);
      Assert.fail();
    } catch (MQClientException ignore) {
    }
  }

  @Test
  public void testMessageCheck() throws MQClientException {
    int bodyMaxSize = 3;

    {
      final Message message = new Message();
      message.setTopic("abc");
      message.setBody(new byte[bodyMaxSize]);
      Validators.messageCheck(message, bodyMaxSize);
    }
    // Null case.
    checkIllegalMessage(null, bodyMaxSize);
    // Topic is blank.
    {
      final Message message = new Message();
      message.setTopic("");
      checkIllegalMessage(message, bodyMaxSize);
    }
    // Body is null.
    {
      final Message message = new Message();
      message.setTopic("abc");
      checkIllegalMessage(message, bodyMaxSize);
    }
    // Body length is zero.
    {
      final Message message = new Message();
      message.setTopic("abc");
      message.setBody(new byte[0]);
      checkIllegalMessage(message, bodyMaxSize);
    }
    // Body length exceeds.
    {
      final Message message = new Message();
      message.setTopic("abc");
      message.setBody(new byte[bodyMaxSize + 1]);
      checkIllegalMessage(message, bodyMaxSize);
    }
  }
}
