package org.apache.rocketmq.client.misc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.constant.SystemTopic;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.Message;

public class Validators {
  public static final String TOPIC_REGEX = "^[%|a-zA-Z0-9._-]+$";
  public static final Pattern TOPIC_PATTERN = Pattern.compile(TOPIC_REGEX);
  public static final int TOPIC_MAX_LENGTH = 255;

  public static final String CONSUMER_GROUP_REGEX = TOPIC_REGEX;
  public static final Pattern CONSUMER_GROUP_PATTERN = Pattern.compile(CONSUMER_GROUP_REGEX);
  public static final int CONSUMER_GROUP_MAX_LENGTH = TOPIC_MAX_LENGTH;

  private Validators() {}

  private static boolean regexNotMatched(String origin, Pattern pattern) {
    Matcher matcher = pattern.matcher(origin);
    return !matcher.matches();
  }

  public static void topicCheck(String topic) throws MQClientException {
    if (!StringUtils.isNoneBlank(topic)) {
      throw new MQClientException("Topic is blank.");
    }
    if (regexNotMatched(topic, TOPIC_PATTERN)) {
      throw new MQClientException(String.format("Topic[%s] is illegal.", topic));
    }
    if (topic.length() > TOPIC_MAX_LENGTH) {
      throw new MQClientException(
          "Topic's length exceeds the threshold, masSize=" + TOPIC_MAX_LENGTH + " bytes");
    }
    if (topic.equals(SystemTopic.DEFAULT_TOPIC.getTopic())) {
      throw new MQClientException("Topic is conflict with the system default topic.");
    }
  }

  public static void consumerGroupCheck(String consumerGroup) throws MQClientException {
    if (!StringUtils.isNoneBlank(consumerGroup)) {
      throw new MQClientException("ConsumerGroup is blank.");
    }
    if (regexNotMatched(consumerGroup, CONSUMER_GROUP_PATTERN)) {
      throw new MQClientException(String.format("ConsumerGroup[%s] is illegal.", consumerGroup));
    }
    if (consumerGroup.length() > CONSUMER_GROUP_MAX_LENGTH) {
      throw new MQClientException(
          "ConsumerGroup' length exceeds the threshold, maxSize"
              + CONSUMER_GROUP_MAX_LENGTH
              + " bytes");
    }
  }

  public static void messageCheck(Message message, int bodyMaxSize) throws MQClientException {
    if (null == message) {
      throw new MQClientException("Message is null.");
    }

    topicCheck(message.getTopic());

    final byte[] body = message.getBody();
    if (null == body) {
      throw new MQClientException("Message body is null.");
    }
    if (0 == body.length) {
      throw new MQClientException("Message body's length is zero.");
    }
    if (body.length > bodyMaxSize) {
      throw new MQClientException(
          "Message body's length exceeds the threshold, maxSize=" + bodyMaxSize + " bytes.");
    }
  }
}
