package org.apache.rocketmq.client.misc;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.constant.ConsumerGroupPrefix;
import org.apache.rocketmq.client.constant.SystemTopic;
import org.apache.rocketmq.client.constant.TopicPrefix;

public class ResourceUtil {
  public static final char NAMESPACE_SEPARATOR = '%';

  private static final int RETRY_TOPIC_PREFIX_LENGTH =
      TopicPrefix.RETRY_TOPIC_PREFIX.getTopicPrefix().length();
  private static final int DLQ_TOPIC_PREFIX_LENGTH =
      TopicPrefix.DLQ_TOPIC_PREFIX.getTopicPrefix().length();

  public static boolean isSystemTopic(String topic) {
    return topic.startsWith(SystemTopic.DEFAULT_TOPIC.getTopic());
  }

  public static boolean isSystemConsumerGroup(String consumerGroup) {
    return consumerGroup.startsWith(
        ConsumerGroupPrefix.CID_RMQ_SYS_PREFIX.getConsumerGroupPrefix());
  }

  private static boolean isSystemResource(String resource) {
    if (StringUtils.isEmpty(resource)) {
      return false;
    }

    if (isSystemTopic(resource)) {
      return true;
    }

    if (isSystemConsumerGroup(resource)) {
      return true;
    }

    return SystemTopic.DEFAULT_TOPIC.getTopic().equals(resource);
  }

  public static boolean isRetryTopic(String resource) {
    if (StringUtils.isEmpty(resource)) {
      return false;
    }
    return resource.startsWith(TopicPrefix.RETRY_TOPIC_PREFIX.getTopicPrefix());
  }

  public static boolean isDLQTopic(String resource) {
    if (StringUtils.isEmpty(resource)) {
      return false;
    }
    return resource.startsWith(TopicPrefix.DLQ_TOPIC_PREFIX.getTopicPrefix());
  }

  public static boolean isAlreadyWithNamespace(String resource, String namespace) {
    if (StringUtils.isEmpty(namespace)
        || StringUtils.isEmpty(resource)
        || isSystemResource(resource)) {
      return false;
    }

    if (isRetryTopic(resource)) {
      resource = resource.substring(RETRY_TOPIC_PREFIX_LENGTH);
    }

    if (isDLQTopic(resource)) {
      resource = resource.substring(DLQ_TOPIC_PREFIX_LENGTH);
    }

    return resource.startsWith(namespace + NAMESPACE_SEPARATOR);
  }

  public static String wrapWithNamespace(String namespace, String resource) {
    if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(resource)) {
      return resource;
    }

    if (isSystemResource(resource)) {
      return resource;
    }

    if (isAlreadyWithNamespace(resource, namespace)) {
      return resource;
    }

    StringBuilder builder = new StringBuilder().append(namespace).append(NAMESPACE_SEPARATOR);

    if (isRetryTopic(resource)) {
      builder.append(resource.substring(RETRY_TOPIC_PREFIX_LENGTH));
      return builder.insert(0, TopicPrefix.RETRY_TOPIC_PREFIX.getTopicPrefix()).toString();
    }

    if (isDLQTopic(resource)) {
      builder.append(resource.substring(DLQ_TOPIC_PREFIX_LENGTH));
      return builder.insert(0, TopicPrefix.DLQ_TOPIC_PREFIX.getTopicPrefix()).toString();
    }

    return builder.append(resource).toString();
  }

  public static String jointWithRetryTopicPrefix(String consumerGroup) {
    return TopicPrefix.RETRY_TOPIC_PREFIX.getTopicPrefix() + consumerGroup;
  }

  public static String jointWithDLQTopicPrefix(String consumerGroup) {
    return TopicPrefix.DLQ_TOPIC_PREFIX.getTopicPrefix() + consumerGroup;
  }

  public static String unwrapWithNamespace(String namespace, String resource) {
    if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(resource)) {
      return resource;
    }
    if (isRetryTopic(resource)) {
      int index = resource.indexOf(NAMESPACE_SEPARATOR, RETRY_TOPIC_PREFIX_LENGTH);
      if (index > 0) {
        return jointWithRetryTopicPrefix(resource.substring(index + 1));
      }
      return resource;
    }
    if (isDLQTopic(resource)) {
      int index = resource.indexOf(NAMESPACE_SEPARATOR, DLQ_TOPIC_PREFIX_LENGTH);
      if (index > 0) {
        return jointWithDLQTopicPrefix(resource.substring(index + 1));
      }
      return resource;
    }
    int index = resource.indexOf(NAMESPACE_SEPARATOR);
    if (index > 0) {
      return resource.substring(index + 1);
    }
    return resource;
  }
}
