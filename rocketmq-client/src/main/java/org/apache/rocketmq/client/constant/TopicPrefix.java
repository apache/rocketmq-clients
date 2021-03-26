package org.apache.rocketmq.client.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum TopicPrefix {
  RETRY_TOPIC_PREFIX("%RETRY%"),
  DLQ_TOPIC_PREFIX("%DLQ%");

  private final String topicPrefix;
}
