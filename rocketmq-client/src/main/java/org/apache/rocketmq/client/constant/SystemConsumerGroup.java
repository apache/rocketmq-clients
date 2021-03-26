package org.apache.rocketmq.client.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum SystemConsumerGroup {
  DEFAULT_CONSUMER_GROUP("DEFAULT_CONSUMER");
  private final String groupId;
}
