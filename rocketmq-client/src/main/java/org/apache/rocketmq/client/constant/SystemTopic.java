package org.apache.rocketmq.client.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum SystemTopic {
  DEFAULT_TOPIC("TBW102");

  private final String topic;
}
