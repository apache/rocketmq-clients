package org.apache.rocketmq.client.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum SystemProducerGroup {
  DEFAULT_PRODUCER_GROUP("DEFAULT_PRODUCER");
  private final String groupId;
}
