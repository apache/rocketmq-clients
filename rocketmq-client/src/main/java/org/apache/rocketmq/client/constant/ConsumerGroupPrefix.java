package org.apache.rocketmq.client.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ConsumerGroupPrefix {
  CID_RMQ_SYS_PREFIX("CID_RMQ_SYS_");

  private final String consumerGroupPrefix;
}
