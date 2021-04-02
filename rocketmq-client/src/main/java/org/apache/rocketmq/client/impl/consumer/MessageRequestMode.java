package org.apache.rocketmq.client.impl.consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum MessageRequestMode {
  PULL(0),
  POP(1);

  private final int mode;
}
