package org.apache.rocketmq.client.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum EnvVariable {
  NAMESRV_ADDR("NAMESRV_ADDR");
  private final String name;
}
