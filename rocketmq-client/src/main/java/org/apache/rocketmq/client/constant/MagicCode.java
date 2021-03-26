package org.apache.rocketmq.client.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum MagicCode {
  /** */
  MESSAGE_MAGIC_CODE(0xAABBCCDD ^ 1880681586 + 8),
  MESSAGE_MAGIC_CODE_V2(0xAABBCCDD ^ 1880681586 + 4);

  private final int code;
}
