package org.apache.rocketmq.client.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum LanguageCode {
  JAVA((byte) 0),
  CPP((byte) 1),
  DOTNET((byte) 2),
  PYTHON((byte) 3),
  DELPHI((byte) 4),
  ERLANG((byte) 5),
  RUBY((byte) 6),
  OTHER((byte) 7),
  HTTP((byte) 8),
  GO((byte) 9),
  PHP((byte) 10),
  OMS((byte) 11);

  private final byte code;
}
