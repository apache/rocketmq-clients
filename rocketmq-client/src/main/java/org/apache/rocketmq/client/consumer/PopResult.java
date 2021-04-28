package org.apache.rocketmq.client.consumer;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.client.message.MessageExt;

@AllArgsConstructor
@Getter
public class PopResult {
  private final String target;
  private final PopStatus popStatus;

  private final long termId;
  private final long popTimestamp;
  private final long invisibleTime;
  private final long restNum;

  private final List<MessageExt> msgFoundList;
}
