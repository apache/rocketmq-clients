package org.apache.rocketmq.client.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode()
@AllArgsConstructor
@NoArgsConstructor
public class MessageQueue {
  private String topic;
  private String brokerName;
  private int queueId;
}
