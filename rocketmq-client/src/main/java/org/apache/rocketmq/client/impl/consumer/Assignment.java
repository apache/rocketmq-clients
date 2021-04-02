package org.apache.rocketmq.client.impl.consumer;

import java.util.Map;
import org.apache.rocketmq.client.message.MessageQueue;

public class Assignment {
  private final MessageQueue messageQueue;
  private final MessageRequestMode messageRequestMode;
  private final Map<String, String> attachments;

  public Assignment(
      MessageQueue messageQueue,
      MessageRequestMode messageRequestMode,
      Map<String, String> attachments) {
    this.messageQueue = messageQueue;
    this.messageRequestMode = messageRequestMode;
    this.attachments = attachments;
  }
}
