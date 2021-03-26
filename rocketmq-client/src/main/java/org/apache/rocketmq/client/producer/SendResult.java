package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.client.message.MessageQueue;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class SendResult {
  private SendStatus sendStatus;
  private String msgId;
  private MessageQueue messageQueue;
  private long queueOffset;
  private String transactionId;
  private String offsetMsgId;
  private String regionId;
  private boolean traceOn = true;

  public SendResult(
      SendStatus sendStatus,
      String msgId,
      String offsetMsgId,
      MessageQueue messageQueue,
      long queueOffset) {
    this.sendStatus = sendStatus;
    this.msgId = msgId;
    this.offsetMsgId = offsetMsgId;
    this.messageQueue = messageQueue;
    this.queueOffset = queueOffset;
  }

  public SendResult(
      final SendStatus sendStatus,
      final String msgId,
      final MessageQueue messageQueue,
      final long queueOffset,
      final String transactionId,
      final String offsetMsgId,
      final String regionId) {
    this.sendStatus = sendStatus;
    this.msgId = msgId;
    this.messageQueue = messageQueue;
    this.queueOffset = queueOffset;
    this.transactionId = transactionId;
    this.offsetMsgId = offsetMsgId;
    this.regionId = regionId;
  }
}
