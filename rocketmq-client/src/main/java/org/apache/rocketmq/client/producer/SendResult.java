package org.apache.rocketmq.client.producer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.client.message.MessageQueue;

@Getter
@ToString
@AllArgsConstructor
public class SendResult {
    private final SendStatus sendStatus;
    private final String msgId;
    private final MessageQueue messageQueue;
    private final long queueOffset;
    private final String transactionId;
}
