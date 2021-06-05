package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class SendResult {
    private final SendStatus sendStatus = SendStatus.SEND_OK;
    private final String msgId;

    public SendResult(String msgId) {
        this.msgId = msgId;
    }
}
