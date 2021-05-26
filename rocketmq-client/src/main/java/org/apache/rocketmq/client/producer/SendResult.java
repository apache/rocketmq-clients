package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class SendResult {
    private final SendStatus sendStatus;
    private final String msgId;

    public SendResult(String msgId) {
        this.sendStatus = SendStatus.SEND_OK;
        this.msgId = msgId;
    }

    public SendStatus getSendStatus() {
        return SendStatus.SEND_OK;
    }
}
