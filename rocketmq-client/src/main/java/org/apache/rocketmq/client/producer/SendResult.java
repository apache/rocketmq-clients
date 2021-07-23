package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.remoting.Endpoints;

@Getter
@ToString
public class SendResult {
    private final SendStatus sendStatus = SendStatus.SEND_OK;
    private final String msgId;
    private final Endpoints endpoints;

    @ToString.Exclude
    private final String transactionId;

    public SendResult(Endpoints endpoints, String msgId) {
        this(endpoints, msgId, StringUtils.EMPTY);
    }

    public SendResult(Endpoints endpoints, String msgId, String transactionId) {
        this.endpoints = endpoints;
        this.msgId = msgId;
        this.transactionId = transactionId;
    }
}
