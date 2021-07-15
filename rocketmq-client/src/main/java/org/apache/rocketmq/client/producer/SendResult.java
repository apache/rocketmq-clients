package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.remoting.RpcTarget;

@Getter
@ToString
public class SendResult {
    private final SendStatus sendStatus = SendStatus.SEND_OK;
    private final String msgId;
    private final RpcTarget target;
    private final String transactionId;

    public SendResult(RpcTarget target, String msgId) {
        this(target, msgId, StringUtils.EMPTY);
    }

    public SendResult(RpcTarget target, String msgId, String transactionId) {
        this.target = target;
        this.msgId = msgId;
        this.transactionId = transactionId;
    }
}
