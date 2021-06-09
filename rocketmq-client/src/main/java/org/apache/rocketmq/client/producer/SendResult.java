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
    private final String transactionId;
    private final RpcTarget rpcTarget;

    public SendResult(RpcTarget rpcTarget, String msgId) {
        this(rpcTarget, msgId, StringUtils.EMPTY);
    }

    public SendResult(RpcTarget rpcTarget, String msgId, String transactionId) {
        this.rpcTarget = rpcTarget;
        this.msgId = msgId;
        this.transactionId = transactionId;
    }
}
