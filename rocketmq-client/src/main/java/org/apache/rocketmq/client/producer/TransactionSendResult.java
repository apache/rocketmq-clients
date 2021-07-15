package org.apache.rocketmq.client.producer;

import lombok.Getter;
import org.apache.rocketmq.client.remoting.RpcTarget;

@Getter
public class TransactionSendResult extends SendResult {
    private final LocalTransactionState localTransactionState;

    public TransactionSendResult(RpcTarget target, String msgId, String transactionId,
                                 LocalTransactionState localTransactionState) {
        super(target, msgId, transactionId);
        this.localTransactionState = localTransactionState;
    }
}
