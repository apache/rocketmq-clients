package org.apache.rocketmq.client.producer;

import lombok.Getter;
import org.apache.rocketmq.client.remoting.Endpoints;

@Getter
public class TransactionSendResult extends SendResult {
    private final LocalTransactionState localTransactionState;

    public TransactionSendResult(Endpoints endpoints, String msgId, String transactionId,
                                 LocalTransactionState localTransactionState) {
        super(endpoints, msgId, transactionId);
        this.localTransactionState = localTransactionState;
    }
}
