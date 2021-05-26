package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.message.MessageQueue;

@Getter
@Setter
public class TransactionSendResult extends SendResult {
    private LocalTransactionState localTransactionState;
    private String errorMessage;
    private RuntimeException runtimeException;

    public TransactionSendResult(
            SendStatus sendStatus,
            String msgId,
            MessageQueue messageQueue,
            long queueOffset,
            String transactionId) {
        super(msgId);
    }
}
