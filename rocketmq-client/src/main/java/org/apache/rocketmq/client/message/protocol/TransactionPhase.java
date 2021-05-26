package org.apache.rocketmq.client.message.protocol;

public enum TransactionPhase {
    NOT_APPLICABLE,
    PREPARE,
    COMMIT,
    ROLLBACK;
}
