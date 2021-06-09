package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.message.Message;

public interface LocalTransactionExecuter {
    LocalTransactionState executeLocalTransactionBranch(Message message, Object object);
}
