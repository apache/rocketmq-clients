package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.message.Message;

public interface LocalTransactionExecutor {
  LocalTransactionState executeLocalTransactionBranch(final Message msg, final Object arg);
}
