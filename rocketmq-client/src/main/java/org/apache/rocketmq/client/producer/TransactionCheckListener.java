package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.message.MessageExt;

public interface TransactionCheckListener {
  LocalTransactionState checkLocalTransactionState(final MessageExt msg);
}
