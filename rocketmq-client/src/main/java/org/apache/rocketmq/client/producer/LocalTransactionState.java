package org.apache.rocketmq.client.producer;

public enum LocalTransactionState {
  COMMIT_MESSAGE,
  ROLLBACK_MESSAGE,
  UNKNOWN,
}
