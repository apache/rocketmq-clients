package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class TransactionSendResult extends SendResult {
  private LocalTransactionState localTransactionState;
  private String errorMessage;
  private RuntimeException runtimeException;
}
