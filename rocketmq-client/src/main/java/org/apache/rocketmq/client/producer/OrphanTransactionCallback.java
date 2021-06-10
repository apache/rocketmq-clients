package org.apache.rocketmq.client.producer;

import apache.rocketmq.v1.PollOrphanTransactionResponse;

public interface OrphanTransactionCallback {
    void onOrphanTransaction(PollOrphanTransactionResponse response);

    void onError(Throwable t);
}
