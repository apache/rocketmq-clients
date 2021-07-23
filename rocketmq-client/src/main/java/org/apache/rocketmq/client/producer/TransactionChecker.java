package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.message.MessageExt;

public interface TransactionChecker {
    TransactionResolution check(final MessageExt msg);
}
