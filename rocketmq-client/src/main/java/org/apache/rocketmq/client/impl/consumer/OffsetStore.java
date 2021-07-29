package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.message.MessageQueue;

public interface OffsetStore {
    void load();

    void updateOffset(MessageQueue mq, long offset);

    long readOffset(MessageQueue mq);
}
