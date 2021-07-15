package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;

public interface ConsumeService {
    void start() throws MQClientException;

    void shutdown();

    void dispatch(ProcessQueue processQueue);

    void submitConsumeTask(
            final List<MessageExt> messageExtList,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue);
}
