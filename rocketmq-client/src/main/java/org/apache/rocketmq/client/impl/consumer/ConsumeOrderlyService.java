package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;

public class ConsumeOrderlyService implements ConsumeService {
    private AtomicReference<ServiceState> state = new AtomicReference<ServiceState>(ServiceState.CREATED);
    private DefaultMQPushConsumerImpl impl;
    private MessageListenerOrderly messageListenerOrderly;

    public ConsumeOrderlyService(
            DefaultMQPushConsumerImpl impl, MessageListenerOrderly messageListenerOrderly) {
        this.impl = impl;
        this.messageListenerOrderly = messageListenerOrderly;
    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void dispatch(ProcessQueue processQueue) {
    }

    @Override
    public void submitConsumeTask(
            List<MessageExt> messageExtList, ProcessQueue processQueue, MessageQueue messageQueue) {
    }
}
