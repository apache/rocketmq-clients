package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.listener.MessageListener;


public class ConsumeOrderlyService extends ConsumeService {
    public ConsumeOrderlyService(DefaultMQPushConsumerImpl consumerImpl, MessageListener messageListener) {
        super(consumerImpl, messageListener);
    }

    @Override
    public void dispatch() {

    }
}
