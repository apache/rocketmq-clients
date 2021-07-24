package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.listener.MessageListener;


public class ConsumeOrderlyService extends ConsumeService {
    public ConsumeOrderlyService(MessageListener messageListener) {
        super(messageListener);
    }

    @Override
    public void dispatch(ProcessQueue processQueue) {

    }
}
