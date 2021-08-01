package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.conf.BaseConfig;
import org.apache.rocketmq.client.exception.ClientException;
import org.testng.annotations.Test;

public class DefaultMQProducerTest extends BaseConfig {

    @Test
    public void testStartAndShutdown() throws ClientException {
        final DefaultMQProducer producer = new DefaultMQProducer(dummyConsumerGroup);
        producer.start();
        producer.shutdown();
    }
}
