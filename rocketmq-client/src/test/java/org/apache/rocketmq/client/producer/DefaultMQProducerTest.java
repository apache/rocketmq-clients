package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.conf.TestBase;
import org.apache.rocketmq.client.exception.ClientException;
import org.testng.annotations.Test;

public class DefaultMQProducerTest extends TestBase {

    @Test
    public void testStartAndShutdown() throws ClientException {
        final DefaultMQProducer producer = new DefaultMQProducer(dummyConsumerGroup0);
        producer.start();
        producer.shutdown();
    }
}
