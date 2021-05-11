package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.conf.BaseConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RemotingException;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.misc.MixAll;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;

public class DefaultMQProducerTest extends BaseConfig {

    private Message initMessage() throws UnsupportedEncodingException {
        return new Message(
                "TestTopic" /* Topic */,
                "TagA" /* Tag */,
                ("Hello RocketMQ ").getBytes(MixAll.DEFAULT_CHARSET) /* Message body */);
    }

    @Test
    public void testStartAndShutdown() throws MQClientException {
        final DefaultMQProducer producer = new DefaultMQProducer(dummyConsumerGroup);
        producer.start();
        producer.shutdown();
    }

    @Test
    public void testSendBeforeStart()
            throws MQBrokerException, RemotingException, InterruptedException,
                   UnsupportedEncodingException {
        final DefaultMQProducer producer = new DefaultMQProducer(dummyConsumerGroup);
        Message msg = initMessage();
        try {
            producer.send(msg);
            Assert.fail();
        } catch (MQClientException e) {
            Assert.assertTrue(e.getErrorMessage().contains("Producer is not started"));
        }
    }

    @Test
    public void testSendAsyncBeforeStart()
            throws UnsupportedEncodingException, RemotingException, InterruptedException {
        final DefaultMQProducer producer = new DefaultMQProducer(dummyConsumerGroup);
        Message msg = initMessage();
        try {
            producer.send(
                    msg,
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                        }

                        @Override
                        public void onException(Throwable e) {
                        }
                    });
            Assert.fail();
        } catch (MQClientException e) {
            Assert.assertTrue(e.getErrorMessage().contains("Producer is not started"));
        }
    }

    @Test
    public void testStartRepeatedly() throws MQClientException {
        final DefaultMQProducer producer = new DefaultMQProducer(dummyConsumerGroup);
        producer.start();
        try {
            producer.start();
            Assert.fail();
        } catch (MQClientException e) {
            Assert.assertTrue(
                    e.getErrorMessage().contains("The producer has attempted to be started before"));
        } finally {
            producer.shutdown();
        }
    }

    @Test
    public void testSetGroupAfterStart() throws MQClientException {
        final DefaultMQProducer producer = new DefaultMQProducer(dummyConsumerGroup);
        producer.start();
        try {
            producer.setProducerGroup(dummyProducerGroup);
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertTrue(
                    e.getMessage().contains("Please set producerGroup before producer started"));
        } finally {
            producer.shutdown();
        }
    }
}
