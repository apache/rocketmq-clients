package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.remoting.AccessCredential;

public class DefaultMQPushConsumer {

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     */
    protected final DefaultMQPushConsumerImpl impl;

    public DefaultMQPushConsumer(final String group) {
        this.impl = new DefaultMQPushConsumerImpl(group);
    }

    public void setConsumerGroup(String group) throws ClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new ClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setGroup(group);
        }
    }

    public String getConsumerGroup() {
        return this.impl.getGroup();
    }

    public void start() throws ClientException {
        this.impl.start();
    }

    public void shutdown() throws ClientException {
        this.impl.shutdown();
    }

    public void setNamesrvAddr(String namesrvAddr) throws ClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new ClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            this.impl.setNamesrvAddr(namesrvAddr);
        }
    }

    // TODO: not allowed to set thead num after start
    public void setConsumeThreadNum(int consumeThreadNum) {
        throw new UnsupportedOperationException();
    }

    public void throttle(String topic, int permitsPerSecond) {
        this.impl.throttle(topic, permitsPerSecond);
    }

    public void registerMessageListener(MessageListenerConcurrently messageListenerConcurrently) {
        this.impl.registerMessageListener(messageListenerConcurrently);
    }

    public void registerMessageListener(MessageListenerOrderly messageListenerOrderly) {
        this.impl.registerMessageListener(messageListenerOrderly);
    }

    public void subscribe(String topic, String subscribeExpression) throws ClientException {
        this.impl.subscribe(topic, subscribeExpression);
    }

    public void unsubscribe(String topic) {
        this.impl.unsubscribe(topic);
    }

    public void setArn(String arn) throws ClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new ClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setArn(arn);
        }
    }

    public void setAccessCredential(AccessCredential accessCredential) throws ClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new ClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setAccessCredential(accessCredential);
        }
    }

    public void setConsumeMessageBatchMaxSize(int batchMaxSize) {
        impl.setConsumeMessageBatchMaxSize(batchMaxSize);
    }

    public void setMaxBatchConsumeWaitTimeMillis(long maxBatchConsumeWaitTimeMillis) {
        impl.setMaxBatchConsumeWaitTimeMillis(maxBatchConsumeWaitTimeMillis);
    }

    public void setMessageModel(MessageModel messageModel) throws ClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new ClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setMessageModel(messageModel);
        }
    }
}
