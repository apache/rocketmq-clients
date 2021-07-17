package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.MQClientException;
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

    public void setConsumerGroup(String group) throws MQClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new MQClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setGroup(group);
        }
    }

    public String getConsumerGroup() {
        return this.impl.getGroup();
    }

    public void start() throws MQClientException {
        this.impl.start();
    }

    public void shutdown() throws MQClientException {
        this.impl.shutdown();
    }

    public void setNamesrvAddr(String namesrvAddr) throws MQClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new MQClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            this.impl.setNamesrvAddr(namesrvAddr);
        }
    }

    // TODO: not allowed to set thead num after start
    public void setConsumeThreadMax(int consumeThreadMax) {
        throw new UnsupportedOperationException();
    }

    // TODO: not allowed to set thead num after start
    public void setConsumeThreadMin(int consumeThreadMin) {
        throw new UnsupportedOperationException();
    }

    public void registerMessageListener(MessageListenerConcurrently messageListenerConcurrently) {
        this.impl.registerMessageListener(messageListenerConcurrently);
    }

    public void registerMessageListener(MessageListenerOrderly messageListenerOrderly) {
        this.impl.registerMessageListener(messageListenerOrderly);
    }

    public void subscribe(String topic, String subscribeExpression) throws MQClientException {
        this.impl.subscribe(topic, subscribeExpression);
    }

    public void unsubscribe(String topic) {
        this.impl.unsubscribe(topic);
    }

    public void setArn(String arn) throws MQClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new MQClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setArn(arn);
        }
    }

    public void setAccessCredential(AccessCredential accessCredential) throws MQClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new MQClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
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

    public void setMessageModel(MessageModel messageModel) throws MQClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new MQClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setMessageModel(messageModel);
        }
    }
}
