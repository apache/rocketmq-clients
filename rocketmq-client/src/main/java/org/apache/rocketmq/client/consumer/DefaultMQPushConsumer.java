/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.consumer.filter.ExpressionType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.OffsetStore;
import org.apache.rocketmq.client.remoting.CredentialsProvider;

public class DefaultMQPushConsumer {

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     */
    protected final DefaultMQPushConsumerImpl impl;

    public DefaultMQPushConsumer(final String group) {
        this.impl = new DefaultMQPushConsumerImpl(group);
    }

    public void setConsumerGroup(String group) throws ClientException {
        impl.setGroup(group);
    }

    public String getConsumerGroup() {
        return this.impl.getGroup();
    }

    public void start() throws ClientException {
        this.impl.start();
    }

    public void shutdown() {
        this.impl.shutdown();
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.impl.setNamesrvAddr(namesrvAddr);
    }

    public void setMessageTracingEnabled(boolean enabled) {
        this.impl.setMessageTracingEnabled(enabled);
    }

    // TODO: not allowed to set thead num after start
    public void setConsumeThreadNum(int consumeThreadNum) {
        throw new UnsupportedOperationException();
    }

    public void throttle(String topic, double permitsPerSecond) {
        this.impl.throttle(topic, permitsPerSecond);
    }

    public void registerMessageListener(MessageListenerConcurrently listener) {
        this.impl.registerMessageListener(listener);
    }

    public void registerMessageListener(MessageListenerOrderly listener) {
        this.impl.registerMessageListener(listener);
    }

    public void subscribe(String topic, String expression) {
        this.impl.subscribe(topic, expression, ExpressionType.TAG);
    }

    public void subscribe(String topic, String expression, ExpressionType expressionType) {
        this.impl.subscribe(topic, expression, expressionType);
    }

    public void unsubscribe(String topic) {
        this.impl.unsubscribe(topic);
    }

    public void setArn(String arn) {
        this.impl.setArn(arn);
    }

    public void setCredentialsProvider(CredentialsProvider provider) {
        this.impl.setCredentialsProvider(provider);
    }

    public void setConsumeMessageBatchMaxSize(int maxSize) {
        this.impl.setConsumeMessageBatchMaxSize(maxSize);
    }

    public void setMaxBatchConsumeWaitTimeMillis(long timeMillis) {
        this.impl.setMaxAwaitTimeMillisPerQueue(timeMillis);
    }

    public void setMessageModel(MessageModel messageModel) {
        this.impl.setMessageModel(messageModel);
    }

    public void setMaxDeliveryAttempts(int maxAttempts) {
        this.impl.setMaxDeliveryAttempts(maxAttempts);
    }

    public int getMaxDeliveryAttempts() {
        return this.impl.getMaxDeliveryAttempts();
    }

    public void setFifoConsumptionSuspendTimeMillis(long timeMillis) {
        this.impl.setFifoConsumptionSuspendTimeMillis(timeMillis);
    }

    public void setMaxTotalCachedMessagesQuantityThreshold(int quantity) {
        this.impl.setMaxTotalCachedMessagesQuantityThreshold(quantity);
    }

    public void setMaxTotalCachedMessageBytesThreshold(int bytes) {
        this.impl.setMaxTotalCachedMessagesBytesThreshold(bytes);
    }

    public void setConsumptionTimeoutMillis(long timeoutMillis) {
        this.impl.setConsumptionTimeoutMillis(timeoutMillis);
    }

    public void setConsumptionThreadsAmount(int threadsAmount) {
        this.impl.setConsumptionThreadsAmount(threadsAmount);
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.impl.setOffsetStore(offsetStore);
    }

    public void setMaxAwaitTimeMillisPerQueue(long timeMillis) {
        this.impl.setMaxAwaitTimeMillisPerQueue(timeMillis);
    }

    public void setMaxAwaitBatchSizePerQueue(int timeMillis) {
        this.impl.setMaxAwaitBatchSizePerQueue(timeMillis);
    }
}
