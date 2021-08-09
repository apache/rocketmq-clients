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
import org.apache.rocketmq.client.tracing.TracingMessageInterceptor;

public class DefaultMQPushConsumer {

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     */
    protected final DefaultMQPushConsumerImpl impl;

    /**
     * Constructor specifying group.
     *
     * @param group group name.
     */
    public DefaultMQPushConsumer(final String group) {
        this.impl = new DefaultMQPushConsumerImpl(group);
    }

    /**
     * Set name of consumer group.
     *
     * @param group name of consumer group.
     * @throws ClientException if there is any client error.
     */
    public void setConsumerGroup(String group) throws ClientException {
        impl.setGroup(group);
    }

    /**
     * Get name of consumer group.
     *
     * @return name of consumer group.
     */
    public String getConsumerGroup() {
        return this.impl.getGroup();
    }

    /**
     * This method gets internal infrastructure readily to serve. Instances must call this method after configuration.
     *
     * @throws ClientException if there is any client error.
     */
    public void start() throws ClientException {
        this.impl.start();
    }

    /**
     * Shut down this client and releasing underlying resources.
     */
    public void shutdown() {
        this.impl.shutdown();
    }

    /**
     * Set ip address of name server, split multi-address by ';'.
     *
     * <p> e.g: 127.0.0.1:9876;127.0.0.2:9875
     *
     * @param address ip address of name server.
     */
    public void setNamesrvAddr(String address) {
        this.impl.setNamesrvAddr(address);
    }

    /**
     * Enable message tracing. If enabled, <a href="https://opentelemetry.io//">OpenTelemetry</a>
     * would be enabled to record message tracing by span. See {@link TracingMessageInterceptor} for more details.
     *
     * @param enabled message tracing is enabled or not.
     */
    public void setMessageTracingEnabled(boolean enabled) {
        this.impl.setMessageTracingEnabled(enabled);
    }

    /**
     * Set message consumption threads amount.
     *
     * @param amount threads amount.
     */
    public void setConsumptionThreadsAmount(int amount) {
        this.impl.setConsumptionThreadsAmount(amount);
    }

    public void throttle(String topic, double permitsPerSecond) {
        this.impl.throttle(topic, permitsPerSecond);
    }

    /**
     * Register message listener for concurrent consumption.
     *
     * @param listener concurrent message listener.
     */
    public void registerMessageListener(MessageListenerConcurrently listener) {
        this.impl.registerMessageListener(listener);
    }

    /**
     * Register message listener for order consumption.
     *
     * @param listener order message listener.
     */
    public void registerMessageListener(MessageListenerOrderly listener) {
        this.impl.registerMessageListener(listener);
    }

    /**
     * Subscribe topic to consume message, and provide expression of {@link ExpressionType#TAG} to filter message.
     *
     * @param topic      subscribed topic.
     * @param expression expression to filter message.
     */
    public void subscribe(String topic, String expression) {
        this.impl.subscribe(topic, expression, ExpressionType.TAG);
    }

    /**
     * Subscribe topic to consume message, and provide expression to filter message.
     *
     * @param topic          subscribed topic.
     * @param expression     expression to filter message.
     * @param expressionType expression type.
     */
    public void subscribe(String topic, String expression, ExpressionType expressionType) {
        this.impl.subscribe(topic, expression, expressionType);
    }

    /**
     * Unsubscribe topic to consume message, once topic was unsubscribed, it would stop to deliver message of this
     * topic.
     *
     * @param topic name of topic.
     */
    public void unsubscribe(String topic) {
        this.impl.unsubscribe(topic);
    }

    /**
     * Set abstract resource name of consumer.
     *
     * @param arn abstract resource name.
     */
    public void setArn(String arn) {
        this.impl.setArn(arn);
    }

    /**
     * Get abstract resource name of consumer.
     *
     * @return abstract resource name.
     */
    public String getArn() {
        return this.impl.getArn();
    }

    /**
     * Set credentials provider for consumer.
     *
     * @param provider credentials provider.
     */
    public void setCredentialsProvider(CredentialsProvider provider) {
        this.impl.setCredentialsProvider(provider);
    }

    /**
     * Set message consumption max batch size.
     *
     * @param size max batch size.
     */
    public void setConsumeMessageBatchMaxSize(int size) {
        this.impl.setConsumeMessageBatchMaxSize(size);
    }

    public void setMaxAwaitTimeMillisPerQueue(long timeMillis) {
        this.impl.setMaxAwaitTimeMillisPerQueue(timeMillis);
    }

    public void setMaxAwaitBatchSizePerQueue(int timeMillis) {
        this.impl.setMaxAwaitBatchSizePerQueue(timeMillis);
    }

    public void setMessageModel(MessageModel messageModel) {
        this.impl.setMessageModel(messageModel);
    }

    /**
     * Set message max delivery attempt times.
     *
     * @param maxAttempts max attempts.
     */
    public void setMaxDeliveryAttempts(int maxAttempts) {
        this.impl.setMaxDeliveryAttempts(maxAttempts);
    }

    public int getMaxDeliveryAttempts() {
        return this.impl.getMaxDeliveryAttempts();
    }

    /**
     * Set fifo suspend time before the next delivery while encounter failure of consumption.
     *
     * @param time suspend time.
     */
    public void setFifoConsumptionSuspendTimeMillis(long time) {
        this.impl.setFifoConsumptionSuspendTimeMillis(time);
    }

    /**
     * Set max quantity threshold of total cached messages.
     *
     * @param quantity message quantity threshold.
     */
    public void setMaxTotalCachedMessagesQuantityThreshold(int quantity) {
        this.impl.setMaxTotalCachedMessagesQuantityThreshold(quantity);
    }

    /**
     * Set max bytes threshold of bodies of total cached messages.
     *
     * @param bytes bytes of bodies of messages.
     */
    public void setMaxTotalCachedMessageBytesThreshold(int bytes) {
        this.impl.setMaxTotalCachedMessagesBytesThreshold(bytes);
    }

    /**
     * Set timeout of message consumption.
     *
     * @param timeout consumption timeout.
     */
    public void setConsumptionTimeoutMillis(long timeout) {
        this.impl.setConsumptionTimeoutMillis(timeout);
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.impl.setOffsetStore(offsetStore);
    }
}
