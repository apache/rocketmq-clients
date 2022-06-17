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

package org.apache.rocketmq.client.producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.producer.ProducerImpl;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.remoting.CredentialsProvider;
import org.apache.rocketmq.client.trace.TracingMessageInterceptor;

/**
 * This class is the entry point for applications intending to send messages.
 *
 * <p>It's fine to tune fields which exposes getter/setter methods, but keep in mind, all of them should work well
 * out of box for most scenarios.
 *
 * <p>This class aggregates various <code>send</code> methods to deliver messages to servers. Each of them has pros and
 * cons; you'd better understand strengths and weakness of them before actually coding.
 */
@ThreadSafe
public class DefaultMQProducer {

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     */
    protected final ProducerImpl impl;

    /**
     * Constructor specifying group.
     *
     * @param group group name.
     * @throws ClientException if there is any client error.
     */
    public DefaultMQProducer(final String group) throws ClientException {
        this.impl = new ProducerImpl(group);
    }

    /**
     * Set name of producer group.
     *
     * @param group name of producer group.
     * @throws ClientException if there is any client error.
     */
    public void setProducerGroup(String group) throws ClientException {
        this.impl.setGroup(group);
    }

    /**
     * Get name of producer group.
     *
     * @return name of producer group.
     */
    public String getProducerGroup() {
        return this.impl.getGroup();
    }

    /**
     * Start this producer instance. <strong> Much internal initializing procedures are carried out to
     * make this instance prepared, thus, it's a must to invoke this method before sending messages.
     * </strong>
     */
    public void start() {
        this.impl.start();
    }

    /**
     * This method shuts down this producer instance and releases related resources.
     */
    public void shutdown() {
        this.impl.shutdown();
    }

    /**
     * Set address of name server.
     *
     * <p> 1. Example usage for ip list. 127.0.0.1:9876[;127.0.0.2:9875]
     * <p> 2. Example usage for domain name: http://MQ_INST_197328126_BXmPlOA6.mq-aone.mq-internal.aliyuncs.com:8081
     *
     * @param address address of name server.
     * @throws ClientException if there is any client error.
     */
    public void setNamesrvAddr(String address) throws ClientException {
        this.impl.setNamesrvAddr(address);
    }

    /**
     * Set abstract resource namespace of producer.
     *
     * @param namespace abstract resource namespace.
     */
    public void setNamespace(String namespace) {
        this.impl.setNamespace(namespace);
    }

    /**
     * Get abstract resource name of producer.
     *
     * @return abstract resource name.
     */
    public String getNamespace() {
        return this.impl.getNamespace();
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
     * Enable message tracing. If enabled, <a href="https://opentelemetry.io">OpenTelemetry</a>
     * would be enabled to record message tracing by span. See {@link TracingMessageInterceptor} for more details.
     *
     * @param enabled message tracing is enabled or not.
     */
    public void setMessageTracingEnabled(boolean enabled) {
        this.impl.setTracingEnabled(enabled);
    }

    public boolean getMessageTracingEnabled() {
        return this.impl.getTracingEnabled();
    }

    /**
     * Set sending timeout for each message.
     *
     * @param timeout sending timeout.
     */
    public void setSendMessageTimeoutMillis(long timeout) {
        this.impl.setSendMessageTimeoutMillis(timeout);
    }

    /**
     * Get sending timeout for each message.
     *
     * @return sending timeout.
     */
    public long getSendMessageTimeoutMillis() {
        return this.impl.getSendMessageTimeoutMillis();
    }

    /**
     * Send message in synchronous mode. This method returns only when the sending procedure totally
     * completes. <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal
     * implementation will retry before claiming failure. As a result, multiple messages may
     * potentially be delivered to broker(s). It's up to the application developers to resolve potential
     * duplication issue.
     *
     * @param msg Message to send.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message
     * ID of the message, {@link SendStatus} indicating broker storage/replication status, message
     * queue sent to, etc.
     * @throws ClientException      if there is any client error.
     * @throws InterruptedException if the sending thread is interrupted.
     * @throws ServerException      if there is any server error.
     * @throws TimeoutException     if there is any timeout error.
     */
    public SendResult send(Message msg) throws ClientException, InterruptedException, ServerException,
                                               TimeoutException {
        return this.impl.send(msg);
    }

    /**
     * Send {@link MessageType#FIFO} with message group in synchronous mode. message in the same message group would
     * be considered as orderly.
     *
     * @param msg          FIFO Message to send.
     * @param messageGroup group of message.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message
     * ID of the message, {@link SendStatus} indicating broker storage/replication status, message
     * queue sent to, etc.
     * @throws ServerException      if there is any client error.
     * @throws ClientException      if the sending thread is interrupted.
     * @throws InterruptedException if there is any server error.
     * @throws TimeoutException     if there is any timeout error.
     */
    public SendResult send(Message msg, String messageGroup) throws ServerException, ClientException,
                                                                    InterruptedException, TimeoutException {
        return this.impl.send(msg, messageGroup);
    }

    /**
     * Same to {@link #send(Message)} with send timeout specified in addition.
     *
     * @param msg     Message to send.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message
     * ID of the message, {@link SendStatus} indicating broker storage/replication status, message
     * queue sent to, etc.
     * @throws ClientException      if there is any client error.
     * @throws InterruptedException if the sending thread is interrupted.
     * @throws ServerException      if there is any server error.
     * @throws TimeoutException     if there is any timeout error.
     */
    public SendResult send(Message msg, long timeout)
            throws ClientException, InterruptedException, ServerException, TimeoutException {
        return this.impl.send(msg, timeout);
    }

    /**
     * Send message to broker asynchronously.
     *
     * <p>This method returns immediately. On sending completion, <code>sendCallback</code> will be
     * executed.
     *
     * <p>Similar to {@link #send(Message)}, internal implementation would potentially retry before
     * claiming sending failure, which may yield message duplication and application developers are
     * the one to resolve this potential issue.
     *
     * @param msg          Message to send.
     * @param sendCallback Callback to execute on sending completed, either successful or
     *                     unsuccessful.
     * @throws ClientException      if there is any client error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public void send(Message msg, SendCallback sendCallback)
            throws ClientException, InterruptedException {
        this.impl.send(msg, sendCallback);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with send timeout specified in addition.
     *
     * @param msg          message to send.
     * @param sendCallback Callback to execute.
     * @param timeout      send timeout.
     * @throws ClientException      if there is any client error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public void send(Message msg, SendCallback sendCallback, long timeout)
            throws ClientException, InterruptedException {
        this.impl.send(msg, sendCallback, timeout);
    }

    /**
     * Similar to <a href="https://en.wikipedia.org/wiki/User_Datagram_Protocol">UDP</a>, this method
     * won't wait for acknowledgement from broker before return. Obviously, it has maximums throughput
     * yet potentials of message loss.
     *
     * @param msg Message to send.
     * @throws ClientException if there is any client error.
     */
    public void sendOneway(Message msg) throws ClientException {
        this.impl.sendOneway(msg);
    }

    /**
     * Same to {@link #send(Message)} with message queue selector specified.
     *
     * @param msg      Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver
     *                 message to.
     * @param arg      Argument to work along with message queue selector.
     * @return {@link SendResult} Inform senders details of the deliverable.
     * @throws ClientException      if there is any client error.
     * @throws InterruptedException if the sending thread is interrupted.
     * @throws ServerException      if there is any server error.
     * @throws TimeoutException     if there is any timeout error.
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
            throws ClientException, InterruptedException, ServerException, TimeoutException {
        return this.impl.send(msg, selector, arg);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object)} with send timeout specified.
     *
     * @param msg      Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver
     *                 message to.
     * @param arg      Argument to work along with message queue selector.
     * @param timeout  Send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message
     * ID of the message, {@link SendStatus} indicating broker storage/replication status, message
     * queue sent to, etc.
     * @throws ClientException      if there is any client error.
     * @throws InterruptedException if the sending thread is interrupted.
     * @throws ServerException      if there is any server error.
     * @throws TimeoutException     if there is any timeout error.
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
            throws ClientException, InterruptedException, ServerException, TimeoutException {
        return this.impl.send(msg, selector, arg, timeout);
    }

    /**
     * Sets an Executor to be used for executing callback methods. If the Executor is not set, will be
     * used.
     *
     * @param executor the instance of Executor.
     */
    public void setCallbackExecutor(final ExecutorService executor) {
        this.impl.setCallbackExecutor(executor);
    }

    public Transaction prepare(Message message) throws ServerException, InterruptedException,
                                                       ClientException, TimeoutException {
        return impl.prepare(message);
    }

    public void setTransactionChecker(final TransactionChecker checker) {
        this.impl.setTransactionChecker(checker);
    }

    public void setTransactionRecoverDelayMillis(final long delayMillis) {
        this.impl.setTransactionRecoverDelayMillis(delayMillis);
    }

    public long getTransactionRecoverDelayMillis() {
        return this.impl.getTransactionRecoverDelayMillis();
    }
}
