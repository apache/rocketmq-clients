package org.apache.rocketmq.client.producer;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.remoting.CredentialsProvider;

public class DefaultMQProducer {

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     */
    protected final DefaultMQProducerImpl impl;

    /**
     * Constructor specifying producer group.
     *
     * @param group Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String group) {
        this.impl = new DefaultMQProducerImpl(group);
    }

    /**
     * Constructor specifying arn and producer group.
     *
     * @param arn   Means abstract resource namespace,
     * @param group Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String arn, final String group) {
        this(group);
        this.impl.setArn(arn);
    }

    public String getProducerGroup() {
        return this.impl.getGroup();
    }

    public void setProducerGroup(String group) {
        this.impl.setGroup(group);
    }

    /**
     * Start this producer instance. <strong> Much internal initializing procedures are carried out to
     * make this instance prepared, thus, it's a must to invoke this method before sending messages.
     * </strong>
     *
     * @throws ClientException if there is any unexpected error.
     */
    public void start() throws ClientException {
        this.impl.start();
    }

    /**
     * This method shuts down this producer instance and releases related resources.
     */
    public void shutdown() {
        this.impl.shutdown();
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.impl.setNamesrvAddr(namesrvAddr);
    }

    public void setArn(String arn) {
        this.impl.setArn(arn);
    }

    public void setCredentialsProvider(CredentialsProvider provider) {
        this.impl.setCredentialsProvider(provider);
    }

    public void setMessageTracingEnabled(boolean tracingEnabled) {
        this.impl.setMessageTracingEnabled(tracingEnabled);
    }

    public void setSendMessageTimeoutMillis(long timeoutMillis) {
        this.impl.setSendMessageTimeoutMillis(timeoutMillis);
    }

    /**
     * Send message in synchronous mode. This method returns only when the sending procedure totally
     * completes. <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal
     * implementation will retry before claiming failure. As a result, multiple messages may
     * potentially delivered to broker(s). It's up to the application developers to resolve potential
     * duplication issue.
     *
     * @param msg Message to send.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message
     * ID of the message, {@link SendStatus} indicating broker storage/replication status, message
     * queue sent to, etc.
     * @throws ClientException    if there is any client error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public SendResult send(Message msg) throws ClientException, InterruptedException, ServerException,
                                               TimeoutException {
        return this.impl.send(msg);
    }

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
     * @throws ClientException    if there is any client error.
     * @throws InterruptedException if the sending thread is interrupted.
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
     * @throws ClientException    if there is any client error.
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
     * @throws ClientException    if there is any client error.
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
     * @throws ClientException    if there is any client error.
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
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message
     * ID of the message, {@link SendStatus} indicating broker storage/replication status, message
     * queue sent to, etc.
     * @throws ClientException    if there is any client error.
     * @throws InterruptedException if the sending thread is interrupted.
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
     * @throws ClientException    if there is any client error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
            throws ClientException, InterruptedException, ServerException, TimeoutException {
        return this.impl.send(msg, selector, arg, timeout);
    }

    /**
     * Sets an Executor to be used for executing callback methods. If the Executor is not set, will be
     * used.
     *
     * @param callbackExecutor the instance of Executor
     */
    public void setCallbackExecutor(final ExecutorService executor) {
        this.impl.setCallbackExecutor(executor);
    }

    public Transaction prepare(Message message) throws ServerException, InterruptedException,
                                                           ClientException, TimeoutException {
        return impl.prepare(message);
    }

    public void setTransactionChecker(final TransactionChecker checker) {
        checkNotNull(checker);
        this.impl.setTransactionChecker(checker);
    }


}
