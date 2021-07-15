package org.apache.rocketmq.client.producer;

import java.util.Collection;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.exception.RemotingException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageBatch;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.remoting.AccessCredential;

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
        impl.setArn(arn);
    }

    public String getProducerGroup() {
        return impl.getGroup();
    }

    public void setProducerGroup(String group) throws MQClientException {
        synchronized (impl) {
            if (ServiceState.CREATED != impl.getState()) {
                throw new MQClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setGroup(group);
        }
    }

    /**
     * Start this producer instance. <strong> Much internal initializing procedures are carried out to
     * make this instance prepared, thus, it's a must to invoke this method before sending messages.
     * </strong>
     *
     * @throws MQClientException if there is any unexpected error.
     */
    public void start() throws MQClientException {
        this.impl.start();
    }

    /**
     * This method shuts down this producer instance and releases related resources.
     */
    public void shutdown() throws MQClientException {
        this.impl.shutdown();
    }

    public void setNamesrvAddr(String namesrvAddr) throws MQClientException {
        synchronized (impl) {
            if (ServiceState.CREATED != impl.getState()) {
                throw new MQClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setNamesrvAddr(namesrvAddr);
        }
    }


    public void setArn(String arn) throws MQClientException {
        synchronized (impl) {
            if (ServiceState.CREATED != impl.getState()) {
                throw new MQClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setArn(arn);
        }
    }

    public void setAccessCredential(AccessCredential accessCredential) throws MQClientException {
        synchronized (impl) {
            if (ServiceState.CREATED != impl.getState()) {
                throw new MQClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setAccessCredential(accessCredential);
        }
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
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public SendResult send(Message msg)
            throws MQClientException, InterruptedException, MQServerException, TimeoutException {
        return this.impl.send(msg);
    }

    /**
     * Same to {@link #send(Message)} with send timeout specified in addition.
     *
     * @param msg     Message to send.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message
     * ID of the message, {@link SendStatus} indicating broker storage/replication status, message
     * queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public SendResult send(Message msg, long timeout)
            throws MQClientException, InterruptedException, MQServerException, TimeoutException {
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
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public void send(Message msg, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.impl.send(msg, sendCallback);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with send timeout specified in addition.
     *
     * @param msg          message to send.
     * @param sendCallback Callback to execute.
     * @param timeout      send timeout.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public void send(Message msg, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.impl.send(msg, sendCallback, timeout);
    }

    /**
     * Similar to <a href="https://en.wikipedia.org/wiki/User_Datagram_Protocol">UDP</a>, this method
     * won't wait for acknowledgement from broker before return. Obviously, it has maximums throughput
     * yet potentials of message loss.
     *
     * @param msg Message to send.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public void sendOneway(Message msg)
            throws MQClientException, RemotingException, InterruptedException {
        this.impl.sendOneway(msg);
    }

    /**
     * Same to {@link #send(Message)} with target message queue specified in addition.
     *
     * @param msg Message to send.
     * @param mq  Target message queue.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message
     * ID of the message, {@link SendStatus} indicating broker storage/replication status, message
     * queue sent to, etc.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public SendResult send(Message msg, MessageQueue mq)
            throws MQClientException, InterruptedException,
                   MQServerException, TimeoutException {
        //        msg.setTopic(withNamespace(msg.getTopic()));
        return this.impl.send(msg);
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
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, MQBrokerException, InterruptedException, MQServerException, TimeoutException {
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
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
            throws MQClientException, InterruptedException, MQServerException, TimeoutException {
        return this.impl.send(msg, selector, arg, timeout);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with message queue selector specified.
     *
     * @param msg          Message to send.
     * @param selector     Message selector through which to get target message queue.
     * @param arg          Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public void send(
            Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.impl.send(msg, selector, arg, sendCallback);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object, SendCallback)} with timeout
     * specified.
     *
     * @param msg          Message to send.
     * @param selector     Message selector through which to get target message queue.
     * @param arg          Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @param timeout      Send timeout.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public void send(
            Message msg,
            MessageQueueSelector selector,
            Object arg,
            SendCallback sendCallback,
            long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.impl.send(msg, selector, arg, sendCallback, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with message queue selector specified.
     *
     * @param msg      Message to send.
     * @param selector Message queue selector, through which to determine target message queue to
     *                 deliver message
     * @param arg      Argument used along with message queue selector.
     * @throws MQClientException    if there is any client error.
     * @throws RemotingException    if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, InterruptedException {
        this.impl.sendOneway(msg, selector, arg);
    }


    public TransactionSendResult sendMessageInTransaction(
            Message msg, LocalTransactionExecuter executor, final Object arg)
            throws MQClientException {
        throw new UnsupportedOperationException();
    }

    public SendResult send(Collection<Message> msgs)
            throws MQClientException, InterruptedException, MQServerException,
                   TimeoutException {
        return this.impl.send(batch(msgs));
    }

    public SendResult send(Collection<Message> msgs, long timeout)
            throws MQClientException, InterruptedException, MQServerException, TimeoutException {
        return this.impl.send(batch(msgs), timeout);
    }

    public void send(Collection<Message> msgs, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.impl.send(batch(msgs), sendCallback, timeout);
    }

    /**
     * Sets an Executor to be used for executing callback methods. If the Executor is not set, will be
     * used.
     *
     * @param callbackExecutor the instance of Executor
     */
    public void setCallbackExecutor(final ThreadPoolExecutor callbackExecutor) throws MQClientException {
        this.impl.setSendCallbackExecutor(callbackExecutor);
    }

    // Not yet implemented.
    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        return null;
    }
}
