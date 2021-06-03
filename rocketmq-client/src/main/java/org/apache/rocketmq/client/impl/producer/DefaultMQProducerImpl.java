package org.apache.rocketmq.client.impl.producer;

import static com.google.protobuf.util.Timestamps.fromMillis;

import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.MessageType;
import apache.rocketmq.v1.ProducerGroup;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SystemAttribute;
import apache.rocketmq.v1.TransactionPhase;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.CommunicationMode;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.constant.SystemProperty;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.exception.RemotingException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.impl.ClientInstanceManager;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageConst;
import org.apache.rocketmq.client.message.MessageIdUtils;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.message.protocol.Encoding;
import org.apache.rocketmq.client.misc.Validators;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.remoting.RpcTarget;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.UtilAll;


@Slf4j
public class DefaultMQProducerImpl implements ProducerObserver {

    public static final int MESSAGE_COMPRESSION_THRESHOLD = 1024 * 1024 * 4;
    public static final int DEFAULT_MESSAGE_COMPRESSION_LEVEL = 5;
    public static final int MESSAGE_COMPRESSION_LEVEL =
            Integer.parseInt(System.getProperty(SystemProperty.MESSAGE_COMPRESSION_LEVEL,
                                                Integer.toString(DEFAULT_MESSAGE_COMPRESSION_LEVEL)));

    private final DefaultMQProducer defaultMQProducer;
    private final ConcurrentMap<String /* topic */, TopicPublishInfo> topicPublishInfoTable;
    private ClientInstance clientInstance;
    private final AtomicReference<ServiceState> state;

    public DefaultMQProducerImpl(DefaultMQProducer defaultMQProducer) {
        this.defaultMQProducer = defaultMQProducer;
        this.topicPublishInfoTable = new ConcurrentHashMap<String, TopicPublishInfo>();

        this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);
    }

    /**
     * Start the producer, not allowed to start producer repeatedly.
     *
     * @throws MQClientException the mq client exception.
     */
    public void start() throws MQClientException {
        log.info("Begin to start the rocketmq producer.");
        final String producerGroup = defaultMQProducer.getGroupName();

        if (!state.compareAndSet(ServiceState.CREATED, ServiceState.STARTING)) {
            throw new MQClientException(
                    "The producer has attempted to be started before, producerGroup=" + producerGroup);
        }

        clientInstance = ClientInstanceManager.getInstance().getClientInstance(defaultMQProducer);

        final boolean registerResult = clientInstance.registerProducerObserver(producerGroup, this);
        if (!registerResult) {
            throw new MQClientException(
                    "The producer group has been created already, please specify another one, producerGroup="
                    + producerGroup);
        } else {
            log.debug("Registered producer observer, producerGroup={}", producerGroup);
        }

        clientInstance.start();
        state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
        log.info("rocketmq producer starts successfully.");
    }

    public void shutdown() throws MQClientException {
        state.compareAndSet(ServiceState.STARTING, ServiceState.STOPPING);
        state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING);
        final ServiceState serviceState = state.get();
        if (ServiceState.STOPPING == serviceState) {
            clientInstance.unregisterProducerObserver(this.getProducerGroup());
            clientInstance.shutdown();
            if (state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED)) {
                log.info("Shutdown DefaultMQProducerImpl successfully");
                return;
            }
        }
        throw new MQClientException("Failed to shutdown producer, state=" + state.get());
    }

    public boolean hasBeenStarted() {
        final ServiceState serviceState = state.get();
        return ServiceState.CREATED != serviceState;
    }


    private SendMessageRequest wrapSendMessageRequest(Message message, MessageQueue mq) {

        final Resource topicResource =
                Resource.newBuilder().setArn(this.getArn()).setName(message.getTopic()).build();

        final Resource groupResource =
                Resource.newBuilder().setName(this.getProducerGroup()).setName(this.getArn()).build();

        final Map<String, String> properties = message.getUserProperties();

        final boolean transactionFlag =
                Boolean.parseBoolean(properties.get(MessageConst.PROPERTY_TRANSACTION_PREPARED));

        final SystemAttribute.Builder systemAttributeBuilder =
                SystemAttribute.newBuilder()
                               .setBornTimestamp(fromMillis(System.currentTimeMillis()))
                               .setPublisherGroup(groupResource)
                               .setMessageId(MessageIdUtils.createUniqID())
                               .setBornHost(UtilAll.getIpv4Address())
                               .setPartitionId(mq.getQueueId());

        Encoding encoding = Encoding.IDENTITY;
        byte[] body = message.getBody();
        if (body.length > MESSAGE_COMPRESSION_THRESHOLD) {
            try {
                body = UtilAll.compressBytesGzip(body, MESSAGE_COMPRESSION_LEVEL);
                encoding = Encoding.GZIP;
            } catch (IOException e) {
                log.warn("Failed to compress message", e);
            }
        }

        switch (encoding) {
            case GZIP:
                systemAttributeBuilder.setBodyEncoding(apache.rocketmq.v1.Encoding.GZIP);
                break;
            case SNAPPY:
                systemAttributeBuilder.setBodyEncoding(apache.rocketmq.v1.Encoding.SNAPPY);
                break;
            case IDENTITY:
            default:
                systemAttributeBuilder.setBodyEncoding(apache.rocketmq.v1.Encoding.IDENTITY);
        }

        if (transactionFlag) {
            systemAttributeBuilder.setTransactionPhase(TransactionPhase.PREPARE);
            systemAttributeBuilder.setMessageType(MessageType.TRANSACTION);
        }

        final SystemAttribute systemAttribute = systemAttributeBuilder.build();

        final apache.rocketmq.v1.Message msg =
                apache.rocketmq.v1.Message.newBuilder()
                                          .setTopic(topicResource)
                                          .setSystemAttribute(systemAttribute)
                                          .putAllUserAttribute(message.getUserProperties())
                                          .setBody(ByteString.copyFrom(body))
                                          .build();

        return SendMessageRequest.newBuilder().setMessage(msg).build();
    }

    private TopicPublishInfo getPublicInfo(String topic) throws MQClientException {
        TopicPublishInfo topicPublishInfo = topicPublishInfoTable.get(topic);
        if (null != topicPublishInfo) {
            return topicPublishInfo;
        }
        TopicRouteData topicRouteData = clientInstance.getTopicRouteInfo(topic);
        topicPublishInfo = new TopicPublishInfo(topic, topicRouteData);
        topicPublishInfoTable.put(topic, topicPublishInfo);
        return topicPublishInfo;
    }

    private MessageQueue selectOneMessageQueue(TopicPublishInfo topicPublishInfo) throws MQClientException {
        final Set<RpcTarget> isolatedTargets = clientInstance.getIsolatedTargets();
        return topicPublishInfo.selectOneMessageQueue(isolatedTargets);
    }

    private int getTotalSendTimes(CommunicationMode mode) {
        int totalSendTimes = 1;
        if (mode == CommunicationMode.SYNC) {
            totalSendTimes += defaultMQProducer.getRetryTimesWhenSendFailed();
        }
        return totalSendTimes;
    }

    boolean isRunning() {
        return ServiceState.STARTED == state.get();
    }

    void ensureRunning() throws MQClientException {
        if (!isRunning()) {
            throw new MQClientException("Producer is not started");
        }
    }

    public SendResult send(Message message)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(message, defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message message, long timeoutMillis)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendDefaultImpl(message, CommunicationMode.SYNC, null, timeoutMillis);
    }

    public SendResult send(Message message, MessageQueue mq)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException,
                   MQServerException {
        return send(message, mq, defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message message, MessageQueue mq, long timeoutMillis)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException,
                   MQServerException {
        ensureRunning();
        return sendKernelImpl(message, mq, CommunicationMode.SYNC, null, timeoutMillis);
    }

    public void send(Message message, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        send(message, sendCallback, defaultMQProducer.getSendMsgTimeout());
    }

    public void send(Message message, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        sendDefaultImpl(message, CommunicationMode.ASYNC, sendCallback, timeout);
    }

    public void send(Message message, MessageQueue mq, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException, MQServerException {
        send(message, mq, sendCallback, defaultMQProducer.getSendMsgTimeout());
    }

    public void send(Message message, MessageQueue mq, SendCallback sendCallback, long timeoutMillis)
            throws MQClientException, RemotingException, InterruptedException, MQServerException {
        ensureRunning();
        sendKernelImpl(message, mq, CommunicationMode.ASYNC, sendCallback, timeoutMillis);
    }

    public void sendOneway(Message message)
            throws MQClientException {
        sendDefaultImpl(
                message, CommunicationMode.ONE_WAY, null, defaultMQProducer.getSendMsgTimeout());
    }

    public void sendOneway(Message message, MessageQueue mq)
            throws MQClientException, MQServerException {
        ensureRunning();
        sendKernelImpl(
                message, mq, CommunicationMode.ONE_WAY, null, defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message message, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(message, selector, arg, defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message message, MessageQueueSelector selector, Object arg, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendSelectImpl(message, selector, arg, CommunicationMode.SYNC, null, timeout);
    }

    public void send(
            Message message, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        send(message, selector, arg, sendCallback, defaultMQProducer.getSendMsgTimeout());
    }

    public void send(
            Message message,
            MessageQueueSelector selector,
            Object arg,
            SendCallback sendCallback,
            long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        sendSelectImpl(message, selector, arg, CommunicationMode.ASYNC, sendCallback, timeout);
    }

    public void sendOneway(Message message, MessageQueueSelector selector, Object arg)
            throws MQClientException {
        sendSelectImpl(
                message,
                selector,
                arg,
                CommunicationMode.ONE_WAY,
                null,
                defaultMQProducer.getSendMsgTimeout());
    }

    private SendResult sendKernelImpl(
            Message message,
            MessageQueue mq,
            CommunicationMode mode,
            SendCallback sendCallback,
            long timeoutMillis)
            throws MQClientException, MQServerException {
        TopicPublishInfo publicInfo = getPublicInfo(message.getTopic());
        final RpcTarget target = publicInfo.resolveRpcTarget(mq.getBrokerName());

        final SendMessageRequest request = wrapSendMessageRequest(message, mq);

        final SendMessageResponse response =
                clientInstance.sendClientAPI(
                        target, mode, request, sendCallback, timeoutMillis, TimeUnit.MILLISECONDS);

        return ClientInstance.processSendResponse(response);
    }

    public SendResult sendDefaultImpl(
            Message message, CommunicationMode mode, SendCallback sendCallback, long timeoutMillis)
            throws MQClientException {
        ensureRunning();
        final String topic = message.getTopic();
        final TopicPublishInfo publicInfo = getPublicInfo(topic);
        final int totalSendTimes = getTotalSendTimes(mode);
        for (int time = 1; time <= totalSendTimes; time++) {
            final MessageQueue messageQueue = selectOneMessageQueue(publicInfo);
            if (null == messageQueue) {
                throw new MQClientException("Failed to select a message queue for sending.");
            }
            try {
                return sendKernelImpl(message, messageQueue, mode, sendCallback, timeoutMillis);
            } catch (Throwable t) {
                log.warn("Exception occurs while sending message, topic={}, time={}.", topic, time, t);

                final RpcTarget target = publicInfo.resolveRpcTarget(messageQueue.getBrokerName());
                target.setIsolated(true);
                log.debug(
                        "Would isolate target for a while cause failed to send message, target={}", target);
            }
        }
        throw new MQClientException("Failed to send message");
    }

    private SendResult sendSelectImpl(
            Message message,
            MessageQueueSelector selector,
            Object arg,
            CommunicationMode mode,
            SendCallback sendCallback,
            long timeoutMillis)
            throws MQClientException {
        ensureRunning();
        Validators.messageCheck(message, defaultMQProducer.getMaxMessageSize());

        final TopicPublishInfo publicInfo = getPublicInfo(message.getTopic());

        SendResult sendResult;
        MessageQueue messageQueue;

        try {
            messageQueue = selector.select(publicInfo.getMessageQueueList(), message, arg);
        } catch (Throwable t) {
            throw new MQClientException("Exception occurs while select message queue.");
        }
        if (null == messageQueue) {
            throw new MQClientException("Message queue is unexpectedly null.");
        }

        final RpcTarget target = publicInfo.resolveRpcTarget(messageQueue.getBrokerName());
        if (!target.isIsolated()) {
            try {
                sendResult = sendKernelImpl(message, messageQueue, mode, sendCallback, timeoutMillis);
                return sendResult;
            } catch (Throwable t) {
                log.warn("Exception occurs while sending message.", t);

                target.setIsolated(true);
                log.debug(
                        "Would isolate target for a while cause failed to send message, target={}", target);
            }
        } else {
            log.warn("Could not select a message queue cause target[{}] is isolated.", target);
        }

        return null;
    }

    // Not implemented yet.
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
    }

    @Override
    public HeartbeatEntry prepareHeartbeatData() {
        Resource groupResource =
                Resource.newBuilder().setArn(this.getArn()).setName(this.getProducerGroup()).build();
        ProducerGroup producerGroup = ProducerGroup.newBuilder().setGroup(groupResource).build();
        return HeartbeatEntry.newBuilder()
                             .setClientId(defaultMQProducer.getClientId())
                             .setProducerGroup(producerGroup)
                             .build();
    }

    @Override
    public void logStats() {
    }

    @Override
    public void onTopicRouteChanged(String topic, TopicRouteData topicRouteData) {
        final TopicPublishInfo topicPublishInfo = topicPublishInfoTable.get(topic);
        // Need filter topic in advance?
        if (null == topicPublishInfo) {
            log.info("No topic publish info, skip updating, topic={}", topic);
            return;
        }
        topicPublishInfo.refreshTopicRoute(topicRouteData);
    }

    private String getProducerGroup() {
        return defaultMQProducer.getProducerGroup();
    }

    private String getArn() {
        return defaultMQProducer.getArn();
    }
}
