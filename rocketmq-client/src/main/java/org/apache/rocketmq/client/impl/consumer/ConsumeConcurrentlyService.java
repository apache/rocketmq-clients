package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.utility.ThreadFactoryImpl;

@Slf4j
public class ConsumeConcurrentlyService implements ConsumeService {
    private final AtomicReference<ServiceState> state;
    private final DefaultMQPushConsumerImpl consumerImpl;

    @Getter
    private final MessageListenerConcurrently messageListenerConcurrently;
    private final ThreadPoolExecutor consumeExecutor;

    public ConsumeConcurrentlyService(
            DefaultMQPushConsumerImpl consumerImpl,
            MessageListenerConcurrently messageListenerConcurrently) {
        this.state = new AtomicReference<ServiceState>(ServiceState.STARTING);
        this.messageListenerConcurrently = messageListenerConcurrently;
        this.consumerImpl = consumerImpl;
        this.consumeExecutor =
                new ThreadPoolExecutor(
                        consumerImpl.getConsumeThreadMin(),
                        consumerImpl.getConsumeThreadMax(),
                        1000 * 60,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        new ThreadFactoryImpl("ConsumeMessageThread"));
    }

    @Override
    public void start() throws ClientException {
        synchronized (this) {
            log.info("Begin to start the consume concurrently service.");
            if (!state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED)) {
                log.warn("The consume concurrently service has been started before");
            }
        }
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            log.info("Begin to shutdown the consume concurrently service.");
            if (!state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING)) {
                log.warn("The consume concurrently service has not been started before");
                return;
            }
            consumeExecutor.shutdown();
            state.compareAndSet(ServiceState.STOPPING, ServiceState.STARTING);
            log.info("Shutdown the consume concurrently service successfully.");
        }
    }

    @Override
    public void dispatch(ProcessQueue processQueue) {
        final List<MessageExt> cachedMessages = processQueue.getCachedMessages();
        final int batchMaxSize = consumerImpl.getConsumeMessageBatchMaxSize();
        final int size = cachedMessages.size();
        for (int i = 0; i < size; i += batchMaxSize) {
            final List<MessageExt> splitMessages =
                    cachedMessages.subList(i, Math.min(size, i + batchMaxSize));
            final ConsumeConcurrentlyTask task = new ConsumeConcurrentlyTask(this, processQueue, splitMessages);
            try {
                consumeExecutor.submit(task);
            } catch (Throwable t) {
                log.error(
                        "Exception raised while submitting consumeTask for mq={}, cached msg count={}, batchMaxSize={}",
                        processQueue.getMessageQueue().simpleName(), size, batchMaxSize, t);
            }
        }
    }

    @Override
    public ListenableFuture<ConsumeStatus> verifyConsumption(final MessageExt messageExt,
                                                             final Partition partition) {
        final SettableFuture<ConsumeStatus> future0 = SettableFuture.create();
        try {
            consumeExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    final ArrayList<MessageExt> messageList = new ArrayList<MessageExt>();
                    messageList.add(messageExt);
                    final MessageQueue messageQueue = new MessageQueue(partition);
                    final ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
                    try {
                        ConsumeStatus consumeStatus = messageListenerConcurrently.consumeMessage(messageList, context);
                        future0.set(consumeStatus);
                    } catch (Throwable t) {
                        log.error("Exception occurs while verification of message consumption, topic={}, "
                                  + "messageId={}", messageExt.getTopic(), messageExt.getMsgId());
                        future0.setException(t);
                    }
                }
            });
        } catch (Throwable t) {
            log.error("Failed to submit task for verification of message consumption, topic={}, messageId={}",
                      messageExt.getTopic(), messageExt.getMsgId());
            future0.setException(t);
        }
        return future0;
    }
}
