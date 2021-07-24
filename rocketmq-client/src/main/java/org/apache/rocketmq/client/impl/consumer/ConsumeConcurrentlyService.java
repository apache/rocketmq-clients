package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;

@Slf4j
public class ConsumeConcurrentlyService extends ConsumeService {

    public ConsumeConcurrentlyService(MessageListener messageListener) {
        super(messageListener);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    public void dispatch(ProcessQueue processQueue) {
        final List<MessageExt> cachedMessages = processQueue.peekMessages();

        final DefaultMQPushConsumerImpl consumerImpl = processQueue.getConsumerImpl();

        final int batchMaxSize = consumerImpl.getConsumeMessageBatchMaxSize();
        final int size = cachedMessages.size();
        for (int i = 0; i < size; i += batchMaxSize) {
            final List<MessageExt> splitMessages = cachedMessages.subList(i, Math.min(size, i + batchMaxSize));
            try {
                consumerImpl.getConsumeExecutor().submit(new ConsumeTask(this, processQueue, splitMessages));
            } catch (Throwable t) {
                log.error("Exception raised while submitting consumeTask for mq={}, cached msg count={}, "
                          + "batchMaxSize={}", processQueue.getMessageQueue().simpleName(), size, batchMaxSize, t);
            }
        }
    }
}
