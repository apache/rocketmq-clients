package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;

@Slf4j
public class ConsumeOrderlyService extends ConsumeService {

    public ConsumeOrderlyService(DefaultMQPushConsumerImpl consumerImpl, MessageListener messageListener) {
        super(consumerImpl, messageListener);
    }

    @Override
    public void dispatch0() {
        final List<ProcessQueue> processQueues = consumerImpl.processQueueList();
        Collections.shuffle(processQueues);

        for (ProcessQueue pq : processQueues) {
            if (pq.messagesCacheSize() <= 0) {
                continue;
            }
            if (!pq.fifoConsumptionTaskInbound()) {
                continue;
            }

            final MessageQueue mq = pq.getMq();
            final String topic = mq.getTopic();
            final RateLimiter rateLimiter = consumerImpl.rateLimiter(topic);

            final List<MessageExt> messageExtList = new ArrayList<MessageExt>();
            boolean hasMessage = false;
            if (null != rateLimiter) {
                if (pq.messagesCacheSize() > 0 && pq.fifoConsumptionTaskInbound() && rateLimiter.tryAcquire()) {
                    final MessageExt messageExt = pq.tryTakeMessage();
                    if (null == messageExt) {
                        log.error("[Bug] FIFO message taken from process queue is null, mq={}", mq);
                        pq.fifoConsumptionTaskOutbound();
                        break;
                    }
                    messageExtList.add(messageExt);
                }
            } else {
                // no rate limiter was set.
                if (pq.messagesCacheSize() > 0 && pq.fifoConsumptionTaskInbound()) {
                    messageExtList.addAll(pq.tryTakeMessages(totalBatchMaxSize));
                }
            }

            if (messageExtList.isEmpty()) {
                continue;
            }

            final ThreadPoolExecutor consumeExecutor = consumerImpl.getConsumeExecutor();
            final ConsumeOrderlyTask task = new ConsumeOrderlyTask(consumerImpl, pq, messageExtList);

            try {
                consumeExecutor.submit(task);
            } catch (Throwable t) {
                // should never reach here.
                log.error("[Bug] Failed to submit task to consumption thread pool, which may cause congestion.", t);
            }
        }
    }
}
