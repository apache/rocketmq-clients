package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;

@Slf4j
public class ConsumeConcurrentlyService extends ConsumeService {

    public ConsumeConcurrentlyService(DefaultMQPushConsumerImpl consumerImpl, MessageListener messageListener) {
        super(consumerImpl, messageListener);
    }

    @Override
    public void dispatch0() {
        final List<ProcessQueue> processQueues = consumerImpl.processQueueList();
        // shuffle all process queue in case messages are always consumed firstly in one message queue.
        Collections.shuffle(processQueues);

        final int totalBatchMaxSize = consumerImpl.getConsumeMessageBatchMaxSize();
        int nextBatchMaxSize = totalBatchMaxSize;
        int actualBatchSize = 0;

        final Map<MessageQueue, List<MessageExt>> messageExtListTable = new HashMap<MessageQueue, List<MessageExt>>();
        // iterate all process queues to submit consumption task.
        for (ProcessQueue pq : processQueues) {
            final List<MessageExt> messageExtList = new ArrayList<MessageExt>();

            // get rate limiter for each topic.
            final MessageQueue mq = pq.getMq();
            final String topic = mq.getTopic();
            final RateLimiter rateLimiter = consumerImpl.rateLimiter(topic);

            if (null != rateLimiter) {
                while (pq.messagesCacheSize() > 0 && actualBatchSize < totalBatchMaxSize && rateLimiter.tryAcquire()) {
                    final MessageExt messageExt = pq.tryTakeMessage();
                    if (null == messageExt) {
                        log.error("[Bug] message taken from process queue is null, mq={}", mq);
                        break;
                    }
                    actualBatchSize++;
                    messageExtList.add(messageExt);
                }
            } else {
                // no rate limiter was set.
                messageExtList.addAll(pq.tryTakeMessages(nextBatchMaxSize));
                actualBatchSize += messageExtList.size();
            }

            // no messages cached, skip this pq.
            if (messageExtList.isEmpty()) {
                continue;
            }

            // add message to message table.
            List<MessageExt> existedList = messageExtListTable.get(mq);
            if (null == existedList) {
                existedList = new ArrayList<MessageExt>();
                messageExtListTable.put(mq, existedList);
            }
            existedList.addAll(messageExtList);

            // actual batch size exceeds the max, prepare to submit them to consume.
            if (actualBatchSize >= totalBatchMaxSize) {
                break;
            }
            // calculate the max batch for the next pq.
            nextBatchMaxSize = totalBatchMaxSize - actualBatchSize;
        }
        // no new message arrived for current round.
        if (actualBatchSize <= 0) {
            return;
        }
        final ThreadPoolExecutor consumeExecutor = consumerImpl.getConsumeExecutor();
        final ConsumeConcurrentlyTask task = new ConsumeConcurrentlyTask(consumerImpl, messageExtListTable);
        try {
            consumeExecutor.submit(task);
        } catch (Throwable t) {
            // should never reach here.
            log.error("[Bug] Failed to submit task to consumption thread pool, which may cause congestion.", t);
        }
    }
}
