package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.utility.ThreadFactoryImpl;

@Slf4j
public class ConsumeConcurrentlyService extends ConsumeService {

    private final Object dispatcherConditionVariable;
    private final ThreadPoolExecutor dispatcherExecutor;

    public ConsumeConcurrentlyService(DefaultMQPushConsumerImpl consumerImpl, MessageListener messageListener) {
        super(consumerImpl, messageListener);
        this.dispatcherConditionVariable = new Object();
        this.dispatcherExecutor = new ThreadPoolExecutor(
                1,
                1,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("ConsumeDispatcherThread"));
    }

    @Override
    public void start() {
        super.start();
        dispatcherExecutor.submit(new Runnable() {
            @Override
            public void run() {
                while (ServiceState.STARTED == state.get()) {
                    try {
                        dispatch0();
                        synchronized (dispatcherConditionVariable) {
                            dispatcherConditionVariable.wait(1000);
                        }
                    } catch (Throwable t) {
                        log.error("Exception raised while schedule message consumption dispatcher", t);
                    }
                }
            }
        });
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.dispatcherExecutor.shutdown();
    }

    public void dispatch0() {
        final List<ProcessQueue> processQueues = consumerImpl.processQueueList();
        // order all process queues descend by their cached message size.
        Collections.sort(processQueues, new Comparator<ProcessQueue>() {
            @Override
            public int compare(ProcessQueue o1, ProcessQueue o2) {
                return o2.messagesCacheSize() - o1.messagesCacheSize();
            }
        });

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
            final RateLimiter rateLimiter = getRateLimiter(topic);

            if (null != rateLimiter) {
                while (pq.messagesCacheSize() > 0 && actualBatchSize < totalBatchMaxSize && rateLimiter.tryAcquire()) {
                    final MessageExt messageExt = pq.tryTakeMessage();
                    if (null == messageExt) {
                        log.info("[Bug] message taken from process queue is null, mq={}", mq);
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
            // Should never reach here.
            log.error("[Bug] Failed to submit task to consume thread pool, which may cause consumption congestion.", t);
        }
        // not all messages are dispatched, start a new round.
        if (consumerImpl.messagesCachedSize() > 0) {
            dispatch0();
        }
    }

    @Override
    public void dispatch() {
        synchronized (dispatcherConditionVariable) {
            dispatcherConditionVariable.notify();
        }
    }
}
