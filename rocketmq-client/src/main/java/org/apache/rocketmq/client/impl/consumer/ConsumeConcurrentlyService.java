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
        Collections.sort(processQueues, new Comparator<ProcessQueue>() {
            @Override
            public int compare(ProcessQueue o1, ProcessQueue o2) {
                return o2.messagesCacheSize() - o1.messagesCacheSize();
            }
        });

        final int totalBatchMaxSize = consumerImpl.getConsumeMessageBatchMaxSize();
        int actualBatchSize = 0;
        int nextBatchMaxSize = totalBatchMaxSize;
        final Map<MessageQueue, List<MessageExt>> messageExtListTable = new HashMap<MessageQueue, List<MessageExt>>();
        for (ProcessQueue pq : processQueues) {
            final MessageQueue mq = pq.getMq();
            final String topic = mq.getTopic();

            final List<MessageExt> messageExtList = new ArrayList<MessageExt>();
            final RateLimiter rateLimiter = getRateLimiter(topic);
            if (null != rateLimiter) {
                while (pq.messagesCacheSize() > 0 && rateLimiter.tryAcquire() && actualBatchSize < totalBatchMaxSize) {
                    final MessageExt messageExt = pq.takeMessage();
                    if (null == messageExt) {
                        log.info("[Bug] message taken from process queue is null, mq={}", mq);
                        break;
                    }
                    actualBatchSize++;
                    messageExtList.add(messageExt);
                }
            } else {
                // No rate limiter was set.
                messageExtList.addAll(pq.takeMessages(nextBatchMaxSize));
                actualBatchSize += messageExtList.size();
            }
            // No message cached, skip this pq.
            if (messageExtList.isEmpty()) {
                continue;
            }
            List<MessageExt> existedList = messageExtListTable.get(mq);
            if (null == existedList) {
                existedList = new ArrayList<MessageExt>();
                messageExtListTable.put(mq, existedList);
            }
            existedList.addAll(messageExtList);
            if (actualBatchSize >= totalBatchMaxSize) {
                break;
            }
            nextBatchMaxSize = totalBatchMaxSize - actualBatchSize;
        }
        // No new message arrived.
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
