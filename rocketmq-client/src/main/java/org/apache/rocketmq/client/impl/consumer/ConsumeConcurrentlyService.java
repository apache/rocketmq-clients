package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageQueue;

@Slf4j
public class ConsumeConcurrentlyService extends ConsumeService {

    private final int batchMaxSize;

    public ConsumeConcurrentlyService(MessageListener messageListener, MessageInterceptor interceptor,
                                      ThreadPoolExecutor consumptionExecutor, ScheduledExecutorService scheduler,
                                      ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable, int batchMaxSize) {
        super(messageListener, interceptor, consumptionExecutor, scheduler, processQueueTable);
        this.batchMaxSize = batchMaxSize;
    }

    @Override
    public void dispatch0() {
        final List<ProcessQueue> processQueues = new ArrayList<ProcessQueue>(processQueueTable.values());
        // shuffle all process queue in case messages are always consumed firstly in one message queue.
        Collections.shuffle(processQueues);

        int accumulativeSize = 0;

        final Map<MessageQueue, List<MessageExt>> messageExtListTable = new HashMap<MessageQueue, List<MessageExt>>();
        // iterate all process queues to submit consumption task.
        for (ProcessQueue pq : processQueues) {
            List<MessageExt> messageExtList = pq.tryTakeMessages(batchMaxSize - accumulativeSize);
            if (messageExtList.isEmpty()) {
                continue;
            }
            final MessageQueue mq = pq.getMq();
            // add message list to message table.
            messageExtListTable.put(mq, messageExtList);
            accumulativeSize += messageExtList.size();
            if (accumulativeSize >= batchMaxSize) {
                break;
            }
        }
        // aggregate all messages into list.
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (List<MessageExt> list : messageExtListTable.values()) {
            messageExtList.addAll(list);
        }

        // no new message arrived for current round.
        if (messageExtList.isEmpty()) {
            return;
        }

        final ListenableFuture<ConsumeStatus> future = consume(messageExtList);
        Futures.addCallback(future, new FutureCallback<ConsumeStatus>() {
            @Override
            public void onSuccess(ConsumeStatus status) {
                for (Map.Entry<MessageQueue, List<MessageExt>> entry : messageExtListTable.entrySet()) {
                    final MessageQueue mq = entry.getKey();
                    final List<MessageExt> messageExtList = entry.getValue();
                    final ProcessQueue pq = processQueueTable.get(mq);
                    if (null == pq) {
                        continue;
                    }
                    pq.eraseMessages(messageExtList, status);
                }
                ConsumeConcurrentlyService.this.dispatch();
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("[Bug] Exception raised in consumption callback.", t);
                ConsumeConcurrentlyService.this.dispatch();
            }
        });
    }
}
