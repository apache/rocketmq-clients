package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
public class ConsumeOrderlyService extends ConsumeService {

    public ConsumeOrderlyService(MessageListener messageListener, MessageInterceptor interceptor,
                                 ThreadPoolExecutor consumptionExecutor, ScheduledExecutorService scheduler,
                                 ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable) {
        super(messageListener, interceptor, consumptionExecutor, scheduler, processQueueTable);
    }

    @Override
    public void dispatch0() {
        final List<ProcessQueue> processQueues = new ArrayList<ProcessQueue>(processQueueTable.values());
        Collections.shuffle(processQueues);

        for (final ProcessQueue pq : processQueues) {
            final MessageExt messageExt = pq.tryTakeFifoMessage();
            if (null == messageExt) {
                continue;
            }

            final ListenableFuture<ConsumeStatus> future = consume(messageExt);
            Futures.addCallback(future, new FutureCallback<ConsumeStatus>() {
                @Override
                public void onSuccess(ConsumeStatus status) {
                    pq.eraseFifoMessage(messageExt, status);
                    ConsumeOrderlyService.this.dispatch();
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("[Bug] Exception raised in consumption callback.", t);
                    ConsumeOrderlyService.this.dispatch();
                }
            });
        }
    }
}
