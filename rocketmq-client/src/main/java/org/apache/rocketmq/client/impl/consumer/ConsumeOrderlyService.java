package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
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

        for (final ProcessQueue pq : processQueues) {
            if (pq.messagesCacheSize() <= 0) {
                continue;
            }
            if (!pq.fifoConsumptionInbound()) {
                continue;
            }

            final MessageQueue mq = pq.getMq();
            final String topic = mq.getTopic();
            final RateLimiter rateLimiter = consumerImpl.rateLimiter(topic);

            final MessageExt messageExt;
            if (null != rateLimiter /* has rate limiter. */ &&
                pq.messagesCacheSize() > 0 /* new message arrived. */ &&
                pq.fifoConsumptionInbound() /* allow consumption task inbound. */) {

                // permit not acquired.
                if (!rateLimiter.tryAcquire()) {
                    // consumption task outbound.
                    pq.fifoConsumptionOutbound();
                    // try to iterate next pq.
                    continue;
                }

                messageExt = pq.tryTakeMessage();
                if (null == messageExt) {
                    log.error("[Bug] FIFO message taken from process queue is null, mq={}", mq);
                    // consumption task outbound.
                    pq.fifoConsumptionOutbound();
                    // try to iterate next pq.
                    continue;
                }
            } else {
                // no rate limiter was set.
                if (pq.messagesCacheSize() > 0 && pq.fifoConsumptionInbound()) {
                    messageExt = pq.tryTakeMessage();
                    if (null == messageExt) {
                        log.error("[Bug] FIFO message taken from process queue is null, mq={}", mq);
                        // consumption task outbound.
                        pq.fifoConsumptionOutbound();
                        // try to iterate next pq.
                        continue;
                    }
                } else {
                    // no new message arrived, try to iterate next pq.
                    continue;
                }
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
