package org.apache.rocketmq.client.impl.consumer;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.message.MessageQueue;

@Slf4j
@AllArgsConstructor
public class ConsumeConcurrentlyTask implements Runnable {

    final DefaultMQPushConsumerImpl consumerImpl;
    final Map<MessageQueue, List<MessageExt>> messageExtListTable;

    @Override
    public void run() {
        List<MessageExt> allMessageExtList = new ArrayList<MessageExt>();
        for (Map.Entry<MessageQueue, List<MessageExt>> entry : messageExtListTable.entrySet()) {
            final MessageQueue mq = entry.getKey();
            final List<MessageExt> messageExtList = entry.getValue();

            final String topic = mq.getTopic();
            final RateLimiter rateLimiter = consumerImpl.rateLimiter(topic);
            if (messageExtList.isEmpty()) {
                log.error("[Bug] messageExt list is empty, mq={}", mq);
                continue;
            }
            if (null != rateLimiter) {
                // await acquire the token
                rateLimiter.acquire(messageExtList.size());
            }
            allMessageExtList.addAll(messageExtList);
        }

        // Intercept before message consumption.
        for (MessageExt messageExt : allMessageExtList) {
            final MessageInterceptorContext context =
                    MessageInterceptorContext.builder().attemptTimes(1 + messageExt.getReconsumeTimes()).build();
            consumerImpl.intercept(MessageHookPoint.PRE_MESSAGE_CONSUMPTION, messageExt, context);
        }

        ConsumeStatus status;
        final ConsumeContext consumeContext = new ConsumeContext();
        final ConsumeService consumeService = consumerImpl.getConsumeService();
        final Stopwatch started = Stopwatch.createStarted();
        try {
            status = consumeService.getMessageListener().consume(allMessageExtList, consumeContext);
        } catch (Throwable t) {
            status = ConsumeStatus.ERROR;
            log.error("Business callback raised an exception while consuming message.", t);
        }

        final long elapsed = started.elapsed(TimeUnit.MILLISECONDS);
        final long elapsedPerMessage = elapsed / allMessageExtList.size();
        // Intercept after message consumption.
        for (int i = 0; i < allMessageExtList.size(); i++) {
            final MessageExt messageExt = allMessageExtList.get(i);
            final MessageInterceptorContext context =
                    MessageInterceptorContext.builder()
                                             .attemptTimes(1 + messageExt.getReconsumeTimes())
                                             .duration(elapsedPerMessage)
                                             .timeUnit(TimeUnit.MILLISECONDS)
                                             .messageIndex(i)
                                             .messageBatchSize(allMessageExtList.size())
                                             .status(ConsumeStatus.OK == status ?
                                                     MessageHookPoint.PointStatus.OK :
                                                     MessageHookPoint.PointStatus.ERROR)
                                             .build();
            consumerImpl.intercept(MessageHookPoint.POST_MESSAGE_CONSUMPTION, messageExt, context);
        }

        for (Map.Entry<MessageQueue, List<MessageExt>> entry : messageExtListTable.entrySet()) {
            final MessageQueue mq = entry.getKey();
            final ProcessQueue pq = consumerImpl.processQueue(mq);
            // Process queue has been removed.
            if (null == pq) {
                continue;
            }
            final List<MessageExt> messageList = entry.getValue();
            pq.eraseMessages(messageList, status);
        }
    }
}
