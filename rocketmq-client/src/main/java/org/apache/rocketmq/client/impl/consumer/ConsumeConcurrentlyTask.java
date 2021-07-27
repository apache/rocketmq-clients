package org.apache.rocketmq.client.impl.consumer;

import com.google.common.base.Stopwatch;
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
        for (List<MessageExt> messageExtList : messageExtListTable.values()) {
            allMessageExtList.addAll(messageExtList);
        }
        // Intercept before message consumption.
        for (MessageExt messageExt : allMessageExtList) {
            final MessageInterceptorContext context =
                    MessageInterceptorContext.builder().attemptTimes(1 + messageExt.getReconsumeTimes()).build();
            consumerImpl.intercept(MessageHookPoint.PRE_MESSAGE_CONSUMPTION, messageExt, context);
        }

        final ConsumeService consumeService = consumerImpl.getConsumeService();
        ConsumeStatus status;

        final ConsumeContext consumeContext = new ConsumeContext();
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
            // process queue has been removed.
            if (null == pq) {
                continue;
            }
            final List<MessageExt> messageList = entry.getValue();
            pq.eraseMessages(messageList, status);
        }
        // check if new messages arrived or not.
        consumeService.dispatch();
    }
}
