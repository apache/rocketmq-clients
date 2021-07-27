package org.apache.rocketmq.client.impl.consumer;

import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageInterceptorContext;

@Slf4j
@AllArgsConstructor
public class ConsumeOrderlyTask implements Runnable {

    final DefaultMQPushConsumerImpl consumerImpl;
    final ProcessQueue pq;
    final List<MessageExt> messageExtList;

    @Override
    public void run() {
        // intercept before message consumption.
        for (MessageExt messageExt : messageExtList) {
            final MessageInterceptorContext context =
                    MessageInterceptorContext.builder().attemptTimes(1 + messageExt.getReconsumeTimes()).build();
            consumerImpl.intercept(MessageHookPoint.PRE_MESSAGE_CONSUMPTION, messageExt, context);
        }

        final ConsumeService consumeService = consumerImpl.getConsumeService();
        ConsumeStatus status;

        final ConsumeContext consumeContext = new ConsumeContext();
        final Stopwatch started = Stopwatch.createStarted();
        try {
            status = consumeService.getMessageListener().consume(messageExtList, consumeContext);
        } catch (Throwable t) {
            status = ConsumeStatus.ERROR;
            log.error("Biz callback raised an exception while consuming fifo messages.", t);
        }

        pq.fifoConsumeTaskOutbound();

        // intercept after message consumption.
        final long elapsed = started.elapsed(TimeUnit.MILLISECONDS);
        final long elapsedPerMessage = elapsed / messageExtList.size();
        for (int i = 0; i < messageExtList.size(); i++) {
            final MessageExt messageExt = messageExtList.get(i);
            final MessageInterceptorContext context =
                    MessageInterceptorContext.builder()
                                             .attemptTimes(1 + messageExt.getReconsumeTimes())
                                             .duration(elapsedPerMessage)
                                             .timeUnit(TimeUnit.MILLISECONDS)
                                             .messageIndex(i)
                                             .messageBatchSize(messageExtList.size())
                                             .status(ConsumeStatus.OK == status ?
                                                     MessageHookPoint.PointStatus.OK :
                                                     MessageHookPoint.PointStatus.ERROR)
                                             .build();
            consumerImpl.intercept(MessageHookPoint.POST_MESSAGE_CONSUMPTION, messageExt, context);
        }

        pq.eraseFifoMessages(messageExtList, status);
        // check if new messages arrived or not.
        consumeService.dispatch();
    }
}
