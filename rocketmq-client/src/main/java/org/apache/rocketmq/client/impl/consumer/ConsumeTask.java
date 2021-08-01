package org.apache.rocketmq.client.impl.consumer;

import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;

@Slf4j
@AllArgsConstructor
public class ConsumeTask implements Callable<ConsumeStatus> {
    private final MessageInterceptor interceptor;
    private final MessageListener messageListener;
    private final List<MessageExt> messageExtList;

    @Override
    public ConsumeStatus call() {
        // intercept before message consumption.
        for (MessageExt messageExt : messageExtList) {
            final MessageInterceptorContext context =
                    MessageInterceptorContext.builder().attempt(messageExt.getDeliveryAttempt()).build();
            interceptor.intercept(MessageHookPoint.PRE_MESSAGE_CONSUMPTION, messageExt, context);
        }

        ConsumeStatus status;

        final ConsumeContext consumeContext = new ConsumeContext();
        final Stopwatch started = Stopwatch.createStarted();
        try {
            status = messageListener.consume(messageExtList, consumeContext);
        } catch (Throwable t) {
            status = ConsumeStatus.ERROR;
            log.error("Message listener raised an exception while consuming messages.", t);
        }
        if (null == status) {
            log.error("Message listener returns a null pointer for consume status");
            status = ConsumeStatus.ERROR;
        }

        // intercept after message consumption.
        final long elapsed = started.elapsed(TimeUnit.MILLISECONDS);
        final long elapsedPerMessage = elapsed / messageExtList.size();
        for (int i = 0; i < messageExtList.size(); i++) {
            final MessageExt messageExt = messageExtList.get(i);
            final MessageInterceptorContext context =
                    MessageInterceptorContext.builder()
                                             .attempt(messageExt.getDeliveryAttempt())
                                             .duration(elapsedPerMessage)
                                             .timeUnit(TimeUnit.MILLISECONDS)
                                             .messageIndex(i)
                                             .messageBatchSize(messageExtList.size())
                                             .status(ConsumeStatus.OK == status ?
                                                     MessageHookPoint.PointStatus.OK :
                                                     MessageHookPoint.PointStatus.ERROR)
                                             .build();
            interceptor.intercept(MessageHookPoint.POST_MESSAGE_CONSUMPTION, messageExt, context);
        }

        return status;
    }
}
