package org.apache.rocketmq.client.impl.consumer;

import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.message.MessageQueue;

@Slf4j
@AllArgsConstructor
public class ConsumeConcurrentlyTask implements Runnable {
    final ConsumeConcurrentlyService consumeConcurrentlyService;
    final ProcessQueue processQueue;
    final List<MessageExt> cachedMessages;

    @Override
    public void run() {
        final MessageQueue messageQueue = processQueue.getMessageQueue();
        if (processQueue.isDropped()) {
            log.debug(
                    "Would not consume message because of the drop of ProcessQueue, mq={}", messageQueue.simpleName());
            return;
        }

        final DefaultMQPushConsumerImpl consumerImpl = processQueue.getConsumerImpl();
        // Intercept message while PRE_MESSAGE_CONSUMPTION
        for (MessageExt messageExt : cachedMessages) {
            final MessageInterceptorContext context =
                    MessageInterceptorContext.builder().attemptTimes(1 + messageExt.getReconsumeTimes()).build();
            consumerImpl.interceptMessage(MessageHookPoint.PRE_MESSAGE_CONSUMPTION, messageExt, context);
        }

        ConsumeConcurrentlyContext consumeContext = new ConsumeConcurrentlyContext(messageQueue);
        ConsumeConcurrentlyStatus status;

        final Stopwatch started = Stopwatch.createStarted();
        try {
            status = consumeConcurrentlyService.getMessageListenerConcurrently()
                                               .consumeMessage(cachedMessages, consumeContext);
        } catch (Throwable t) {
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            log.error("Business callback raised an exception while consuming message.", t);
        }

        final long consumptionDuration = started.elapsed(TimeUnit.MILLISECONDS);
        final long consumptionDurationPerMessage = consumptionDuration / cachedMessages.size();
        // Intercept message while POST_MESSAGE_CONSUMPTION.
        for (int i = 0; i < cachedMessages.size(); i++) {
            final MessageExt messageExt = cachedMessages.get(i);
            final MessageInterceptorContext context =
                    MessageInterceptorContext.builder()
                                             .attemptTimes(1 + messageExt.getReconsumeTimes())
                                             .duration(consumptionDurationPerMessage)
                                             .timeUnit(TimeUnit.MILLISECONDS)
                                             .messageIndex(i)
                                             .messageBatchSize(cachedMessages.size())
                                             .status(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status ?
                                                     MessageHookPoint.PointStatus.OK :
                                                     MessageHookPoint.PointStatus.ERROR)
                                             .build();
            consumerImpl.interceptMessage(MessageHookPoint.POST_MESSAGE_CONSUMPTION, messageExt, context);
        }

        processQueue.removeCachedMessages(cachedMessages);

        for (MessageExt messageExt : cachedMessages) {
            switch (status) {
                case CONSUME_SUCCESS:
                    try {
                        consumerImpl.consumeSuccessMsgCount.incrementAndGet();
                        consumerImpl.ackMessage(messageExt);
                    } catch (Throwable t) {
                        log.warn("Failed to ACK message, mq={}, msgId={}", messageQueue.simpleName(),
                                 messageExt.getMsgId(), t);
                    }
                    break;
                case RECONSUME_LATER:
                default:
                    try {
                        consumerImpl.consumeFailureMsgCount.incrementAndGet();
                        consumerImpl.nackMessage(messageExt);
                    } catch (Throwable t) {
                        log.warn("Failed to NACK message, mq={}, msgId={}", messageQueue.simpleName(),
                                 messageExt.getMsgId(), t);
                    }
            }
        }
    }
}
