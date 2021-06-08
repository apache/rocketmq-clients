package org.apache.rocketmq.client.impl.consumer;

import com.google.common.base.Stopwatch;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.tracing.EventName;
import org.apache.rocketmq.client.tracing.SpanName;
import org.apache.rocketmq.client.tracing.TracingAttribute;
import org.apache.rocketmq.client.tracing.TracingUtility;

@Slf4j
@AllArgsConstructor
public class ConsumeConcurrentlyTask implements Runnable {
    final ConsumeConcurrentlyService consumeConcurrentlyService;
    final ProcessQueue processQueue;
    final List<MessageExt> cachedMessages;

    @Override
    public void run() {
        if (processQueue.isDropped()) {
            log.debug(
                    "Would not consume message because of the drop of ProcessQueue, mq={}",
                    processQueue.getMessageQueue().simpleName());
            return;
        }
        ConsumeConcurrentlyContext context =
                new ConsumeConcurrentlyContext(processQueue.getMessageQueue());
        ConsumeConcurrentlyStatus status;

        final Stopwatch started = Stopwatch.createStarted();
        try {
            status = consumeConcurrentlyService.getMessageListenerConcurrently().consumeMessage(cachedMessages,
                                                                                                context);
        } catch (Throwable t) {
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            log.error("Business callback raised an exception while consuming message.", t);
        }
        final long duration = started.elapsed(TimeUnit.MILLISECONDS);
        final int messageNum = cachedMessages.size();
        final double durationEachMessage = duration * 1.0 / messageNum;
        final long finalEndTimestamp = System.currentTimeMillis();

        final Tracer tracer = processQueue.getTracer();
        if (null != tracer) {
            // Estimate message consuming start timestamp.
            final DefaultMQPushConsumer consumer =
                    processQueue.getConsumerImpl().getDefaultMQPushConsumer();
            final String group = consumer.getConsumerGroup();
            final String arn = consumer.getArn();
            for (int i = 0; i < messageNum; i++) {
                final long startTimestamp = (long) (finalEndTimestamp - (messageNum - i) * durationEachMessage);
                final long endTimestamp = startTimestamp + (long) durationEachMessage;

                final MessageExt cachedMessage = cachedMessages.get(i);
                final String traceContext = cachedMessage.getTraceContext();

                final SpanBuilder spanBuilder =
                        tracer.spanBuilder(SpanName.CONSUME_MSG).setStartTimestamp(startTimestamp,
                                                                                   TimeUnit.MILLISECONDS);
                // Set sending-message's span as parent if it is valid.
                final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
                if (spanContext.isValid()) {
                    spanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
                }
                final Span span = spanBuilder.startSpan();
                // Record message born-timestamp.
                span.addEvent(EventName.MSG_BORN, cachedMessage.getBornTimestamp(), TimeUnit.MILLISECONDS);
                span.setAttribute(TracingAttribute.ARN, arn);
                span.setAttribute(TracingAttribute.TOPIC, cachedMessage.getTopic());
                span.setAttribute(TracingAttribute.MSG_ID, cachedMessage.getMsgId());
                span.setAttribute(TracingAttribute.GROUP, group);
                span.setAttribute(TracingAttribute.TAGS, cachedMessage.getTags());
                span.setAttribute(TracingAttribute.KEYS, cachedMessage.getKeys());
                span.setAttribute(TracingAttribute.RETRY_TIMES, cachedMessage.getReconsumeTimes());
                span.setAttribute(TracingAttribute.MSG_TYPE, cachedMessage.getMsgType().getName());
                if (status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
                    span.setStatus(StatusCode.OK);
                } else {
                    span.setStatus(StatusCode.ERROR);
                }
                span.end(endTimestamp, TimeUnit.MILLISECONDS);
            }
        }

        processQueue.removeCachedMessages(cachedMessages);

        for (MessageExt messageExt : cachedMessages) {
            switch (status) {
                case CONSUME_SUCCESS:
                    try {
                        processQueue.getConsumerImpl().consumeSuccessNum.incrementAndGet();
                        processQueue.ackMessage(messageExt);
                    } catch (Throwable t) {
                        log.warn(
                                "Failed to ACK message, mq={}, msgId={}",
                                processQueue.getMessageQueue().simpleName(),
                                messageExt.getMsgId(),
                                t);
                    }
                    break;
                case RECONSUME_LATER:
                default:
                    try {
                        processQueue.getConsumerImpl().consumeFailureNum.incrementAndGet();
                        processQueue.negativeAckMessage(messageExt);
                    } catch (Throwable t) {
                        log.warn(
                                "Failed to NACK message, mq={}, msgId={}",
                                processQueue.getMessageQueue().simpleName(),
                                messageExt.getMsgId(),
                                t);
                    }
            }
        }
    }
}
