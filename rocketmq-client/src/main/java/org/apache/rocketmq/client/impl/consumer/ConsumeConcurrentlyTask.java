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
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;
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
        final MessageQueue messageQueue = processQueue.getMessageQueue();
        if (processQueue.isDropped()) {
            log.debug(
                    "Would not consume message because of the drop of ProcessQueue, mq={}", messageQueue.simpleName());
            return;
        }

        recordMessageWaitingConsumptionSpan();

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
        ConsumeConcurrentlyStatus status;

        final Stopwatch started = Stopwatch.createStarted();
        try {
            status = consumeConcurrentlyService.getMessageListenerConcurrently()
                                               .consumeMessage(cachedMessages, context);
        } catch (Throwable t) {
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            log.error("Business callback raised an exception while consuming message.", t);
        }
        // Record trace of CONSUME_MESSAGE.
        final long consumptionDuration = started.elapsed(TimeUnit.MILLISECONDS);

        recordConsumeMessageSpan(consumptionDuration, status);

        processQueue.removeCachedMessages(cachedMessages);

        final DefaultMQPushConsumerImpl consumerImpl = processQueue.getConsumerImpl();
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

    private void recordConsumeMessageSpan(long consumptionDuration, ConsumeConcurrentlyStatus status) {
        final Tracer tracer = processQueue.getTracer();
        final DefaultMQPushConsumerImpl consumerImpl = processQueue.getConsumerImpl();
        if (null == tracer || !consumerImpl.isMessageTracingEnabled()) {
            return;
        }
        final int messageNum = cachedMessages.size();
        final double durationEachMessage = consumptionDuration * 1.0 / messageNum;
        final long finalEndTimestamp = System.currentTimeMillis();
        for (int i = 0; i < messageNum; i++) {
            final long startTimestamp = (long) (finalEndTimestamp - (messageNum - i) * durationEachMessage);
            final long endTimestamp = startTimestamp + (long) durationEachMessage;

            final MessageExt cachedMessage = cachedMessages.get(i);
            final String traceContext = cachedMessage.getTraceContext();

            final SpanBuilder spanBuilder =
                    tracer.spanBuilder(SpanName.CONSUME_MESSAGE).setStartTimestamp(startTimestamp,
                                                                                   TimeUnit.MILLISECONDS);
            final Span span = wrapSpan(traceContext, spanBuilder, cachedMessage);
            if (status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
                span.setStatus(StatusCode.OK);
            } else {
                span.setStatus(StatusCode.ERROR);
            }
            span.end(endTimestamp, TimeUnit.MILLISECONDS);
        }
    }

    private void recordMessageWaitingConsumptionSpan() {
        final Tracer tracer = processQueue.getTracer();
        final DefaultMQPushConsumerImpl consumerImpl = processQueue.getConsumerImpl();
        if (null == tracer || !consumerImpl.isMessageTracingEnabled()) {
            return;
        }
        for (MessageExt cachedMessage : cachedMessages) {
            final long decodedTimestamp = cachedMessage.getDecodedTimestamp();
            final SpanBuilder spanBuilder =
                    tracer.spanBuilder(SpanName.WAITING_CONSUMPTION).setStartTimestamp(decodedTimestamp,
                                                                                       TimeUnit.MILLISECONDS);
            final String traceContext = cachedMessage.getTraceContext();
            final Span span = wrapSpan(traceContext, spanBuilder, cachedMessage);
            span.end();
        }
    }

    private Span wrapSpan(String traceContext, SpanBuilder spanBuilder, MessageExt messageExt) {
        // Set sending-message's span as parent if it is valid.
        final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
        if (spanContext.isValid()) {
            spanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
        }
        final Span span = spanBuilder.startSpan();
        final DefaultMQPushConsumerImpl consumerImpl = processQueue.getConsumerImpl();
        // Record message born-timestamp.
        span.addEvent(EventName.MSG_BORN, messageExt.getBornTimestamp(), TimeUnit.MILLISECONDS);
        span.setAttribute(TracingAttribute.ARN, consumerImpl.getArn());
        span.setAttribute(TracingAttribute.TOPIC, messageExt.getTopic());
        span.setAttribute(TracingAttribute.MSG_ID, messageExt.getMsgId());
        span.setAttribute(TracingAttribute.GROUP, consumerImpl.getGroup());
        span.setAttribute(TracingAttribute.TAGS, messageExt.getTags());
        span.setAttribute(TracingAttribute.KEYS, messageExt.getKeys());
        span.setAttribute(TracingAttribute.RETRY_TIMES, messageExt.getReconsumeTimes());
        span.setAttribute(TracingAttribute.MSG_TYPE, messageExt.getMsgType().getName());
        return span;
    }
}
