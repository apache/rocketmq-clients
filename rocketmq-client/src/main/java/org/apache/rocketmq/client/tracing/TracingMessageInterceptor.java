package org.apache.rocketmq.client.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.impl.ClientBaseImpl;
import org.apache.rocketmq.client.message.MessageAccessor;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;

public class TracingMessageInterceptor implements MessageInterceptor {

    private final ClientBaseImpl client;
    private final ConcurrentMap<String/* span id */, Span> inflightSpans;

    public TracingMessageInterceptor(ClientBaseImpl client) {
        this.client = client;
        this.inflightSpans = new ConcurrentHashMap<String, Span>();
    }

    @Override
    public void intercept(MessageHookPoint hookPoint, MessageExt message, MessageInterceptorContext context) {
        final Tracer tracer = client.getTracer();
        if (null == tracer) {
            return;
        }

        final String accessKey = client.getAccessCredential().getAccessKey();
        final String arn = client.getArn();
        final String group = client.getGroup();

        switch (hookPoint) {
            case PRE_SEND_MESSAGE: {
                Span span = tracer.spanBuilder(SpanName.SEND_MESSAGE).startSpan();

                span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
                span.setAttribute(TracingAttribute.ARN, arn);
                span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
                span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
                span.setAttribute(TracingAttribute.GROUP, group);
                span.setAttribute(TracingAttribute.TAGS, message.getTags());
                span.setAttribute(TracingAttribute.KEYS, message.getKeys());
                span.setAttribute(TracingAttribute.ATTEMPT, context.getAttempt());
                // span.setAttribute(TracingAttribute.MSG_TYPE, message.getMsgType().getName());
                final long deliveryTimestamp = message.getDeliveryTimestamp();
                if (deliveryTimestamp > 0) {
                    span.setAttribute(TracingAttribute.DELIVERY_TIMESTAMP, deliveryTimestamp);
                }
                String traceContext = TracingUtility.injectSpanContextToTraceParent(span.getSpanContext());
                final String spanId = span.getSpanContext().getSpanId();
                inflightSpans.put(spanId, span);
                MessageAccessor.getMessageImpl(message).getSystemAttribute().setTraceContext(traceContext);
                break;
            }
            case POST_SEND_MESSAGE: {
                String traceContext = message.getTraceContext();
                final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
                Span span = inflightSpans.remove(spanContext.getSpanId());
                if (null != span) {
                    final StatusCode statusCode = TracingUtility.convertToTraceStatus(context.getStatus());
                    span.setStatus(statusCode);
                    span.end();
                }
                break;
            }
            case PRE_PULL_MESSAGE:
                break;
            case POST_PULL_MESSAGE: {
                final String traceContext = message.getTraceContext();
                final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
                long startTimestamp =
                        System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(context.getDuration(),
                                                                                   context.getTimeUnit());
                final SpanBuilder spanBuilder =
                        tracer.spanBuilder(SpanName.PULL_MESSAGE).setStartTimestamp(startTimestamp,
                                                                                    context.getTimeUnit());
                if (spanContext.isValid()) {
                    spanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
                }
                final Span span = spanBuilder.startSpan();
                span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
                span.setAttribute(TracingAttribute.ARN, arn);
                span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
                span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
                span.setAttribute(TracingAttribute.GROUP, group);
                span.setAttribute(TracingAttribute.TAGS, message.getTags());
                span.setAttribute(TracingAttribute.KEYS, message.getKeys());
                span.setAttribute(TracingAttribute.ATTEMPT, context.getAttempt());
                // span.setAttribute(TracingAttribute.MSG_TYPE, message.getMsgType().getName());
                break;
            }
            case PRE_MESSAGE_CONSUMPTION: {
                String traceContext = message.getTraceContext();
                final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
                final SpanBuilder spanBuilder = tracer.spanBuilder(SpanName.WAITING_CONSUMPTION)
                                                      .setStartTimestamp(message.getDecodedTimestamp(),
                                                                         TimeUnit.MILLISECONDS);
                if (spanContext.isValid()) {
                    spanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
                }
                Span span = spanBuilder.startSpan();
                span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
                span.setAttribute(TracingAttribute.ARN, arn);
                span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
                span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
                span.setAttribute(TracingAttribute.GROUP, group);
                span.setAttribute(TracingAttribute.TAGS, message.getTags());
                span.setAttribute(TracingAttribute.KEYS, message.getKeys());
                span.setAttribute(TracingAttribute.ATTEMPT, context.getAttempt());
                // span.setAttribute(TracingAttribute.MSG_TYPE, message.getMsgType().getName());
                span.end();
                break;
            }
            case POST_MESSAGE_CONSUMPTION: {
                final String traceContext = message.getTraceContext();
                final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
                long startTimestamp =
                        System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(
                                (context.getMessageBatchSize()
                                 - context.getMessageIndex()) * context.getDuration(),
                                context.getTimeUnit());

                final SpanBuilder spanBuilder =
                        tracer.spanBuilder(SpanName.CONSUME_MESSAGE).setStartTimestamp(startTimestamp,
                                                                                       TimeUnit.MILLISECONDS);
                if (spanContext.isValid()) {
                    spanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
                }
                final Span span = spanBuilder.startSpan();
                span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
                span.setAttribute(TracingAttribute.ARN, arn);
                span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
                span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
                span.setAttribute(TracingAttribute.GROUP, group);
                span.setAttribute(TracingAttribute.TAGS, message.getTags());
                span.setAttribute(TracingAttribute.KEYS, message.getKeys());
                span.setAttribute(TracingAttribute.ATTEMPT, context.getAttempt());
                // span.setAttribute(TracingAttribute.MSG_TYPE, message.getMsgType().getName());

                final StatusCode statusCode = TracingUtility.convertToTraceStatus(context.getStatus());
                span.setStatus(statusCode);

                final long endTimestamp = startTimestamp + context.getDuration();
                span.end(endTimestamp, TimeUnit.MILLISECONDS);
                break;
            }
            case PRE_END_MESSAGE: {
                break;
            }
            case POST_END_MESSAGE: {
                final String traceContext = message.getTraceContext();
                final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);

                final SpanBuilder spanBuilder = tracer.spanBuilder(SpanName.END_MESSAGE);
                if (spanContext.isValid()) {
                    spanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
                }
                long startTimestamp =
                        System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(context.getDuration(),
                                                                                   context.getTimeUnit());
                final Span span = spanBuilder.setStartTimestamp(startTimestamp, TimeUnit.MILLISECONDS).startSpan();
                span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
                span.setAttribute(TracingAttribute.ARN, arn);
                span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
                span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
                span.setAttribute(TracingAttribute.GROUP, group);
                span.setAttribute(TracingAttribute.TRANSACTION_ID, message.getTransactionId());

                span.end();
                break;
            }
            default:
        }
    }
}
