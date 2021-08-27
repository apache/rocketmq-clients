/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.impl.ClientImpl;
import org.apache.rocketmq.client.message.MessageAccessor;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageHookPointStatus;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.producer.TransactionResolution;
import org.apache.rocketmq.client.remoting.CredentialsProvider;
import org.apache.rocketmq.utility.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message tracing is a specified implement of {@link MessageInterceptor}, which would collect related information
 * during the whole lifecycle of message, and transform them into {@link Span} of given {@link Tracer} of
 * {@link ClientImpl}.
 */
public class TracingMessageInterceptor implements MessageInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TracingMessageInterceptor.class);

    private final ClientImpl client;
    /**
     * Record inflight span, means the span is not ended.
     *
     * <p>Key is span id.
     */
    private final ConcurrentMap<String, Span> inflightSpans;
    private final ThreadLocal<SpanContext> waitingConsumptionSpanContextThreadLocal;

    public TracingMessageInterceptor(ClientImpl client) {
        this.client = client;
        this.inflightSpans = new ConcurrentHashMap<String, Span>();
        this.waitingConsumptionSpanContextThreadLocal = new ThreadLocal<SpanContext>();
    }

    public int getInflightSpanSize() {
        return inflightSpans.size();
    }

    private void interceptPreSendMessage(Tracer tracer, String accessKey, MessageExt message,
                                         MessageInterceptorContext context) {
        final Span parentSpan = tracer.spanBuilder(SpanName.PARENT).startSpan();
        try {
            final SpanContext parentSpanContext = parentSpan.getSpanContext();
            final Context parentContext = Context.current().with(Span.wrap(parentSpanContext));
            Span span = tracer.spanBuilder(SpanName.SEND_MESSAGE).setParent(parentContext).startSpan();
            if (StringUtils.isNotEmpty(accessKey)) {
                span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
            }
            span.setAttribute(TracingAttribute.ARN, client.getArn());
            span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
            span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
            span.setAttribute(TracingAttribute.GROUP, client.getGroup());
            span.setAttribute(TracingAttribute.TAG, message.getTag());
            span.setAttribute(TracingAttribute.KEYS, message.getKeys());
            span.setAttribute(TracingAttribute.ATTEMPT, context.getAttempt());
            span.setAttribute(TracingAttribute.MSG_TYPE, message.getMsgType().getName());
            span.setAttribute(TracingAttribute.CLIENT_ID, client.getClientId());
            final long deliveryTimestamp = message.getDeliveryTimestamp();
            if (deliveryTimestamp > 0) {
                span.setAttribute(TracingAttribute.DELIVERY_TIMESTAMP, deliveryTimestamp);
            }
            span.setAttribute(TracingAttribute.HOST, UtilAll.hostName());
            String traceContext = TracingUtility.injectSpanContextToTraceParent(span.getSpanContext());
            final String spanId = span.getSpanContext().getSpanId();
            inflightSpans.put(spanId, span);
            MessageAccessor.getMessageImpl(message).getSystemAttribute().setTraceContext(traceContext);
        } finally {
            parentSpan.end();
        }
    }

    private void interceptPostSendMessage(MessageExt message, MessageInterceptorContext context) {
        String traceContext = message.getTraceContext();
        final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
        Span span = inflightSpans.remove(spanContext.getSpanId());
        if (null != span) {
            final StatusCode statusCode = TracingUtility.convertToTraceStatus(context.getBizStatus());
            span.setStatus(statusCode);
            span.end();
        }
    }

    private void interceptPreMessageConsumption(Tracer tracer, String accessKey, MessageExt message,
                                                MessageInterceptorContext context) {
        String traceContext = message.getTraceContext();
        final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
        final SpanBuilder spanBuilder = tracer.spanBuilder(SpanName.AWAIT_CONSUMPTION)
                                              .setStartTimestamp(message.getDecodedTimestamp(),
                                                                 TimeUnit.MILLISECONDS);
        if (spanContext.isValid()) {
            spanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
        }
        Span span = spanBuilder.startSpan();
        if (StringUtils.isNotEmpty(accessKey)) {
            span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
        }
        span.setAttribute(TracingAttribute.ARN, client.getArn());
        span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
        span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
        span.setAttribute(TracingAttribute.GROUP, client.getGroup());
        span.setAttribute(TracingAttribute.TAG, message.getTag());
        span.setAttribute(TracingAttribute.HOST, UtilAll.hostName());
        span.setAttribute(TracingAttribute.KEYS, message.getKeys());
        span.setAttribute(TracingAttribute.ATTEMPT, context.getAttempt());
        span.setAttribute(TracingAttribute.MSG_TYPE, message.getMsgType().getName());
        span.setAttribute(TracingAttribute.AVAILABLE_TIMESTAMP, message.getStoreTimestamp());
        span.setAttribute(TracingAttribute.CLIENT_ID, client.getClientId());
        span.end();
        waitingConsumptionSpanContextThreadLocal.set(span.getSpanContext());
    }

    private void interceptPostMessageConsumption(Tracer tracer, String accessKey, MessageExt message,
                                                 MessageInterceptorContext context) {
        final SpanContext waitingConsumptionSpanContext = waitingConsumptionSpanContextThreadLocal.get();
        waitingConsumptionSpanContextThreadLocal.remove();
        final String traceContext = message.getTraceContext();
        final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
        final long endTimestamp = System.currentTimeMillis();
        long startTimestamp = endTimestamp - context.getTimeUnit().toMillis(context.getDuration());
        final SpanBuilder spanBuilder =
                tracer.spanBuilder(SpanName.CONSUME_MESSAGE).setStartTimestamp(startTimestamp,
                                                                               TimeUnit.MILLISECONDS);
        if (spanContext.isValid()) {
            spanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
        }
        if (null != waitingConsumptionSpanContext) {
            spanBuilder.addLink(waitingConsumptionSpanContext);
        }
        final Span span = spanBuilder.startSpan();
        if (StringUtils.isNotEmpty(accessKey)) {
            span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
        }
        span.setAttribute(TracingAttribute.ARN, client.getArn());
        span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
        span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
        span.setAttribute(TracingAttribute.GROUP, client.getGroup());
        span.setAttribute(TracingAttribute.TAG, message.getTag());
        span.setAttribute(TracingAttribute.HOST, UtilAll.hostName());
        span.setAttribute(TracingAttribute.KEYS, message.getKeys());
        span.setAttribute(TracingAttribute.ATTEMPT, context.getAttempt());
        span.setAttribute(TracingAttribute.MSG_TYPE, message.getMsgType().getName());
        span.setAttribute(TracingAttribute.AVAILABLE_TIMESTAMP, message.getStoreTimestamp());
        span.setAttribute(TracingAttribute.BATCH_SIZE, context.getBatchSize());
        span.setAttribute(TracingAttribute.CLIENT_ID, client.getClientId());

        final StatusCode statusCode = TracingUtility.convertToTraceStatus(context.getBizStatus());
        span.setStatus(statusCode);

        span.end(endTimestamp, TimeUnit.MILLISECONDS);
    }

    private void interceptPostEndMessage(Tracer tracer, String accessKey, MessageExt message,
                                         MessageInterceptorContext context) {
        final String traceContext = message.getTraceContext();
        final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);

        final SpanBuilder spanBuilder = tracer.spanBuilder(SpanName.END_MESSAGE);
        if (spanContext.isValid()) {
            spanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
        }
        long startTimestamp =
                System.currentTimeMillis() - context.getTimeUnit().toMillis(context.getDuration());
        final Span span = spanBuilder.setStartTimestamp(startTimestamp, TimeUnit.MILLISECONDS).startSpan();
        if (StringUtils.isNotEmpty(accessKey)) {
            span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
        }
        span.setAttribute(TracingAttribute.ARN, client.getArn());
        span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
        span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
        span.setAttribute(TracingAttribute.GROUP, client.getGroup());
        span.setAttribute(TracingAttribute.HOST, UtilAll.hostName());
        TransactionResolution resolution = MessageHookPointStatus.OK.equals(context.getBizStatus()) ?
                                           TransactionResolution.COMMIT : TransactionResolution.ROLLBACK;
        span.setAttribute(TracingAttribute.COMMIT_ACTION, resolution.getName());
        span.setAttribute(TracingAttribute.CLIENT_ID, client.getClientId());

        span.end();
    }

    @Override

    public void intercept(MessageHookPoint hookPoint, MessageExt message, MessageInterceptorContext context) {
        final Tracer tracer = client.getTracer();
        if (null == tracer) {
            return;
        }

        String accessKey = "";
        final CredentialsProvider credentialsProvider = client.getCredentialsProvider();
        if (null != credentialsProvider) {
            try {
                accessKey = credentialsProvider.getCredentials().getAccessKey();
            } catch (ClientException e) {
                log.error("Failed to fetch accessKey, clientId={}", client.getClientId(), e);
            }
        }
        switch (hookPoint) {
            case PRE_SEND_MESSAGE:
                interceptPreSendMessage(tracer, accessKey, message, context);
                break;
            case POST_SEND_MESSAGE:
                interceptPostSendMessage(message, context);
                break;
            case PRE_MESSAGE_CONSUMPTION:
                interceptPreMessageConsumption(tracer, accessKey, message, context);
                break;
            case POST_MESSAGE_CONSUMPTION:
                interceptPostMessageConsumption(tracer, accessKey, message, context);
                break;
            case POST_END_MESSAGE:
                interceptPostEndMessage(tracer, accessKey, message, context);
                break;
            default:
                break;
        }
    }
}
