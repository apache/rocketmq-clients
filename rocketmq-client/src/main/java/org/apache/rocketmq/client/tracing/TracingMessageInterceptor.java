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
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.remoting.CredentialsProvider;
import org.apache.rocketmq.utility.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingMessageInterceptor implements MessageInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TracingMessageInterceptor.class);
    
    private final ClientImpl client;
    private final ConcurrentMap<String/* span id */, Span> inflightSpans;
    private final ThreadLocal<SpanContext> waitingConsumptionSpanContextThreadLocal;

    public TracingMessageInterceptor(ClientImpl client) {
        this.client = client;
        this.inflightSpans = new ConcurrentHashMap<String, Span>();
        this.waitingConsumptionSpanContextThreadLocal = new ThreadLocal<SpanContext>();
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
                log.error("Failed to fetch accessKey", e);
            }
        }
        final String arn = client.getArn();
        final String group = client.getGroup();

        switch (hookPoint) {
            case PRE_SEND_MESSAGE: {
                final Span parentSpan = tracer.spanBuilder(SpanName.PARENT).startSpan();
                try {
                    final SpanContext parentSpanContext = parentSpan.getSpanContext();
                    final Context parentContext = Context.current().with(Span.wrap(parentSpanContext));
                    Span span = tracer.spanBuilder(SpanName.SEND_MESSAGE).setParent(parentContext).startSpan();
                    if (StringUtils.isNotEmpty(accessKey)) {
                        span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
                    }
                    span.setAttribute(TracingAttribute.ARN, arn);
                    span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
                    span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
                    span.setAttribute(TracingAttribute.GROUP, group);
                    span.setAttribute(TracingAttribute.TAG, message.getTag());
                    span.setAttribute(TracingAttribute.KEYS, message.getKeys());
                    span.setAttribute(TracingAttribute.ATTEMPT, context.getAttempt());
                    span.setAttribute(TracingAttribute.MSG_TYPE, message.getMsgType().getName());
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
                if (StringUtils.isNotEmpty(accessKey)) {
                    span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
                }
                span.setAttribute(TracingAttribute.ARN, arn);
                span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
                span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
                span.setAttribute(TracingAttribute.GROUP, group);
                span.setAttribute(TracingAttribute.TAG, message.getTag());
                span.setAttribute(TracingAttribute.HOST, UtilAll.hostName());
                span.setAttribute(TracingAttribute.KEYS, message.getKeys());
                span.setAttribute(TracingAttribute.ATTEMPT, context.getAttempt());
                span.setAttribute(TracingAttribute.MSG_TYPE, message.getMsgType().getName());
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
                if (StringUtils.isNotEmpty(accessKey)) {
                    span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
                }
                span.setAttribute(TracingAttribute.ARN, arn);
                span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
                span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
                span.setAttribute(TracingAttribute.GROUP, group);
                span.setAttribute(TracingAttribute.TAG, message.getTag());
                span.setAttribute(TracingAttribute.HOST, UtilAll.hostName());
                span.setAttribute(TracingAttribute.KEYS, message.getKeys());
                span.setAttribute(TracingAttribute.ATTEMPT, context.getAttempt());
                span.setAttribute(TracingAttribute.MSG_TYPE, message.getMsgType().getName());
                span.end();
                waitingConsumptionSpanContextThreadLocal.set(span.getSpanContext());
                break;
            }
            case POST_MESSAGE_CONSUMPTION: {
                final SpanContext waitingConsumptionSpanContext = waitingConsumptionSpanContextThreadLocal.get();
                waitingConsumptionSpanContextThreadLocal.remove();
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
                if (null != waitingConsumptionSpanContext) {
                    spanBuilder.addLink(waitingConsumptionSpanContext);
                }
                final Span span = spanBuilder.startSpan();
                if (StringUtils.isNotEmpty(accessKey)) {
                    span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
                }
                span.setAttribute(TracingAttribute.ARN, arn);
                span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
                span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
                span.setAttribute(TracingAttribute.GROUP, group);
                span.setAttribute(TracingAttribute.TAG, message.getTag());
                span.setAttribute(TracingAttribute.HOST, UtilAll.hostName());
                span.setAttribute(TracingAttribute.KEYS, message.getKeys());
                span.setAttribute(TracingAttribute.ATTEMPT, context.getAttempt());
                span.setAttribute(TracingAttribute.MSG_TYPE, message.getMsgType().getName());

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
                if (StringUtils.isNotEmpty(accessKey)) {
                    span.setAttribute(TracingAttribute.ACCESS_KEY, accessKey);
                }
                span.setAttribute(TracingAttribute.ARN, arn);
                span.setAttribute(TracingAttribute.TOPIC, message.getTopic());
                span.setAttribute(TracingAttribute.MSG_ID, message.getMsgId());
                span.setAttribute(TracingAttribute.GROUP, group);
                span.setAttribute(TracingAttribute.HOST, UtilAll.hostName());

                span.end();
                break;
            }
            default:
        }
    }
}
