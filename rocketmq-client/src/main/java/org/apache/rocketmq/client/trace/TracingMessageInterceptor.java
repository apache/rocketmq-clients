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

package org.apache.rocketmq.client.trace;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.impl.ClientImpl;
import org.apache.rocketmq.client.impl.consumer.PushConsumerImpl;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageImplAccessor;
import org.apache.rocketmq.client.message.MessageInterceptor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.remoting.CredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message tracing is a specified implement of {@link MessageInterceptor}, which would collect related information
 * during the whole lifecycle of message, and transform them into {@link Span} of given {@link Tracer} of
 * {@link ClientImpl}.
 */
public class TracingMessageInterceptor implements MessageInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TracingMessageInterceptor.class);

    private static final String MESSAGING_SYSTEM = "rocketmq";
    private static final String MESSAGING_PROTOCOL_VALUE = "RMQ-gRPC";

    private final MessageTracer messageTracer;
    /**
     * Record inflight span, means the span is not ended.
     *
     * <p>Key is trace context.
     */
    private final ConcurrentMap<String, Span> inflightSendSpans;
    private final ConcurrentMap<String, Span> inflightAwaitSpans;
    private final ThreadLocal<Span> processSpanThreadLocal;

    public TracingMessageInterceptor(MessageTracer messageTracer) {
        this.messageTracer = messageTracer;
        this.inflightSendSpans = new ConcurrentHashMap<String, Span>();
        this.inflightAwaitSpans = new ConcurrentHashMap<String, Span>();
        this.processSpanThreadLocal = new ThreadLocal<Span>();
    }

    public int getInflightSendSpanSize() {
        return inflightSendSpans.size();
    }

    public int getInflightAwaitSpanSize() {
        return inflightAwaitSpans.size();
    }

    private String getSpanName(String topic, RocketmqOperation operation) {
        return messageTracer.getClientImpl().getNamespace() + '/' + topic + " " + operation.getName();
    }

    private void addUniversalAttributes(RocketmqOperation operation, Span span, MessageInterceptorContext context) {
        final ClientImpl clientImpl = messageTracer.getClientImpl();
        span.setAttribute(MessagingAttributes.MESSAGING_SYSTEM, MESSAGING_SYSTEM);
        span.setAttribute(MessagingAttributes.MESSAGING_DESTINATION, context.getTopic());
        span.setAttribute(MessagingAttributes.MESSAGING_DESTINATION_KIND,
                          MessagingAttributes.MessagingDestinationKindValues.TOPIC);
        span.setAttribute(MessagingAttributes.MESSAGING_PROTOCOL, MESSAGING_PROTOCOL_VALUE);
        span.setAttribute(MessagingAttributes.MESSAGING_PROTOCOL_VERSION, MixAll.getProtocolVersion());
        span.setAttribute(MessagingAttributes.MESSAGING_URL, clientImpl.getNameServerStr());

        span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_NAMESPACE, clientImpl.getNamespace());
        span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_CLIENT_ID, clientImpl.getId());
        span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_CLIENT_GROUP, clientImpl.getGroup());
        span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_OPERATION, operation.getName());
        final CredentialsProvider credentialsProvider = clientImpl.getCredentialsProvider();
        // No credential provider was set.
        if (null == credentialsProvider) {
            return;
        }
        try {
            final String accessKey = credentialsProvider.getCredentials().getAccessKey();
            span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_ACCESS_KEY, accessKey);
        } catch (Throwable t) {
            log.warn("Failed to parse accessKey from credentials provider for tracing, clientId={}",
                     clientImpl.getId(), t);
        }
    }

    private void addMessageModelAttribute(Span span) {
        final ClientImpl clientImpl = messageTracer.getClientImpl();
        if (clientImpl instanceof PushConsumerImpl) {
            PushConsumerImpl pushConsumer = (PushConsumerImpl) clientImpl;
            final MessageModel messageModel = pushConsumer.getMessageModel();
            switch (messageModel) {
                case CLUSTERING:
                    span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_CONSUMPTION_MODEL,
                                      MessageModel.CLUSTERING.getName());
                    break;
                case BROADCASTING:
                    span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_CONSUMPTION_MODEL,
                                      MessageModel.BROADCASTING.getName());
                    break;
                default:
                    break;
            }
        }
    }

    private void addMessageUniversalAttributes(Span span, MessageExt messageExt) {
        span.setAttribute(MessagingAttributes.MESSAGING_MESSAGE_ID, messageExt.getMsgId());
        span.setAttribute(MessagingAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES, messageExt.getBody().length);

        span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_MESSAGE_TAG, messageExt.getTag());
        final List<String> keyList = MessageImplAccessor.getMessageImpl(messageExt).getSystemAttribute().getKeyList();
        if (!keyList.isEmpty()) {
            span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_MESSAGE_KEYS, keyList);
        }
        span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_MESSAGE_TYPE, messageExt.getMsgType().getName());
        final long deliveryTimestamp = messageExt.getDeliveryTimestamp();
        if (deliveryTimestamp > 0) {
            span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_DELIVERY_TIMESTAMP, deliveryTimestamp);
        }
    }

    private void interceptPreSendMessage(Tracer tracer, MessageExt messageExt, MessageInterceptorContext context) {
        final String sendSpanName = getSpanName(context.getTopic(), RocketmqOperation.SEND);
        Span span = tracer.spanBuilder(sendSpanName).setSpanKind(SpanKind.PRODUCER).startSpan();
        String traceContext = TracingUtility.injectSpanContextToTraceParent(span.getSpanContext());
        inflightSendSpans.put(traceContext, span);
        MessageImplAccessor.getMessageImpl(messageExt).getSystemAttribute().setTraceContext(traceContext);
    }

    private void interceptPostSendMessage(MessageExt messageExt, MessageInterceptorContext context) {
        String traceContext = messageExt.getTraceContext();
        final Span span = inflightSendSpans.remove(traceContext);
        if (null != span) {
            addUniversalAttributes(RocketmqOperation.SEND, span, context);
            addMessageUniversalAttributes(span, messageExt);
            span.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_ATTEMPT, context.getAttempt());
            final StatusCode statusCode = TracingUtility.convertToTraceStatus(context.getStatus());
            span.setStatus(statusCode);
            span.end();
        }
    }

    private void interceptPostCommitMessage(Tracer tracer, MessageExt messageExt, MessageInterceptorContext context) {
        final long endNanoTime = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        long startNanoTime = endNanoTime - context.getTimeUnit().toNanos(context.getDuration());
        final String commitSpanName = getSpanName(context.getTopic(), RocketmqOperation.COMMIT);
        final SpanBuilder commitSpanBuilder = tracer.spanBuilder(commitSpanName)
                                                    .setStartTimestamp(startNanoTime, TimeUnit.NANOSECONDS);

        String traceContext = messageExt.getTraceContext();
        final SpanContext sendSpanContext = TracingUtility.extractContextFromTraceParent(traceContext);
        if (sendSpanContext.isValid()) {
            commitSpanBuilder.setParent(Context.current().with(Span.wrap(sendSpanContext)));
        }

        final Span commitSpan = commitSpanBuilder.setSpanKind(SpanKind.PRODUCER).startSpan();
        addUniversalAttributes(RocketmqOperation.COMMIT, commitSpan, context);
        addMessageUniversalAttributes(commitSpan, messageExt);
        final StatusCode statusCode = TracingUtility.convertToTraceStatus(context.getStatus());
        commitSpan.setStatus(statusCode);
        commitSpan.end(endNanoTime, TimeUnit.NANOSECONDS);
    }

    private void interceptPostRollbackMessage(Tracer tracer, MessageExt messageExt, MessageInterceptorContext context) {
        final long endNanoTime = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        long startNanoTime = endNanoTime - context.getTimeUnit().toNanos(context.getDuration());
        final String rollbackSpanName = getSpanName(context.getTopic(), RocketmqOperation.ROLLBACK);
        final SpanBuilder rollbackSpanBuilder = tracer.spanBuilder(rollbackSpanName)
                                                      .setStartTimestamp(startNanoTime, TimeUnit.NANOSECONDS);

        String traceContext = messageExt.getTraceContext();
        final SpanContext sendSpanContext = TracingUtility.extractContextFromTraceParent(traceContext);
        if (sendSpanContext.isValid()) {
            rollbackSpanBuilder.setParent(Context.current().with(Span.wrap(sendSpanContext)));
        }

        final Span rollbackSpan = rollbackSpanBuilder.setSpanKind(SpanKind.PRODUCER).startSpan();
        addUniversalAttributes(RocketmqOperation.ROLLBACK, rollbackSpan, context);
        addMessageUniversalAttributes(rollbackSpan, messageExt);
        final StatusCode statusCode = TracingUtility.convertToTraceStatus(context.getStatus());
        rollbackSpan.setStatus(statusCode);
        rollbackSpan.end(endNanoTime, TimeUnit.NANOSECONDS);
    }

    private void interceptPostPull(Tracer tracer, MessageExt messageExt, MessageInterceptorContext context) {
        final long endNanoTime = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        long startNanoTime = endNanoTime - context.getTimeUnit().toNanos(context.getDuration());
        final String pullSpanName = getSpanName(context.getTopic(), RocketmqOperation.PULL);
        final SpanBuilder pullSpanBuilder = tracer.spanBuilder(pullSpanName)
                                                  .setStartTimestamp(startNanoTime, TimeUnit.NANOSECONDS);

        final Span pullSpan = pullSpanBuilder.setSpanKind(SpanKind.CONSUMER).startSpan();
        addMessageModelAttribute(pullSpan);
        pullSpan.setAttribute(MessagingAttributes.MESSAGING_OPERATION,
                              MessagingAttributes.MessagingOperationValues.RECEIVE);
        addUniversalAttributes(RocketmqOperation.PULL, pullSpan, context);
        pullSpan.setStatus(TracingUtility.convertToTraceStatus(context.getStatus()));
        pullSpan.end(endNanoTime, TimeUnit.NANOSECONDS);

        if (null == messageExt) {
            return;
        }
        final String traceContext = messageExt.getTraceContext();
        final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
        final String awaitSpanName = getSpanName(messageExt.getTopic(), RocketmqOperation.AWAIT);

        final SpanBuilder awaitSpanBuilder = tracer.spanBuilder(awaitSpanName);
        if (spanContext.isValid()) {
            awaitSpanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
        }
        awaitSpanBuilder.addLink(pullSpan.getSpanContext());
        Span awaitSpan = awaitSpanBuilder.setSpanKind(SpanKind.CONSUMER).startSpan();
        addMessageModelAttribute(awaitSpan);
        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(messageExt);
        String awaitTraceContext = TracingUtility.injectSpanContextToTraceParent(awaitSpan.getSpanContext());
        messageImpl.getSystemAttribute().setTraceContext(awaitTraceContext);
        inflightAwaitSpans.put(awaitTraceContext, awaitSpan);
    }

    private void interceptPostReceive(Tracer tracer, MessageExt messageExt, MessageInterceptorContext context) {
        final long endNanoTime = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        long startNanoTime = endNanoTime - context.getTimeUnit().toNanos(context.getDuration());
        final String receiveSpanName = getSpanName(context.getTopic(), RocketmqOperation.RECEIVE);
        final SpanBuilder receiveSpanBuilder = tracer.spanBuilder(receiveSpanName)
                                                     .setStartTimestamp(startNanoTime, TimeUnit.NANOSECONDS);
        final Span receiveSpan = receiveSpanBuilder.setSpanKind(SpanKind.CONSUMER).startSpan();
        addMessageModelAttribute(receiveSpan);
        receiveSpan.setAttribute(MessagingAttributes.MESSAGING_OPERATION,
                                 MessagingAttributes.MessagingOperationValues.RECEIVE);
        receiveSpan.setStatus(TracingUtility.convertToTraceStatus(context.getStatus()));
        receiveSpan.end(endNanoTime, TimeUnit.NANOSECONDS);

        if (null == messageExt) {
            return;
        }
        final String traceContext = messageExt.getTraceContext();
        final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(traceContext);
        final String awaitSpanName = getSpanName(messageExt.getTopic(), RocketmqOperation.AWAIT);

        final SpanBuilder awaitSpanBuilder = tracer.spanBuilder(awaitSpanName);
        if (spanContext.isValid()) {
            awaitSpanBuilder.setParent(Context.current().with(Span.wrap(spanContext)));
        }
        awaitSpanBuilder.addLink(receiveSpan.getSpanContext());
        Span awaitSpan = awaitSpanBuilder.setSpanKind(SpanKind.CONSUMER).startSpan();
        addMessageModelAttribute(awaitSpan);
        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(messageExt);
        String awaitTraceContext = TracingUtility.injectSpanContextToTraceParent(awaitSpan.getSpanContext());
        messageImpl.getSystemAttribute().setTraceContext(awaitTraceContext);
        inflightAwaitSpans.put(awaitTraceContext, awaitSpan);
    }

    private void interceptPreMessageConsumption(Tracer tracer, MessageExt messageExt,
                                                MessageInterceptorContext context) {
        String awaitTraceContext = messageExt.getTraceContext();
        final Span awaitSpan = inflightAwaitSpans.remove(awaitTraceContext);
        if (null != awaitSpan) {
            awaitSpan.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_AVAILABLE_TIMESTAMP,
                                   messageExt.getStoreTimestamp());
            addUniversalAttributes(RocketmqOperation.AWAIT, awaitSpan, context);
            addMessageUniversalAttributes(awaitSpan, messageExt);
            awaitSpan.end();
        }
        final SpanContext awaitSpanContext = TracingUtility.extractContextFromTraceParent(awaitTraceContext);
        final String processSpanName = getSpanName(messageExt.getTopic(), RocketmqOperation.PROCESS);
        final SpanBuilder spanBuilder = tracer.spanBuilder(processSpanName);
        if (awaitSpanContext.isValid()) {
            spanBuilder.setParent(Context.current().with(Span.wrap(awaitSpanContext)));
        }
        Span processSpan = spanBuilder.setSpanKind(SpanKind.CONSUMER).startSpan();
        addMessageModelAttribute(processSpan);
        String processTraceSpan = TracingUtility.injectSpanContextToTraceParent(processSpan.getSpanContext());
        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(messageExt);
        messageImpl.getSystemAttribute().setTraceContext(processTraceSpan);
        processSpanThreadLocal.set(processSpan);
    }

    private void interceptPostMessageConsumption(MessageExt messageExt, MessageInterceptorContext context) {
        final Span processSpan = processSpanThreadLocal.get();
        if (null == processSpan) {
            return;
        }
        processSpanThreadLocal.remove();
        processSpan.setAttribute(MessagingAttributes.MESSAGING_OPERATION,
                                 MessagingAttributes.MessagingOperationValues.PROCESS);
        processSpan.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_ATTEMPT, context.getAttempt());
        processSpan.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_AVAILABLE_TIMESTAMP,
                                 messageExt.getStoreTimestamp());
        processSpan.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_BATCH_SIZE, context.getBatchSize());
        addUniversalAttributes(RocketmqOperation.PROCESS, processSpan, context);
        addMessageUniversalAttributes(processSpan, messageExt);
        processSpan.setStatus(TracingUtility.convertToTraceStatus(context.getStatus()));
        processSpan.end();
    }

    private void interceptPostAckMessage(Tracer tracer, MessageExt messageExt, MessageInterceptorContext context) {
        final long endNanoTime = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        long startNanoTime = endNanoTime - context.getTimeUnit().toNanos(context.getDuration());
        final String ackSpanName = getSpanName(messageExt.getTopic(), RocketmqOperation.ACK);
        final SpanBuilder ackSpanBuilder = tracer.spanBuilder(ackSpanName)
                                                 .setStartTimestamp(startNanoTime, TimeUnit.NANOSECONDS);

        String processTraceContext = messageExt.getTraceContext();
        final SpanContext processSpanContext = TracingUtility.extractContextFromTraceParent(processTraceContext);
        if (processSpanContext.isValid()) {
            ackSpanBuilder.setParent(Context.current().with(Span.wrap(processSpanContext)));
        }
        final Span ackSpan = ackSpanBuilder.setSpanKind(SpanKind.CONSUMER).startSpan();
        addMessageModelAttribute(ackSpan);
        ackSpan.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_ATTEMPT, context.getAttempt());
        ackSpan.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_AVAILABLE_TIMESTAMP, messageExt.getStoreTimestamp());
        addUniversalAttributes(RocketmqOperation.ACK, ackSpan, context);
        addMessageUniversalAttributes(ackSpan, messageExt);
        final StatusCode statusCode = TracingUtility.convertToTraceStatus(context.getStatus());
        ackSpan.setStatus(statusCode);
        ackSpan.end(endNanoTime, TimeUnit.NANOSECONDS);
    }

    private void interceptPostNackMessage(Tracer tracer, MessageExt messageExt,
                                          MessageInterceptorContext context) {
        final long endNanoTime = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        long startNanoTime = endNanoTime - context.getTimeUnit().toNanos(context.getDuration());
        final String nackSpanName = getSpanName(messageExt.getTopic(), RocketmqOperation.NACK);
        final SpanBuilder nackSpanBuilder = tracer.spanBuilder(nackSpanName)
                                                  .setStartTimestamp(startNanoTime, TimeUnit.NANOSECONDS);

        String processTraceContext = messageExt.getTraceContext();
        final SpanContext processSpanContext = TracingUtility.extractContextFromTraceParent(processTraceContext);
        if (processSpanContext.isValid()) {
            nackSpanBuilder.setParent(Context.current().with(Span.wrap(processSpanContext)));
        }
        final Span nackSpan = nackSpanBuilder.setSpanKind(SpanKind.CONSUMER).startSpan();
        addMessageModelAttribute(nackSpan);
        nackSpan.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_AVAILABLE_TIMESTAMP,
                              messageExt.getStoreTimestamp());
        addUniversalAttributes(RocketmqOperation.NACK, nackSpan, context);
        addMessageUniversalAttributes(nackSpan, messageExt);
        final StatusCode statusCode = TracingUtility.convertToTraceStatus(context.getStatus());
        nackSpan.setStatus(statusCode);
        nackSpan.end(endNanoTime, TimeUnit.NANOSECONDS);
    }

    private void interceptPostForwardMessageToDLQ(Tracer tracer, MessageExt messageExt,
                                                  MessageInterceptorContext context) {
        final long endNanoTime = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        long startNanoTime = endNanoTime - context.getTimeUnit().toNanos(context.getDuration());
        final String dlqSpanName = getSpanName(messageExt.getTopic(), RocketmqOperation.DLQ);
        final SpanBuilder dlqSpanBuilder = tracer.spanBuilder(dlqSpanName)
                                                 .setStartTimestamp(startNanoTime, TimeUnit.NANOSECONDS);

        String processTraceContext = messageExt.getTraceContext();
        final SpanContext processSpanContext = TracingUtility.extractContextFromTraceParent(processTraceContext);
        if (processSpanContext.isValid()) {
            dlqSpanBuilder.setParent(Context.current().with(Span.wrap(processSpanContext)));
        }
        final Span dlqSpan = dlqSpanBuilder.setSpanKind(SpanKind.CONSUMER).startSpan();
        addMessageModelAttribute(dlqSpan);
        dlqSpan.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_ATTEMPT, context.getAttempt());
        dlqSpan.setAttribute(RocketmqAttributes.MESSAGING_ROCKETMQ_AVAILABLE_TIMESTAMP, messageExt.getStoreTimestamp());
        addUniversalAttributes(RocketmqOperation.DLQ, dlqSpan, context);
        addMessageUniversalAttributes(dlqSpan, messageExt);
        final StatusCode statusCode = TracingUtility.convertToTraceStatus(context.getStatus());
        dlqSpan.setStatus(statusCode);
        dlqSpan.end(endNanoTime, TimeUnit.NANOSECONDS);
    }

    @Override
    public void intercept(MessageHookPoint hookPoint, MessageExt message, MessageInterceptorContext context) {
        final Tracer tracer = messageTracer.getTracer();
        if (null == tracer) {
            return;
        }
        switch (hookPoint) {
            case PRE_SEND_MESSAGE:
                interceptPreSendMessage(tracer, message, context);
                break;
            case POST_SEND_MESSAGE:
                interceptPostSendMessage(message, context);
                break;
            case POST_COMMIT_MESSAGE:
                interceptPostCommitMessage(tracer, message, context);
                break;
            case POST_ROLLBACK_MESSAGE:
                interceptPostRollbackMessage(tracer, message, context);
                break;
            case POST_PULL:
                interceptPostPull(tracer, message, context);
                break;
            case POST_RECEIVE:
                interceptPostReceive(tracer, message, context);
                break;
            case PRE_MESSAGE_CONSUMPTION:
                interceptPreMessageConsumption(tracer, message, context);
                break;
            case POST_MESSAGE_CONSUMPTION:
                interceptPostMessageConsumption(message, context);
                break;
            case POST_ACK_MESSAGE:
                interceptPostAckMessage(tracer, message, context);
                break;
            case POST_NACK_MESSAGE:
                interceptPostNackMessage(tracer, message, context);
                break;
            case POST_FORWARD_MESSAGE_TO_DLQ:
                interceptPostForwardMessageToDLQ(tracer, message, context);
                break;
            default:
                break;
        }
    }
}
