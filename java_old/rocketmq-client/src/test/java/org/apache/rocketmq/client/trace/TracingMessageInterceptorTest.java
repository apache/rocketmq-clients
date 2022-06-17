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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.PollCommandRequest;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.impl.ClientImpl;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageImplAccessor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.client.tools.TestBase;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TracingMessageInterceptorTest extends TestBase {
    @Mock
    private Tracer tracer;
    @Mock
    private SpanBuilder spanBuilder;
    @Mock
    private Span span;
    @Mock
    private MessageTracer messageTracer;
    @InjectMocks
    private TracingMessageInterceptor interceptor;
    private final SpanContext spanContext;
    private final ClientImpl clientImpl;

    public TracingMessageInterceptorTest() throws ClientException {
        this.spanContext =
                TracingUtility.extractContextFromTraceParent(TracingUtilityTest.FAKE_SERIALIZED_SPAN_CONTEXT);
        this.clientImpl = new ClientImpl(FAKE_GROUP_0) {
            @Override
            public void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData) {
            }

            @Override
            public NotifyClientTerminationRequest wrapNotifyClientTerminationRequest() {
                return null;
            }

            @Override
            public HeartbeatRequest wrapHeartbeatRequest() {
                return null;
            }

            @Override
            public PollCommandRequest wrapPollCommandRequest() {
                return null;
            }

            @Override
            public void doHealthCheck() {
            }

            @Override
            public void doStats() {
            }
        };
    }

    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);
        this.interceptor = new TracingMessageInterceptor(messageTracer);
        when(messageTracer.getTracer()).thenReturn(tracer);
        when(messageTracer.getClientImpl()).thenReturn(clientImpl);
        when(tracer.spanBuilder(ArgumentMatchers.<String>any())).thenReturn(spanBuilder);
        when(spanBuilder.startSpan()).thenReturn(span);
        when(spanBuilder.setSpanKind(ArgumentMatchers.<SpanKind>any())).thenReturn(spanBuilder);
        when(spanBuilder.setStartTimestamp(anyLong(), ArgumentMatchers.<TimeUnit>any())).thenReturn(spanBuilder);
        when(spanBuilder.setParent(ArgumentMatchers.<Context>any())).thenReturn(spanBuilder);
        when(span.getSpanContext()).thenReturn(spanContext);
    }

    @Test
    public void testInterceptSendMessage() {
        MessageInterceptorContext context = MessageInterceptorContext.builder().build();
        final MessageExt fakeMessageExt = fakeMessageExt();
        assertTrue(StringUtils.isEmpty(fakeMessageExt.getTraceContext()));
        interceptor.intercept(MessageHookPoint.PRE_SEND_MESSAGE, fakeMessageExt, context);

        verify(spanBuilder, times(1)).startSpan();
        ArgumentCaptor<SpanKind> spanKindArgumentCaptor = ArgumentCaptor.forClass(SpanKind.class);
        verify(spanBuilder, times(1)).setSpanKind(spanKindArgumentCaptor.capture());

        final SpanKind spanKindArgumentCaptorValue = spanKindArgumentCaptor.getValue();
        assertEquals(spanKindArgumentCaptorValue, SpanKind.PRODUCER);

        assertTrue(StringUtils.isNotEmpty(fakeMessageExt.getTraceContext()));
        assertEquals(MessageImplAccessor.getMessageImpl(fakeMessageExt).getSystemAttribute().getTraceContext(),
                     TracingUtilityTest.FAKE_SERIALIZED_SPAN_CONTEXT);
        assertEquals(interceptor.getInflightSendSpanSize(), 1);
        interceptor.intercept(MessageHookPoint.POST_SEND_MESSAGE, fakeMessageExt, context);
        assertEquals(interceptor.getInflightSendSpanSize(), 0);
    }

    @Test
    public void testInterceptPostCommitMessage() {
        MessageInterceptorContext context = MessageInterceptorContext.builder().build();
        final MessageExt fakeMessageExt = fakeMessageExt();
        interceptor.intercept(MessageHookPoint.POST_COMMIT_MESSAGE, fakeMessageExt, context);

        verify(spanBuilder, never()).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(1)).startSpan();
        verify(span, times(1)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());
        verify(span, times(1)).setStatus(ArgumentMatchers.<StatusCode>any());
        ArgumentCaptor<SpanKind> spanKindArgumentCaptor = ArgumentCaptor.forClass(SpanKind.class);
        verify(spanBuilder, times(1)).setSpanKind(spanKindArgumentCaptor.capture());

        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.PRODUCER);

        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(fakeMessageExt);
        messageImpl.getSystemAttribute().setTraceContext(TracingUtilityTest.FAKE_SERIALIZED_SPAN_CONTEXT);
        interceptor.intercept(MessageHookPoint.POST_COMMIT_MESSAGE, fakeMessageExt, context);

        verify(spanBuilder, times(1)).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(2)).startSpan();
        verify(span, times(2)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());
        verify(span, times(2)).setStatus(ArgumentMatchers.<StatusCode>any());
        verify(spanBuilder, times(2)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.PRODUCER);
    }

    @Test
    public void testInterceptPostRollbackMessage() {
        MessageInterceptorContext context = MessageInterceptorContext.builder().build();
        final MessageExt fakeMessageExt = fakeMessageExt();
        interceptor.intercept(MessageHookPoint.POST_ROLLBACK_MESSAGE, fakeMessageExt, context);

        verify(spanBuilder, never()).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(1)).startSpan();
        verify(span, times(1)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());
        verify(span, times(1)).setStatus(ArgumentMatchers.<StatusCode>any());
        ArgumentCaptor<SpanKind> spanKindArgumentCaptor = ArgumentCaptor.forClass(SpanKind.class);
        verify(spanBuilder, times(1)).setSpanKind(spanKindArgumentCaptor.capture());

        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.PRODUCER);

        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(fakeMessageExt);
        messageImpl.getSystemAttribute().setTraceContext(TracingUtilityTest.FAKE_SERIALIZED_SPAN_CONTEXT);
        interceptor.intercept(MessageHookPoint.POST_ROLLBACK_MESSAGE, fakeMessageExt, context);

        verify(spanBuilder, times(1)).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(2)).startSpan();
        verify(span, times(2)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());
        verify(span, times(2)).setStatus(ArgumentMatchers.<StatusCode>any());
        verify(spanBuilder, times(2)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.PRODUCER);
    }

    @Test
    public void testInterceptPostPull() {
        MessageInterceptorContext context = MessageInterceptorContext.builder().build();
        interceptor.intercept(MessageHookPoint.POST_PULL, null, context);
        verify(spanBuilder, never()).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(1)).startSpan();
        verify(span, times(1)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());
        verify(span, times(1)).setStatus(ArgumentMatchers.<StatusCode>any());
        ArgumentCaptor<SpanKind> spanKindArgumentCaptor = ArgumentCaptor.forClass(SpanKind.class);
        verify(spanBuilder, times(1)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.CONSUMER);

        final MessageExt fakeMessageExt = fakeMessageExt();
        interceptor.intercept(MessageHookPoint.POST_PULL, fakeMessageExt, context);
        verify(spanBuilder, never()).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(3)).startSpan();
        verify(span, times(2)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());
        verify(span, times(2)).setStatus(ArgumentMatchers.<StatusCode>any());
        verify(spanBuilder, times(3)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.CONSUMER);
        assertEquals(interceptor.getInflightAwaitSpanSize(), 1);

        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(fakeMessageExt);
        messageImpl.getSystemAttribute().setTraceContext(TracingUtilityTest.FAKE_SERIALIZED_SPAN_CONTEXT);
        interceptor.intercept(MessageHookPoint.POST_PULL, fakeMessageExt, context);
        verify(spanBuilder, times(1)).setParent(ArgumentMatchers.<Context>any());
    }

    @Test
    public void testInterceptPostReceive() {
        MessageInterceptorContext context = MessageInterceptorContext.builder().build();
        interceptor.intercept(MessageHookPoint.POST_RECEIVE, null, context);
        verify(spanBuilder, never()).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(1)).startSpan();
        verify(span, times(1)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());
        verify(span, times(1)).setStatus(ArgumentMatchers.<StatusCode>any());
        ArgumentCaptor<SpanKind> spanKindArgumentCaptor = ArgumentCaptor.forClass(SpanKind.class);
        verify(spanBuilder, times(1)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.CONSUMER);

        final MessageExt fakeMessageExt = fakeMessageExt();
        interceptor.intercept(MessageHookPoint.POST_RECEIVE, fakeMessageExt, context);
        verify(spanBuilder, never()).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(3)).startSpan();
        verify(span, times(2)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());
        verify(span, times(2)).setStatus(ArgumentMatchers.<StatusCode>any());
        verify(spanBuilder, times(3)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.CONSUMER);
        assertEquals(interceptor.getInflightAwaitSpanSize(), 1);

        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(fakeMessageExt);
        messageImpl.getSystemAttribute().setTraceContext(TracingUtilityTest.FAKE_SERIALIZED_SPAN_CONTEXT);
        interceptor.intercept(MessageHookPoint.POST_RECEIVE, fakeMessageExt, context);
        verify(spanBuilder, times(1)).setParent(ArgumentMatchers.<Context>any());
    }

    @Test
    public void testInterceptPreMessageConsumption() {
        MessageInterceptorContext context = MessageInterceptorContext.builder().build();
        final MessageExt fakeMessageExt = fakeMessageExt();
        interceptor.intercept(MessageHookPoint.POST_RECEIVE, fakeMessageExt, context);
        assertEquals(interceptor.getInflightAwaitSpanSize(), 1);
        interceptor.intercept(MessageHookPoint.PRE_MESSAGE_CONSUMPTION, fakeMessageExt, context);
        assertEquals(interceptor.getInflightAwaitSpanSize(), 0);
        verify(spanBuilder, times(1)).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(3)).startSpan();
        ArgumentCaptor<SpanKind> spanKindArgumentCaptor = ArgumentCaptor.forClass(SpanKind.class);
        verify(spanBuilder, times(3)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.CONSUMER);
    }

    @Test
    public void testInterceptPostMessageConsumption() {
        MessageInterceptorContext context = MessageInterceptorContext.builder().build();
        final MessageExt fakeMessageExt = fakeMessageExt();
        interceptor.intercept(MessageHookPoint.POST_MESSAGE_CONSUMPTION, fakeMessageExt, context);
        verify(span, never()).end();
        interceptor.intercept(MessageHookPoint.PRE_MESSAGE_CONSUMPTION, fakeMessageExt, context);
        interceptor.intercept(MessageHookPoint.POST_MESSAGE_CONSUMPTION, fakeMessageExt, context);
        verify(span, times(1)).end();
    }

    @Test
    public void testInterceptPostAckMessage() {
        MessageInterceptorContext context = MessageInterceptorContext.builder().build();
        final MessageExt fakeMessageExt = fakeMessageExt();
        interceptor.intercept(MessageHookPoint.POST_ACK_MESSAGE, fakeMessageExt, context);
        verify(spanBuilder, never()).setParent(ArgumentMatchers.<Context>any());
        ArgumentCaptor<SpanKind> spanKindArgumentCaptor = ArgumentCaptor.forClass(SpanKind.class);
        verify(spanBuilder, times(1)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.CONSUMER);
        verify(span, times(1)).setStatus(ArgumentMatchers.<StatusCode>any());
        verify(span, times(1)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());

        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(fakeMessageExt);
        messageImpl.getSystemAttribute().setTraceContext(TracingUtilityTest.FAKE_SERIALIZED_SPAN_CONTEXT);
        interceptor.intercept(MessageHookPoint.POST_ACK_MESSAGE, fakeMessageExt, context);
        verify(spanBuilder, times(1)).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(2)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.CONSUMER);
        verify(span, times(2)).setStatus(ArgumentMatchers.<StatusCode>any());
        verify(span, times(2)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());
    }

    @Test
    public void testInterceptPostNackMessage() {
        MessageInterceptorContext context = MessageInterceptorContext.builder().build();
        final MessageExt fakeMessageExt = fakeMessageExt();
        interceptor.intercept(MessageHookPoint.POST_NACK_MESSAGE, fakeMessageExt, context);
        verify(spanBuilder, never()).setParent(ArgumentMatchers.<Context>any());
        ArgumentCaptor<SpanKind> spanKindArgumentCaptor = ArgumentCaptor.forClass(SpanKind.class);
        verify(spanBuilder, times(1)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.CONSUMER);
        verify(span, times(1)).setStatus(ArgumentMatchers.<StatusCode>any());
        verify(span, times(1)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());

        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(fakeMessageExt);
        messageImpl.getSystemAttribute().setTraceContext(TracingUtilityTest.FAKE_SERIALIZED_SPAN_CONTEXT);
        interceptor.intercept(MessageHookPoint.POST_NACK_MESSAGE, fakeMessageExt, context);
        verify(spanBuilder, times(1)).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(2)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.CONSUMER);
        verify(span, times(2)).setStatus(ArgumentMatchers.<StatusCode>any());
        verify(span, times(2)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());
    }

    @Test
    public void testInterceptPostForwardMessageToDLQ() {
        MessageInterceptorContext context = MessageInterceptorContext.builder().build();
        final MessageExt fakeMessageExt = fakeMessageExt();
        interceptor.intercept(MessageHookPoint.POST_FORWARD_MESSAGE_TO_DLQ, fakeMessageExt, context);
        verify(spanBuilder, never()).setParent(ArgumentMatchers.<Context>any());
        ArgumentCaptor<SpanKind> spanKindArgumentCaptor = ArgumentCaptor.forClass(SpanKind.class);
        verify(spanBuilder, times(1)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.CONSUMER);
        verify(span, times(1)).setStatus(ArgumentMatchers.<StatusCode>any());
        verify(span, times(1)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());

        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(fakeMessageExt);
        messageImpl.getSystemAttribute().setTraceContext(TracingUtilityTest.FAKE_SERIALIZED_SPAN_CONTEXT);
        interceptor.intercept(MessageHookPoint.POST_FORWARD_MESSAGE_TO_DLQ, fakeMessageExt, context);
        verify(spanBuilder, times(1)).setParent(ArgumentMatchers.<Context>any());
        verify(spanBuilder, times(2)).setSpanKind(spanKindArgumentCaptor.capture());
        assertEquals(spanKindArgumentCaptor.getValue(), SpanKind.CONSUMER);
        verify(span, times(2)).setStatus(ArgumentMatchers.<StatusCode>any());
        verify(span, times(2)).end(anyLong(), ArgumentMatchers.<TimeUnit>any());
    }
}