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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.apache.rocketmq.client.impl.ClientImpl;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageHookPoint;
import org.apache.rocketmq.client.message.MessageImplAccessor;
import org.apache.rocketmq.client.message.MessageInterceptorContext;
import org.apache.rocketmq.client.tools.TestBase;
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
    private ClientImpl client;

    @InjectMocks
    private TracingMessageInterceptor interceptor;

    private final SpanContext spanContext;

    public TracingMessageInterceptorTest() {
        this.spanContext =
                TracingUtility.extractContextFromTraceParent(TracingUtilityTest.FAKE_SERIALIZED_SPAN_CONTEXT);
    }

    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);
        when(client.getTracer()).thenReturn(tracer);
        when(tracer.spanBuilder(ArgumentMatchers.<String>any())).thenReturn(spanBuilder);
        when(spanBuilder.startSpan()).thenReturn(span);
        when(spanBuilder.setParent(ArgumentMatchers.<Context>any())).thenReturn(spanBuilder);
        when(span.getSpanContext()).thenReturn(spanContext);
    }

    @Test
    public void testInterceptSendMessage() {
        MessageInterceptorContext context = MessageInterceptorContext.builder().build();
        final MessageExt fakeMessageExt = fakeMessageExt();
        interceptor.intercept(MessageHookPoint.PRE_SEND_MESSAGE, fakeMessageExt, context);
        assertEquals(MessageImplAccessor.getMessageImpl(fakeMessageExt).getSystemAttribute().getTraceContext(),
                     TracingUtilityTest.FAKE_SERIALIZED_SPAN_CONTEXT);
        assertEquals(interceptor.getInflightSpanSize(), 1);
        interceptor.intercept(MessageHookPoint.POST_SEND_MESSAGE, fakeMessageExt, context);
        assertEquals(interceptor.getInflightSpanSize(), 0);
    }
}