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


import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TracingUtilityTest {
    public static final String FAKE_SERIALIZED_SPAN_CONTEXT = "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01";

    private static final String FAKE_TRACE_ID_HEX = "0102030405060708090a0b0c0d0e0f10";
    private static final String FAKE_SPAN_ID_HEX = "0102030405060708";

    @Test
    public void testInjectSpanContextToTraceParent() {
        SpanContext spanContext = SpanContext.createFromRemoteParent(FAKE_TRACE_ID_HEX,
                                                                     FAKE_SPAN_ID_HEX, TraceFlags.getSampled(),
                                                                     TraceState.getDefault());
        Assert.assertEquals(TracingUtility.injectSpanContextToTraceParent(spanContext), FAKE_SERIALIZED_SPAN_CONTEXT);
    }


    @Test
    public void testExtractContextFromTraceParent() {
        final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(FAKE_SERIALIZED_SPAN_CONTEXT);
        Assert.assertEquals(spanContext.getTraceId(), FAKE_TRACE_ID_HEX);
        Assert.assertEquals(spanContext.getSpanId(), FAKE_SPAN_ID_HEX);
        Assert.assertTrue(spanContext.isSampled());
        Assert.assertTrue(spanContext.isRemote());
    }
}