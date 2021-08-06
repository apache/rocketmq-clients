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


import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TracingUtilityTest {

    private final String dummyTraceIdHex = "0102030405060708090a0b0c0d0e0f10";
    private final String dummySpanIdHex = "0102030405060708";
    private final String dummySerializedSpanContext = "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01";

    @Test
    public void testInjectSpanContextToTraceParent() {
        SpanContext spanContext = SpanContext.createFromRemoteParent(dummyTraceIdHex,
                                                                     dummySpanIdHex, TraceFlags.getSampled(),
                                                                     TraceState.getDefault());
        Assert.assertEquals(TracingUtility.injectSpanContextToTraceParent(spanContext), dummySerializedSpanContext);
    }


    @Test
    public void testExtractContextFromTraceParent() {
        final SpanContext spanContext = TracingUtility.extractContextFromTraceParent(dummySerializedSpanContext);
        Assert.assertEquals(spanContext.getTraceId(), dummyTraceIdHex);
        Assert.assertEquals(spanContext.getSpanId(), dummySpanIdHex);
        Assert.assertTrue(spanContext.isSampled());
        Assert.assertTrue(spanContext.isRemote());
    }
}