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