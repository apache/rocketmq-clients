package org.apache.rocketmq.client.tracing;

import io.opentelemetry.api.internal.OtelEncodingUtils;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import java.util.HashSet;
import java.util.Set;

public class TracingUtility {

    public static final String TOPIC = "topic";
    public static final String CONSUMER_GROUP = "consumer_group";
    public static final String MSG_ID = "msg_id";
    public static final String TAGS = "tags";
    public static final String STORE_HOST = "store_host";
    public static final String SUCCESS = "consumer_group";
    public static final String RETRY_TIMES = "retry_times";
    public static final String EXPIRED = "expired";

    private static final String VERSION = "00";
    private static final String VERSION_00 = "00";
    private static final int VERSION_SIZE = 2;
    private static final char TRACE_PARENT_DELIMITER = '-';
    private static final int TRACE_PARENT_DELIMITER_SIZE = 1;
    private static final int TRACE_ID_HEX_SIZE = TraceId.getLength();
    private static final int SPAN_ID_HEX_SIZE = SpanId.getLength();
    private static final int TRACE_OPTION_HEX_SIZE = TraceFlags.getLength();
    private static final int TRACE_ID_OFFSET = VERSION_SIZE + TRACE_PARENT_DELIMITER_SIZE;
    private static final int SPAN_ID_OFFSET = TRACE_ID_OFFSET + TRACE_ID_HEX_SIZE + TRACE_PARENT_DELIMITER_SIZE;
    private static final int TRACE_OPTION_OFFSET = SPAN_ID_OFFSET + SPAN_ID_HEX_SIZE + TRACE_PARENT_DELIMITER_SIZE;
    private static final int TRACE_PARENT_HEADER_SIZE = TRACE_OPTION_OFFSET + TRACE_OPTION_HEX_SIZE;

    private static final Set<String> VALID_VERSIONS;

    private TracingUtility() {
    }

    static {
        // A valid version is 1 byte representing an 8-bit unsigned integer, version ff is invalid.
        VALID_VERSIONS = new HashSet<String>();
        for (int i = 0; i < 255; i++) {
            String version = Long.toHexString(i);
            if (version.length() < 2) {
                version = '0' + version;
            }
            VALID_VERSIONS.add(version);
        }
    }

    public static String injectSpanContextToTraceParent(SpanContext spanContext) {
        if (!spanContext.isValid()) {
            return "";
        }
        char[] chars = new char[TRACE_PARENT_HEADER_SIZE];
        chars[0] = VERSION.charAt(0);
        chars[1] = VERSION.charAt(1);
        chars[2] = TRACE_PARENT_DELIMITER;

        String traceId = spanContext.getTraceId();
        for (int i = 0; i < traceId.length(); i++) {
            chars[TRACE_ID_OFFSET + i] = traceId.charAt(i);
        }

        chars[SPAN_ID_OFFSET - 1] = TRACE_PARENT_DELIMITER;

        String spanId = spanContext.getSpanId();
        for (int i = 0; i < spanId.length(); i++) {
            chars[SPAN_ID_OFFSET + i] = spanId.charAt(i);
        }

        chars[TRACE_OPTION_OFFSET - 1] = TRACE_PARENT_DELIMITER;
        String traceFlagsHex = spanContext.getTraceFlags().asHex();
        chars[TRACE_OPTION_OFFSET] = traceFlagsHex.charAt(0);
        chars[TRACE_OPTION_OFFSET + 1] = traceFlagsHex.charAt(1);
        return new String(chars, 0, TRACE_PARENT_HEADER_SIZE);
    }

    public static SpanContext extractContextFromTraceParent(String traceParent) {
        if (null == traceParent) {
            return SpanContext.getInvalid();
        }
        boolean isValid =
                (traceParent.length() == TRACE_PARENT_HEADER_SIZE
                 || (traceParent.length() > TRACE_PARENT_HEADER_SIZE
                     && traceParent.charAt(TRACE_PARENT_HEADER_SIZE) == TRACE_PARENT_DELIMITER))
                && traceParent.charAt(TRACE_ID_OFFSET - 1) == TRACE_PARENT_DELIMITER
                && traceParent.charAt(SPAN_ID_OFFSET - 1) == TRACE_PARENT_DELIMITER
                && traceParent.charAt(TRACE_OPTION_OFFSET - 1) == TRACE_PARENT_DELIMITER;
        if (!isValid) {
            return SpanContext.getInvalid();
        }

        String version = traceParent.substring(0, 2);
        if (!VALID_VERSIONS.contains(version)) {
            return SpanContext.getInvalid();
        }
        if (version.equals(VERSION_00) && traceParent.length() > TRACE_PARENT_HEADER_SIZE) {
            return SpanContext.getInvalid();
        }

        String traceId = traceParent.substring(TRACE_ID_OFFSET, TRACE_ID_OFFSET + TraceId.getLength());
        String spanId = traceParent.substring(SPAN_ID_OFFSET, SPAN_ID_OFFSET + SpanId.getLength());
        char firstTraceFlagsChar = traceParent.charAt(TRACE_OPTION_OFFSET);
        char secondTraceFlagsChar = traceParent.charAt(TRACE_OPTION_OFFSET + 1);

        if (!OtelEncodingUtils.isValidBase16Character(firstTraceFlagsChar)
            || !OtelEncodingUtils.isValidBase16Character(secondTraceFlagsChar)) {
            return SpanContext.getInvalid();
        }

        TraceFlags traceFlags =
                TraceFlags.fromByte(
                        OtelEncodingUtils.byteFromBase16(firstTraceFlagsChar, secondTraceFlagsChar));
        return SpanContext.createFromRemoteParent(traceId, spanId, traceFlags, TraceState.getDefault());
    }
}
