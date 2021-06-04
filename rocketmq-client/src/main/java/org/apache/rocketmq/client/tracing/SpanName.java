package org.apache.rocketmq.client.tracing;

public class SpanName {
    public static final String SEND_MESSAGE_SYNC = "SendMessageSync";
    public static final String SEND_MESSAGE_ASYNC = "SendMessageAsync";
    public static final String CONSUME_MESSAGE = "ConsumeMessage";

    private SpanName() {
    }
}
