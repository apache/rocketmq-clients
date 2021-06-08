package org.apache.rocketmq.client.tracing;

public class SpanName {
    public static final String SEND_MSG_SYNC = "SendMsgSync";
    public static final String SEND_MSG_ASYNC = "SendMsgAsync";
    public static final String CONSUME_MSG = "ConsumeMsg";

    private SpanName() {
    }
}
