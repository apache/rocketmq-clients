package org.apache.rocketmq.client.tracing;

public class SpanName {
    public static final String SEND_MESSAGE = "SendMessage";
    public static final String WAITING_CONSUMPTION = "WaitingConsumption";
    public static final String CONSUME_MESSAGE = "ConsumeMessage";
    public static final String END_MESSAGE = "EndMessage";

    private SpanName() {
    }
}
