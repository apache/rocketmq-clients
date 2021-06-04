package org.apache.rocketmq.client.tracing;

public class TracingAttribute {
    public static final String ARN = "arn";
    public static final String TOPIC = "topic";
    public static final String PRODUCER_GROUP = "producer_group";
    public static final String CONSUMER_GROUP = "consumer_group";
    public static final String MSG_ID = "msg_id";
    public static final String TAGS = "tags";
    public static final String STORE_HOST = "store_host";
    public static final String SUCCESS = "consumer_group";
    public static final String RETRY_TIMES = "retry_times";
    public static final String EXPIRED = "expired";

    private TracingAttribute() {
    }
}
