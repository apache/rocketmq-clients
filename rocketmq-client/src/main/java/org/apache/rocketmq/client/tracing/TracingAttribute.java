package org.apache.rocketmq.client.tracing;

public class TracingAttribute {
    public static final String ARN = "arn";
    public static final String ACCESS_KEY = "ak";
    public static final String TOPIC = "topic";
    public static final String GROUP = "group";
    public static final String MSG_ID = "msg_id";
    public static final String TAGS = "tags";
    public static final String TRANSACTION_ID = "trans_id";
    public static final String DELIVERY_TIMESTAMP = "delivery_timestamp";
    public static final String COMMIT_ACTION = "commit_action";
    public static final String BORN_HOST = "born_host";
    public static final String KEYS = "keys";
    public static final String ATTEMPT = "attempt";
    public static final String MSG_TYPE = "msg_type";

    private TracingAttribute() {
    }
}
