package org.apache.rocketmq.client.tracing;

public class TracingAttribute {
    public static final String ARN = "arn";
    public static final String TOPIC = "topic";
    public static final String GROUP = "group";
    public static final String MSG_ID = "msg_id";
    public static final String TAGS = "tags";
    public static final String TRANSACTION_ID = "trans_id";
    //    public static final String STORE_HOST = "store_host";
    public static final String BORN_HOST = "born_host";
    public static final String KEYS = "keys";
    public static final String RETRY_TIMES = "retry_times";
    public static final String MSG_TYPE = "msg_type";

    private TracingAttribute() {
    }
}
