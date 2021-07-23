package org.apache.rocketmq.client.misc;

public class MixAll {
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final String MESSAGE_KEY_SEPARATOR = " ";

    public static final long MASTER_BROKER_ID = 0;

    public static final long DEFAULT_INVISIBLE_TIME_MILLIS = 30 * 1000L;
    public static final long DEFAULT_POLL_TIME_MILLIS = 10 * 1000L;
    public static final int DEFAULT_MAX_MESSAGE_NUMBER_PRE_BATCH = 32;

    private MixAll() {
    }
}
