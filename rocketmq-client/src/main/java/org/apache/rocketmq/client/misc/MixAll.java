package org.apache.rocketmq.client.misc;

public class MixAll {
    public static String DEFAULT_CHARSET = "UTF-8";

    public static long MASTER_BROKER_ID = 0;

    public static long DEFAULT_INVISIBLE_TIME_MILLIS = 30 * 1000L;
    public static long DEFAULT_POLL_TIME_MILLIS = 10 * 1000L;
    public static int DEFAULT_MAX_MESSAGE_NUMBER_PRE_BATCH = 32;

    public static long DEFAULT_LONG_POLLING_TIMEOUT_MILLIS = 15 * 1000L;
    public static int DEFAULT_MAX_CACHED_MESSAGES_COUNT_PER_MESSAGE_QUEUE = 1024;
    public static int DEFAULT_MAX_CACHED_MESSAGES_SIZE_PER_MESSAGE_QUEUE = 5 * 1024 * 1024;
    public static long DEFAULT_MAX_POP_MESSAGE_INTERVAL_MILLIS = 30 * 1000L;

    public static long DEFAULT_NACK_INVISIBLE_TIME_MILLIS = 1200;

    private MixAll() {
    }
}
