package org.apache.rocketmq.client.producer;

public enum SendStatus {
    /**
     * Send message successfully.
     */
    SEND_OK,

    @Deprecated
    SEND_FAILURE,
    /**
     * Only master is a available.
     */
    @Deprecated
    SLAVE_NOT_AVAILABLE,
    @Deprecated
    FLUSH_DISK_TIMEOUT,
    @Deprecated
    FLUSH_SLAVE_TIMEOUT;
}
