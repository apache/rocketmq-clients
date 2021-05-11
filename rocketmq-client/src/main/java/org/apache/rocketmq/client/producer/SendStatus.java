package org.apache.rocketmq.client.producer;

public enum SendStatus {
    /**
     * Send message successfully.
     */
    SEND_OK,
    /**
     * Only master is a available.
     */
    SLAVE_NOT_AVAILABLE,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT;

    public boolean isSuccess() {
        switch (this) {
            case SEND_OK:
            case SLAVE_NOT_AVAILABLE:
                return true;
            default:
                return false;
        }
    }
}
