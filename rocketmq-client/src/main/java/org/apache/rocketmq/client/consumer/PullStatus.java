package org.apache.rocketmq.client.consumer;

public enum PullStatus {
    /**
     * Messages are received as expected or no new message arrived.
     */
    OK,
    /**
     * Deadline expired before matched messages are found in the server side.
     */
    DEADLINE_EXCEEDED,
    /**
     * Resource has been exhausted, perhaps a per-user quota. For example, too many receive-message requests are
     * submitted to the same partition at the same time.
     */
    RESOURCE_EXHAUSTED,
    /**
     * The target partition does not exist, which might have been deleted.
     */
    NOT_FOUND,
    /**
     * Receive-message operation was attempted past the valid range. For pull operation, clients may try to pull expired
     * messages.
     */
    OUT_OF_RANGE,
    /**
     * Serious errors occurred in the server side.
     */
    INTERNAL,
    /**
     * Only for test purpose.
     */
    STATUS_FOR_TEST;
}
