package org.apache.rocketmq.client.consumer;

public enum PullStatus {
    /**
     * Founded
     */
    FOUND,
    /**
     * No new message can be pull
     */
    NO_NEW_MSG,
    /**
     * Filtering results can not match
     */
    NO_MATCHED_MSG,
    /**
     * Illegal offset,may be too big or too small
     */
    OFFSET_ILLEGAL
}
