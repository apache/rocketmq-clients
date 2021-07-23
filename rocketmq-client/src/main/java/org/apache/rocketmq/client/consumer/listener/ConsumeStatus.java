package org.apache.rocketmq.client.consumer.listener;

public enum ConsumeStatus {
    /**
     * Success consumption
     */
    CONSUME_SUCCESS,
    /**
     * Failure consumption,later try to consume
     */
    RECONSUME_LATER;
}
