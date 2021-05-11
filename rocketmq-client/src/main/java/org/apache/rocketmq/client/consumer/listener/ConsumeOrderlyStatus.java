package org.apache.rocketmq.client.consumer.listener;

public enum ConsumeOrderlyStatus {
    /**
     * Success consumption
     */
    SUCCESS,
    /**
     * Rollback consumption(only for binlog consumption)
     */
    @Deprecated
    ROLLBACK,
    /**
     * Commit offset(only for binlog consumption)
     */
    @Deprecated
    COMMIT,
    /**
     * Suspend current queue a moment
     */
    SUSPEND_CURRENT_QUEUE_A_MOMENT;
}
