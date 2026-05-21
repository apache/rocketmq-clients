/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.apis.consumer;

import java.time.Duration;

/**
 * BatchPolicy defines the batching strategy for batch message consumption in {@link PushConsumer}.
 *
 * <p>Messages are accumulated in an internal buffer and flushed as a batch when <strong>any</strong> of the
 * following conditions is met:
 * <ul>
 *     <li>The number of buffered messages reaches {@link #getMaxBatchSize()}</li>
 *     <li>The cumulative byte size of buffered messages reaches {@link #getMaxBatchBytes()}</li>
 *     <li>The elapsed time since the first message entered the buffer reaches {@link #getMaxWaitTime()}</li>
 * </ul>
 *
 * <p>Use the {@link Builder} to create instances with sensible defaults:
 * <pre>{@code
 * BatchPolicy policy = BatchPolicy.newBuilder()
 *     .setMaxBatchSize(64)
 *     .setMaxWaitTime(Duration.ofMillis(500))
 *     .build();
 * }</pre>
 *
 * <p>Or use the direct constructor for full control:
 * <pre>{@code
 * BatchPolicy policy = new BatchPolicy(32, 4 * 1024 * 1024, Duration.ofSeconds(1));
 * }</pre>
 *
 * @see BatchMessageListener
 * @see PushConsumerBuilder#setBatchMessageListener(BatchMessageListener, BatchPolicy)
 */
public class BatchPolicy {

    /**
     * Default maximum number of messages per batch.
     */
    public static final int DEFAULT_MAX_BATCH_SIZE = 32;

    /**
     * Default maximum cumulative byte size per batch (4 MB).
     */
    public static final int DEFAULT_MAX_BATCH_BYTES = 4 * 1024 * 1024;

    /**
     * Default maximum wait time since the first message entered the buffer.
     */
    public static final Duration DEFAULT_MAX_WAIT_TIME = Duration.ofSeconds(5);

    private final int maxBatchSize;
    private final int maxBatchBytes;
    private final Duration maxWaitTime;

    /**
     * Creates a new BatchPolicy with the specified parameters.
     *
     * @param maxBatchSize  the maximum number of messages per batch, must be positive.
     * @param maxBatchBytes the maximum cumulative byte size of messages per batch, must be positive.
     * @param maxWaitTime   the maximum time to wait since the first message entered the buffer before
     *                      triggering a flush, must not be null or zero.
     * @throws IllegalArgumentException if maxBatchSize or maxBatchBytes is not positive,
     *                                  or if maxWaitTime is null or non-positive.
     */
    public BatchPolicy(int maxBatchSize, int maxBatchBytes, Duration maxWaitTime) {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive, actual: " + maxBatchSize);
        }
        if (maxBatchBytes <= 0) {
            throw new IllegalArgumentException("maxBatchBytes must be positive, actual: " + maxBatchBytes);
        }
        if (maxWaitTime == null || maxWaitTime.isZero() || maxWaitTime.isNegative()) {
            throw new IllegalArgumentException("maxWaitTime must be positive, actual: " + maxWaitTime);
        }
        this.maxBatchSize = maxBatchSize;
        this.maxBatchBytes = maxBatchBytes;
        this.maxWaitTime = maxWaitTime;
    }

    /**
     * Creates a new {@link Builder} for constructing a BatchPolicy with default values.
     *
     * @return a new builder instance.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Gets the maximum number of messages per batch.
     *
     * <p>When the number of buffered messages reaches this threshold, a batch flush is triggered immediately.
     *
     * @return the maximum batch size.
     */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    /**
     * Gets the maximum cumulative byte size of messages per batch.
     *
     * <p>When the total byte size of buffered messages reaches this threshold, a batch flush is triggered
     * immediately. The byte size is calculated from the message body only.
     *
     * @return the maximum batch bytes.
     */
    public int getMaxBatchBytes() {
        return maxBatchBytes;
    }

    /**
     * Gets the maximum wait time since the first message entered the buffer.
     *
     * <p>If neither {@link #getMaxBatchSize()} nor {@link #getMaxBatchBytes()} is reached within this duration,
     * a batch flush is triggered by the timer.
     *
     * @return the maximum wait time.
     */
    public Duration getMaxWaitTime() {
        return maxWaitTime;
    }

    @Override
    public String toString() {
        return "BatchPolicy{"
            + "maxBatchSize=" + maxBatchSize
            + ", maxBatchBytes=" + maxBatchBytes
            + ", maxWaitTime=" + maxWaitTime
            + '}';
    }

    /**
     * Builder for creating {@link BatchPolicy} instances with sensible defaults.
     *
     * <p>Default values:
     * <ul>
     *     <li>{@code maxBatchSize} = {@value DEFAULT_MAX_BATCH_SIZE}</li>
     *     <li>{@code maxBatchBytes} = 4 MB</li>
     *     <li>{@code maxWaitTime} = 5 seconds</li>
     * </ul>
     */
    public static class Builder {
        private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
        private int maxBatchBytes = DEFAULT_MAX_BATCH_BYTES;
        private Duration maxWaitTime = DEFAULT_MAX_WAIT_TIME;

        private Builder() {
        }

        /**
         * Sets the maximum number of messages per batch.
         *
         * @param maxBatchSize the maximum batch size, must be positive.
         * @return this builder instance.
         * @throws IllegalArgumentException if maxBatchSize is not positive.
         */
        public Builder setMaxBatchSize(int maxBatchSize) {
            if (maxBatchSize <= 0) {
                throw new IllegalArgumentException("maxBatchSize must be positive, actual: " + maxBatchSize);
            }
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        /**
         * Sets the maximum cumulative byte size of messages per batch.
         *
         * @param maxBatchBytes the maximum batch bytes, must be positive.
         * @return this builder instance.
         * @throws IllegalArgumentException if maxBatchBytes is not positive.
         */
        public Builder setMaxBatchBytes(int maxBatchBytes) {
            if (maxBatchBytes <= 0) {
                throw new IllegalArgumentException("maxBatchBytes must be positive, actual: " + maxBatchBytes);
            }
            this.maxBatchBytes = maxBatchBytes;
            return this;
        }

        /**
         * Sets the maximum wait time since the first message entered the buffer.
         *
         * @param maxWaitTime the maximum wait time, must not be null or non-positive.
         * @return this builder instance.
         * @throws IllegalArgumentException if maxWaitTime is null, zero, or negative.
         */
        public Builder setMaxWaitTime(Duration maxWaitTime) {
            if (maxWaitTime == null || maxWaitTime.isZero() || maxWaitTime.isNegative()) {
                throw new IllegalArgumentException("maxWaitTime must be positive, actual: " + maxWaitTime);
            }
            this.maxWaitTime = maxWaitTime;
            return this;
        }

        /**
         * Builds a new {@link BatchPolicy} instance with the configured parameters.
         *
         * @return the constructed BatchPolicy.
         */
        public BatchPolicy build() {
            return new BatchPolicy(maxBatchSize, maxBatchBytes, maxWaitTime);
        }
    }
}
