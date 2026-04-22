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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Duration;

/**
 * Policy that controls how messages are aggregated into batches before being delivered
 * to a {@link BatchMessageListener} by the {@link PushConsumer}.
 *
 * <p>A batch is considered ready when <em>any</em> of the following conditions is met:
 * <ol>
 *   <li>The number of collected messages reaches {@code maxBatchCount}.</li>
 *   <li>The total body size (in bytes) of collected messages reaches {@code maxBatchBytes}.</li>
 *   <li>The time elapsed since the first message was collected reaches {@code maxWaitTime}.</li>
 * </ol>
 *
 * <p>The {@code maxBatchBytes} limit is critical for preventing client-side OOM when messages
 * have large bodies.
 */
public class BatchPolicy {

    /**
     * Default maximum number of messages returned in a single batch.
     */
    public static final int DEFAULT_MAX_BATCH_COUNT = 32;

    /**
     * Default maximum total body size (in bytes) before a batch is returned.
     * 4 MB by default.
     */
    public static final long DEFAULT_MAX_BATCH_BYTES = 4L * 1024 * 1024;

    /**
     * Default maximum wait time before a partial batch is returned.
     */
    public static final Duration DEFAULT_MAX_WAIT_TIME = Duration.ofSeconds(5);

    private final int maxBatchCount;
    private final long maxBatchBytes;
    private final Duration maxWaitTime;

    /**
     * Creates a {@link BatchPolicy} with the specified message count and wait time,
     * using the {@link #DEFAULT_MAX_BATCH_BYTES default byte limit}.
     *
     * @param maxBatchCount the maximum number of messages in a batch; must be &gt; 0.
     * @param maxWaitTime   the maximum time to wait for the batch to fill up; must be positive.
     */
    public BatchPolicy(int maxBatchCount, Duration maxWaitTime) {
        this(maxBatchCount, DEFAULT_MAX_BATCH_BYTES, maxWaitTime);
    }

    /**
     * Creates a {@link BatchPolicy} with all parameters specified.
     *
     * @param maxBatchCount the maximum number of messages in a batch; must be &gt; 0.
     * @param maxBatchBytes the maximum total body size (in bytes) of messages in a batch; must be &gt; 0.
     * @param maxWaitTime   the maximum time to wait for the batch to fill up; must be positive.
     */
    public BatchPolicy(int maxBatchCount, long maxBatchBytes, Duration maxWaitTime) {
        checkArgument(maxBatchCount > 0, "maxBatchCount must be greater than 0");
        checkArgument(maxBatchBytes > 0, "maxBatchBytes must be greater than 0");
        checkNotNull(maxWaitTime, "maxWaitTime should not be null");
        checkArgument(!maxWaitTime.isNegative() && !maxWaitTime.isZero(),
            "maxWaitTime must be positive");
        this.maxBatchCount = maxBatchCount;
        this.maxBatchBytes = maxBatchBytes;
        this.maxWaitTime = maxWaitTime;
    }

    /**
     * Returns the maximum number of messages that can be contained in a single batch.
     *
     * @return max batch count.
     */
    public int getMaxBatchCount() {
        return maxBatchCount;
    }

    /**
     * Returns the maximum total body size (in bytes) of messages in a single batch.
     *
     * @return max batch bytes.
     */
    public long getMaxBatchBytes() {
        return maxBatchBytes;
    }

    /**
     * Returns the maximum time to wait before returning a partial batch.
     *
     * @return max wait time.
     */
    public Duration getMaxWaitTime() {
        return maxWaitTime;
    }

    /**
     * Returns a new {@link Builder} with all defaults pre-populated.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "BatchPolicy{maxBatchCount=" + maxBatchCount
            + ", maxBatchBytes=" + maxBatchBytes
            + ", maxWaitTime=" + maxWaitTime + '}';
    }

    /**
     * Builder for {@link BatchPolicy}.  All fields have sensible defaults; only override
     * what you need.
     *
     * <pre>{@code
     * BatchPolicy policy = BatchPolicy.builder()
     *     .setMaxBatchCount(64)
     *     .setMaxWaitTime(Duration.ofSeconds(10))
     *     .build();
     * }</pre>
     */
    public static class Builder {
        private int maxBatchCount = DEFAULT_MAX_BATCH_COUNT;
        private long maxBatchBytes = DEFAULT_MAX_BATCH_BYTES;
        private Duration maxWaitTime = DEFAULT_MAX_WAIT_TIME;

        Builder() {
        }

        /**
         * Sets the maximum number of messages in a batch.
         *
         * @param maxBatchCount must be &gt; 0; default {@value DEFAULT_MAX_BATCH_COUNT}.
         */
        public Builder setMaxBatchCount(int maxBatchCount) {
            this.maxBatchCount = maxBatchCount;
            return this;
        }

        /**
         * Sets the maximum total body size (in bytes) before a batch is returned.
         *
         * @param maxBatchBytes must be &gt; 0; default {@value DEFAULT_MAX_BATCH_BYTES}.
         */
        public Builder setMaxBatchBytes(long maxBatchBytes) {
            this.maxBatchBytes = maxBatchBytes;
            return this;
        }

        /**
         * Sets the maximum time to wait for the batch to fill up.
         *
         * @param maxWaitTime must be positive; default 5 seconds.
         */
        public Builder setMaxWaitTime(Duration maxWaitTime) {
            this.maxWaitTime = maxWaitTime;
            return this;
        }

        /**
         * Builds a new {@link BatchPolicy} with the configured values.
         *
         * @return a new {@link BatchPolicy} instance.
         * @throws IllegalArgumentException if any parameter is invalid.
         */
        public BatchPolicy build() {
            return new BatchPolicy(maxBatchCount, maxBatchBytes, maxWaitTime);
        }
    }
}
