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

package org.apache.rocketmq.client.java.metrics;

import io.opentelemetry.sdk.metrics.Aggregation;
import java.util.Arrays;

public enum HistogramEnum {
    /**
     * A histogram that records the cost time of successful api calls of message publishing.
     *
     * <p>Labels: {@link MetricLabels#TOPIC}, {@link MetricLabels#CLIENT_ID}, {@link MetricLabels#INVOCATION_STATUS}.
     *
     * <p>The time unit of bucket is milliseconds.
     */
    SEND_SUCCESS_COST_TIME("rocketmq_send_cost_time", Aggregation.explicitBucketHistogram(Arrays.asList(1.0, 5.0,
        10.0, 20.0, 50.0, 200.0, 500.0))),

    /**
     * A histogram that records the latency of message delivery from remote.
     *
     * <p>Labels: {@link MetricLabels#TOPIC}, {@link MetricLabels#CLIENT_ID}, {@link MetricLabels#CONSUMER_GROUP}.
     *
     * <p>The time unit of bucket is milliseconds.
     */
    DELIVERY_LATENCY("rocketmq_delivery_latency", Aggregation.explicitBucketHistogram(Arrays.asList(1.0,
        5.0, 10.0, 20.0, 50.0, 200.0, 500.0))),

    /**
     * A histogram that records await time of message consumption.
     *
     * <p>Labels: {@link MetricLabels#TOPIC}, {@link MetricLabels#CLIENT_ID}, {@link MetricLabels#CONSUMER_GROUP}.
     *
     * <p>The time unit of bucket is milliseconds.
     */
    AWAIT_TIME("rocketmq_await_time", Aggregation.explicitBucketHistogram(Arrays.asList(1.0, 5.0,
        20.0, 100.0, 1000.0, 5 * 1000.0, 10 * 1000.0))),
    /**
     * A histogram that records the process time of message consumption.
     *
     * <p>Labels: {@link MetricLabels#TOPIC}, {@link MetricLabels#CLIENT_ID}, {@link MetricLabels#CONSUMER_GROUP},
     * {@link MetricLabels#INVOCATION_STATUS}.
     *
     * <p>The time unit of bucket is milliseconds.
     */
    PROCESS_TIME("rocketmq_process_time", Aggregation.explicitBucketHistogram(Arrays.asList(1.0, 5.0,
        10.0, 100.0, 1000.0, 10 * 1000.0, 60 * 1000.0)));

    private final String name;
    private final Aggregation bucket;

    HistogramEnum(String name, Aggregation bucket) {
        this.name = name;
        this.bucket = bucket;
    }

    public String getName() {
        return name;
    }

    public Aggregation getBucket() {
        return bucket;
    }
}
