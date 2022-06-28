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

public enum MetricName {
    /**
     * A histogram that records the cost time of successful api calls of message publishing.
     *
     * <p>Labels: {@link MetricLabels#TOPIC}, {@link MetricLabels#CLIENT_ID}, {@link MetricLabels#INVOCATION_STATUS}.
     */
    SEND_SUCCESS_COST_TIME("rocketmq_send_cost_time"),
    /**
     * A gauge that records the cached message count of push consumer.
     *
     * <p>Labels: {@link MetricLabels#TOPIC}, {@link MetricLabels#CLIENT_ID}, {@link MetricLabels#CONSUMER_GROUP}.
     */
    CONSUMER_CACHED_MESSAGES("rocketmq_consumer_cached_messages"),
    /**
     * A gauge that records the cached message bytes of push consumer.
     *
     * <p>Labels: {@link MetricLabels#TOPIC}, {@link MetricLabels#CLIENT_ID}, {@link MetricLabels#CONSUMER_GROUP}.
     */
    CONSUMER_CACHED_BYTES("rocketmq_consumer_cached_bytes"),
    /**
     * A histogram that records the latency of message delivery from remote.
     *
     * <p>Labels: {@link MetricLabels#TOPIC}, {@link MetricLabels#CLIENT_ID}, {@link MetricLabels#CONSUMER_GROUP}.
     */
    DELIVERY_LATENCY("rocketmq_delivery_latency"),
    /**
     * A histogram that records await time of message consumption.
     *
     * <p>Labels: {@link MetricLabels#TOPIC}, {@link MetricLabels#CLIENT_ID}, {@link MetricLabels#CONSUMER_GROUP}.
     */
    AWAIT_TIME("rocketmq_await_time"),
    /**
     * A histogram that records the process time of message consumption.
     *
     * <p>Labels: {@link MetricLabels#TOPIC}, {@link MetricLabels#CLIENT_ID}, {@link MetricLabels#CONSUMER_GROUP},
     * {@link MetricLabels#INVOCATION_STATUS}.
     */
    PROCESS_TIME("rocketmq_process_time");

    private final String name;

    MetricName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
