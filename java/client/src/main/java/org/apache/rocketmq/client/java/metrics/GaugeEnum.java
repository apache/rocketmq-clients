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

public enum GaugeEnum {
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
    CONSUMER_CACHED_BYTES("rocketmq_consumer_cached_bytes");

    private final String name;

    GaugeEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
