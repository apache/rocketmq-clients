<?php
/**
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

namespace Apache\Rocketmq;

/**
 * Histogram Enum - Predefined histogram metrics
 * 
 * Reference: Java HistogramEnum
 */
class HistogramEnum {
    /**
     * Send cost time histogram
     * Labels: topic, client_id, invocation_status
     * Unit: milliseconds
     */
    const SEND_COST_TIME = 'rocketmq_send_cost_time';
    
    /**
     * Delivery latency histogram
     * Labels: topic, client_id, consumer_group
     * Unit: milliseconds
     */
    const DELIVERY_LATENCY = 'rocketmq_delivery_latency';
    
    /**
     * Await time histogram
     * Labels: topic, client_id, consumer_group
     * Unit: milliseconds
     */
    const AWAIT_TIME = 'rocketmq_await_time';
    
    /**
     * Process time histogram
     * Labels: topic, client_id, consumer_group, invocation_status
     * Unit: milliseconds
     */
    const PROCESS_TIME = 'rocketmq_process_time';
    
    /**
     * Get histogram buckets configuration
     * 
     * @param string $histogramName Histogram name
     * @return array Bucket boundaries in milliseconds
     */
    public static function getBuckets(string $histogramName): array {
        $buckets = [
            self::SEND_COST_TIME => [1.0, 5.0, 10.0, 20.0, 50.0, 200.0, 500.0],
            self::DELIVERY_LATENCY => [1.0, 5.0, 10.0, 20.0, 50.0, 200.0, 500.0],
            self::AWAIT_TIME => [1.0, 5.0, 20.0, 100.0, 1000.0, 5000.0, 10000.0],
            self::PROCESS_TIME => [1.0, 5.0, 10.0, 100.0, 1000.0, 10000.0, 60000.0],
        ];
        
        return $buckets[$histogramName] ?? [1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0];
    }
    
    /**
     * Get all histogram names
     * 
     * @return array Array of histogram names
     */
    public static function getAll(): array {
        return [
            self::SEND_COST_TIME,
            self::DELIVERY_LATENCY,
            self::AWAIT_TIME,
            self::PROCESS_TIME,
        ];
    }
    
    /**
     * Get labels for a specific histogram
     * 
     * @param string $histogramName Histogram name
     * @return array Required label keys
     */
    public static function getLabels(string $histogramName): array {
        $labels = [
            self::SEND_COST_TIME => [
                MetricLabels::TOPIC,
                MetricLabels::CLIENT_ID,
                MetricLabels::INVOCATION_STATUS,
            ],
            self::DELIVERY_LATENCY => [
                MetricLabels::TOPIC,
                MetricLabels::CLIENT_ID,
                MetricLabels::CONSUMER_GROUP,
            ],
            self::AWAIT_TIME => [
                MetricLabels::TOPIC,
                MetricLabels::CLIENT_ID,
                MetricLabels::CONSUMER_GROUP,
            ],
            self::PROCESS_TIME => [
                MetricLabels::TOPIC,
                MetricLabels::CLIENT_ID,
                MetricLabels::CONSUMER_GROUP,
                MetricLabels::INVOCATION_STATUS,
            ],
        ];
        
        return $labels[$histogramName] ?? [MetricLabels::TOPIC, MetricLabels::CLIENT_ID];
    }
}
