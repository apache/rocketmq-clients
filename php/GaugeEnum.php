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
 * Gauge Enum - Predefined gauge metrics
 * 
 * Reference: Java GaugeEnum
 */
class GaugeEnum {
    /**
     * Consumer cached messages count
     * Labels: topic, client_id, consumer_group
     */
    const CONSUMER_CACHED_MESSAGES = 'rocketmq_consumer_cached_messages';
    
    /**
     * Consumer cached bytes
     * Labels: topic, client_id, consumer_group
     */
    const CONSUMER_CACHED_BYTES = 'rocketmq_consumer_cached_bytes';
    
    /**
     * Get all gauge names
     * 
     * @return array Array of gauge names
     */
    public static function getAll(): array {
        return [
            self::CONSUMER_CACHED_MESSAGES,
            self::CONSUMER_CACHED_BYTES,
        ];
    }
    
    /**
     * Get labels for a specific gauge
     * 
     * @param string $gaugeName Gauge name
     * @return array Required label keys
     */
    public static function getLabels(string $gaugeName): array {
        $labels = [
            self::CONSUMER_CACHED_MESSAGES => [
                MetricLabels::TOPIC,
                MetricLabels::CLIENT_ID,
                MetricLabels::CONSUMER_GROUP,
            ],
            self::CONSUMER_CACHED_BYTES => [
                MetricLabels::TOPIC,
                MetricLabels::CLIENT_ID,
                MetricLabels::CONSUMER_GROUP,
            ],
        ];
        
        return $labels[$gaugeName] ?? [MetricLabels::TOPIC, MetricLabels::CLIENT_ID];
    }
}
