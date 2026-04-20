<?php

declare(strict_types=1);

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

namespace Apache\Rocketmq\Consumer;

use Apache\Rocketmq\MetricLabels;

/**
 * ProcessQueue Gauge Observer - Collects gauge metrics from ProcessQueues
 * 
 * References Java: org.apache.rocketmq.client.java.impl.consumer.ProcessQueueGaugeObserver
 * 
 * This observer provides real-time metrics about:
 * - CONSUMER_CACHED_MESSAGES: Total cached message count per topic
 * - CONSUMER_CACHED_BYTES: Total cached message bytes per topic
 */
class ProcessQueueGaugeObserver
{
    /** @var array<string, ProcessQueue> Process queue table */
    private $processQueueTable;
    
    /** @var string Client ID */
    private $clientId;
    
    /** @var string Consumer group */
    private $consumerGroup;
    
    /** @var array Supported gauge types */
    private $gauges;
    
    /**
     * Constructor
     * 
     * @param array $processQueueTable Map of message queue key to ProcessQueue
     * @param string $clientId Client ID
     * @param string $consumerGroup Consumer group name
     */
    public function __construct(array $processQueueTable, string $clientId, string $consumerGroup)
    {
        $this->processQueueTable = $processQueueTable;
        $this->clientId = $clientId;
        $this->consumerGroup = $consumerGroup;
        
        // Initialize supported gauges (aligned with Java)
        $this->gauges = [
            \Apache\Rocketmq\GaugeEnum::CONSUMER_CACHED_MESSAGES,
            \Apache\Rocketmq\GaugeEnum::CONSUMER_CACHED_BYTES,
        ];
    }
    
    /**
     * Get list of supported gauge types
     * 
     * @return array Array of GaugeEnum constants
     */
    public function getGauges(): array
    {
        return $this->gauges;
    }
    
    /**
     * Get gauge values for a specific gauge type
     * 
     * References Java ProcessQueueGaugeObserver.getValues():
     * Aggregates cached message count/bytes by topic across all process queues.
     * 
     * @param string $gauge Gauge type (GaugeEnum constant)
     * @return array Map of Attributes => value
     *               Each key is a JSON-encoded attribute map
     *               Each value is the aggregated metric value
     */
    public function getValues(string $gauge): array
    {
        switch ($gauge) {
            case \Apache\Rocketmq\GaugeEnum::CONSUMER_CACHED_MESSAGES:
                return $this->getCachedMessageCounts();
                
            case \Apache\Rocketmq\GaugeEnum::CONSUMER_CACHED_BYTES:
                return $this->getCachedMessageBytes();
                
            default:
                return [];
        }
    }
    
    /**
     * Get cached message counts aggregated by topic
     * 
     * @return array Map of attributes (JSON) => count
     */
    private function getCachedMessageCounts(): array
    {
        $cachedMessageCountMap = [];
        
        foreach ($this->processQueueTable as $pq) {
            try {
                $topic = $pq->getMessageQueue()->getTopic()->getName();
                
                // Build attributes
                $attributes = [
                    MetricLabels::TOPIC => $topic,
                    MetricLabels::CONSUMER_GROUP => $this->consumerGroup,
                    MetricLabels::CLIENT_ID => $this->clientId,
                ];
                
                // Create unique key for this attribute set
                $attrKey = json_encode($attributes);
                
                // Aggregate count by topic
                $count = isset($cachedMessageCountMap[$attrKey]) ? $cachedMessageCountMap[$attrKey] : 0;
                $count += $pq->getCachedMessageCount();
                $cachedMessageCountMap[$attrKey] = $count;
            } catch (\Throwable $e) {
                // Skip invalid process queues
                continue;
            }
        }
        
        return $cachedMessageCountMap;
    }
    
    /**
     * Get cached message bytes aggregated by topic
     * 
     * @return array Map of attributes (JSON) => bytes
     */
    private function getCachedMessageBytes(): array
    {
        $cachedMessageBytesMap = [];
        
        foreach ($this->processQueueTable as $pq) {
            try {
                $topic = $pq->getMessageQueue()->getTopic()->getName();
                
                // Build attributes
                $attributes = [
                    MetricLabels::TOPIC => $topic,
                    MetricLabels::CONSUMER_GROUP => $this->consumerGroup,
                    MetricLabels::CLIENT_ID => $this->clientId,
                ];
                
                // Create unique key for this attribute set
                $attrKey = json_encode($attributes);
                
                // Aggregate bytes by topic
                $bytes = isset($cachedMessageBytesMap[$attrKey]) ? $cachedMessageBytesMap[$attrKey] : 0;
                $bytes += $pq->getCachedMessageSizeInBytes();
                $cachedMessageBytesMap[$attrKey] = $bytes;
            } catch (\Throwable $e) {
                // Skip invalid process queues
                continue;
            }
        }
        
        return $cachedMessageBytesMap;
    }
}
