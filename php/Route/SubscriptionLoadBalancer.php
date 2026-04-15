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

namespace Apache\Rocketmq\Route;

/**
 * SubscriptionLoadBalancer provides load balancing for subscription queries
 * 
 * References Java SubscriptionLoadBalancer (2.9KB)
 * 
 * Key features:
 * - Round-robin message queue selection
 * - Consumer group based assignment
 * - Consistent hashing for stable assignment
 */
class SubscriptionLoadBalancer
{
    /**
     * @var TopicRouteData Topic route data
     */
    private $topicRouteData;
    
    /**
     * Constructor
     * 
     * @param TopicRouteData $topicRouteData Topic route data
     */
    public function __construct(TopicRouteData $topicRouteData)
    {
        $this->topicRouteData = $topicRouteData;
    }
    
    /**
     * Pick message queue for consumer
     * 
     * Uses consistent hashing to assign message queues to consumers.
     * Consumers in the same group will get stable assignments.
     * 
     * @param string $consumerGroup Consumer group name
     * @param string $clientId Client ID
     * @param MessageQueue[] $assignedMessageQueues Assigned message queues
     * @return MessageQueue Selected message queue
     * @throws \InvalidArgumentException If no queues available
     */
    public function pickMessageQueue(
        string $consumerGroup,
        string $clientId,
        array $assignedMessageQueues
    ): MessageQueue {
        if (empty($assignedMessageQueues)) {
            throw new \InvalidArgumentException("No message queues available");
        }
        
        // Simple round-robin based on client ID hash
        $hash = crc32($consumerGroup . ':' . $clientId);
        $index = abs($hash) % count($assignedMessageQueues);
        
        return $assignedMessageQueues[$index];
    }
    
    /**
     * Get all message queues for topic
     * 
     * @return MessageQueue[] Message queues
     */
    public function getMessageQueues(): array
    {
        return $this->topicRouteData->getMessageQueues();
    }
    
    /**
     * Get message queue count
     * 
     * @return int Queue count
     */
    public function getQueueCount(): int
    {
        return $this->topicRouteData->getQueueCount();
    }
}
