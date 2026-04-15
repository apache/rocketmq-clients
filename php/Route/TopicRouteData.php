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

use Apache\Rocketmq\Exception\NotFoundException;
use Apache\Rocketmq\Util;

/**
 * TopicRouteData manages topic routing information
 * 
 * References Java TopicRouteData implementation (106 lines)
 * 
 * Key features:
 * - Manages message queue list for a topic
 * - Provides endpoint selection for assignment queries
 * - Round-robin load balancing for query endpoints
 * - Extracts all unique endpoints from message queues
 */
class TopicRouteData
{
    /**
     * @var int Index for round-robin selection
     */
    private $index;
    
    /**
     * @var MessageQueue[] Message queues
     */
    private $messageQueues = [];
    
    /**
     * Constructor from Protobuf message queues
     * 
     * @param \Apache\Rocketmq\V2\MessageQueue[] $messageQueues Protobuf message queues
     */
    public function __construct(array $messageQueues)
    {
        // Random initial index for load balancing
        $this->index = random_int(0, PHP_INT_MAX);
        
        $this->messageQueues = [];
        foreach ($messageQueues as $messageQueue) {
            $this->messageQueues[] = new MessageQueue($messageQueue);
        }
    }
    
    /**
     * Get all unique endpoints from message queues
     * 
     * @return Endpoints[] Unique endpoints
     */
    public function getTotalEndpoints(): array
    {
        $endpointsMap = [];
        
        foreach ($this->messageQueues as $messageQueue) {
            $brokerEndpoints = $messageQueue->getBroker()->getEndpoints();
            $key = $brokerEndpoints->getFacade();
            $endpointsMap[$key] = $brokerEndpoints;
        }
        
        return array_values($endpointsMap);
    }
    
    /**
     * Get message queues
     * 
     * @return MessageQueue[] Message queues
     */
    public function getMessageQueues(): array
    {
        return $this->messageQueues;
    }
    
    /**
     * Pick endpoints to query assignments
     * 
     * Uses round-robin to select endpoints from master brokers with READ permission.
     * This ensures load balancing across brokers when querying assignments.
     * 
     * @return Endpoints Selected endpoints
     * @throws NotFoundException If no suitable endpoints found
     */
    public function pickEndpointsToQueryAssignments(): Endpoints
    {
        $nextIndex = $this->index++;
        $queueCount = count($this->messageQueues);
        
        if ($queueCount === 0) {
            throw new NotFoundException("No message queues available");
        }
        
        // Try each queue starting from nextIndex
        for ($i = 0; $i < $queueCount; $i++) {
            $queueIndex = $nextIndex % $queueCount;
            $messageQueue = $this->messageQueues[$queueIndex];
            $nextIndex++;
            
            $broker = $messageQueue->getBroker();
            
            // Only query from master broker (id == 0)
            if ($broker->getId() !== 0) {
                continue;
            }
            
            // Only query from readable queues
            if (!$messageQueue->isReadable()) {
                continue;
            }
            
            return $broker->getEndpoints();
        }
        
        throw new NotFoundException("Failed to pick endpoints to query assignment");
    }
    
    /**
     * Get message queue by topic and queue ID
     * 
     * @param string $topic Topic name
     * @param int $queueId Queue ID
     * @return MessageQueue|null Message queue or null
     */
    public function getMessageQueue(string $topic, int $queueId): ?MessageQueue
    {
        foreach ($this->messageQueues as $messageQueue) {
            if ($messageQueue->getTopic() === $topic && $messageQueue->getQueueId() === $queueId) {
                return $messageQueue;
            }
        }
        return null;
    }
    
    /**
     * Get message queues by topic
     * 
     * @param string $topic Topic name
     * @return MessageQueue[] Message queues for topic
     */
    public function getMessageQueuesByTopic(string $topic): array
    {
        $result = [];
        foreach ($this->messageQueues as $messageQueue) {
            if ($messageQueue->getTopic() === $topic) {
                $result[] = $messageQueue;
            }
        }
        return $result;
    }
    
    /**
     * Get queue count
     * 
     * @return int Queue count
     */
    public function getQueueCount(): int
    {
        return count($this->messageQueues);
    }
    
    /**
     * Check equality
     * 
     * @param TopicRouteData $other Other route data
     * @return bool Whether equal
     */
    public function equals(TopicRouteData $other): bool
    {
        if (count($this->messageQueues) !== count($other->messageQueues)) {
            return false;
        }
        
        for ($i = 0; $i < count($this->messageQueues); $i++) {
            if (!$this->messageQueues[$i]->equals($other->messageQueues[$i])) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * {@inheritdoc}
     */
    public function __toString(): string
    {
        $queueStrs = [];
        foreach ($this->messageQueues as $queue) {
            $queueStrs[] = (string)$queue;
        }
        return "TopicRouteData{queues=[" . implode(", ", $queueStrs) . "]}";
    }
}
