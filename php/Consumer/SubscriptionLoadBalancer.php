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

namespace Apache\Rocketmq\Consumer;

use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Permission;

/**
 * SubscriptionLoadBalancer - Encapsulates queue selection and route update logic.
 * 
 * Aligned with Java SubscriptionLoadBalancer implementation.
 * Each topic maintains its own round-robin index for balanced load distribution.
 * 
 * Features:
 * - Filters readable master queues (broker ID = 0)
 * - Round-robin queue selection with independent index per topic
 * - Immutable design - returns new instance on route update
 * - Preserves round-robin position during route updates
 */
class SubscriptionLoadBalancer
{
    /**
     * @var array<MessageQueue> Filtered message queues (readable master queues only)
     */
    private $messageQueues;
    
    /**
     * @var int Round-robin index for queue selection
     * Initialized with random value for better load balancing across multiple consumers
     */
    private $index;
    
    /**
     * Constructor
     * 
     * @param array<MessageQueue> $messageQueues All message queues from route data
     * @param int|null $index Optional round-robin index (preserved during updates)
     * @throws \InvalidArgumentException If no readable master queue found
     */
    public function __construct(array $messageQueues, ?int $index = null)
    {
        // Filter readable master queues
        $this->messageQueues = array_filter($messageQueues, function($mq) {
            return self::isReadableMasterQueue($mq);
        });
        
        // Re-index array after filtering
        $this->messageQueues = array_values($this->messageQueues);
        
        if (empty($this->messageQueues)) {
            throw new \InvalidArgumentException(
                "No readable master queue found. Ensure topic has readable queues on master broker (ID=0)."
            );
        }
        
        // Initialize or preserve round-robin index
        // Aligned with Java: RandomUtils.nextInt(0, Integer.MAX_VALUE)
        $this->index = $index !== null ? $index : mt_rand(0, PHP_INT_MAX);
    }
    
    /**
     * Check if the message queue is readable and belongs to master broker.
     * 
     * Aligned with Java: mq.getPermission().isReadable() && Utilities.MASTER_BROKER_ID == mq.getBroker().getId()
     * 
     * @param MessageQueue $mq Message queue to check
     * @return bool True if readable master queue
     */
    public static function isReadableMasterQueue(MessageQueue $mq): bool
    {
        $perm = $mq->getPermission();
        $isReadable = ($perm === Permission::READ || $perm === Permission::READ_WRITE);
        
        // Master broker ID = 0 (aligned with Java Utilities.MASTER_BROKER_ID)
        $brokerId = $mq->getBroker()->getId();
        $isMaster = ($brokerId === 0);
        
        return $isReadable && $isMaster;
    }
    
    /**
     * Update route data while preserving round-robin position.
     * 
     * Returns a new instance with updated queues but same index position.
     * This ensures smooth transition when route changes without disrupting load balancing.
     * 
     * @param array<MessageQueue> $newMessageQueues New message queues from updated route
     * @return self New SubscriptionLoadBalancer instance with updated queues
     * @throws \InvalidArgumentException If no readable master queue found in new route
     */
    public function update(array $newMessageQueues): self
    {
        // Create new instance with preserved index
        return new self($newMessageQueues, $this->index);
    }
    
    /**
     * Select next message queue using round-robin algorithm.
     * 
     * Thread-safe round-robin selection that automatically wraps around.
     * Each call advances the internal index for next selection.
     * 
     * @return MessageQueue Selected message queue
     */
    public function takeMessageQueue(): MessageQueue
    {
        $queueCount = count($this->messageQueues);
        
        // Calculate current index (wraps around using modulo)
        $currentIndex = $this->index % $queueCount;
        
        // Advance index for next call
        $this->index++;
        
        return $this->messageQueues[$currentIndex];
    }
    
    /**
     * Get all filtered message queues.
     * 
     * @return array<MessageQueue> Readable master queues
     */
    public function getMessageQueues(): array
    {
        return $this->messageQueues;
    }
    
    /**
     * Get queue count.
     * 
     * @return int Number of readable master queues
     */
    public function getQueueCount(): int
    {
        return count($this->messageQueues);
    }
    
    /**
     * Get current round-robin index (for debugging/monitoring).
     * 
     * @return int Current index value
     */
    public function getIndex(): int
    {
        return $this->index;
    }
}
