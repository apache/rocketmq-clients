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

namespace Apache\Rocketmq\Producer;

require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\Logger;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Permission;

/**
 * Publishing Load Balancer for Producer
 * 
 * Encapsulates message queue selection logic with support for:
 * - Round-robin load balancing
 * - Message group based routing (FIFO messages)
 * - Endpoints isolation handling
 * 
 * Aligned with Java PublishingLoadBalancer.
 */
class PublishingLoadBalancer
{
    /**
     * @var int Round-robin index
     */
    private $index;
    
    /**
     * @var array Array of writable master MessageQueue objects
     */
    private $messageQueues;
    
    /**
     * @var string Topic name (for logging)
     */
    private $topic;
    
    /**
     * Constructor
     * 
     * @param array $messageQueues All message queues from route data
     * @param string $topic Topic name
     * @throws \InvalidArgumentException If no writable master queue found
     */
    public function __construct(array $messageQueues, string $topic = '')
    {
        $this->topic = $topic;
        
        // Initialize index with random value (aligned with Java RandomUtils.nextInt(0, Integer.MAX_VALUE))
        $this->index = mt_rand(0, PHP_INT_MAX);
        
        // Filter: only writable master queues (aligned with Java L59-61)
        // Java: mq.getPermission().isWritable() && Utilities.MASTER_BROKER_ID == mq.getBroker().getId()
        $writableMqs = [];
        foreach ($messageQueues as $mq) {
            if ($this->isWritableMasterQueue($mq)) {
                $writableMqs[] = $mq;
            }
        }
        
        if (empty($writableMqs)) {
            throw new \InvalidArgumentException(
                "No writable message queue found for topic: {$topic}"
            );
        }
        
        $this->messageQueues = $writableMqs;
        
        Logger::debug("Created PublishingLoadBalancer, topic={}, writableQueues={}", [
            $topic,
            count($writableMqs)
        ]);
    }
    
    /**
     * Update load balancer with new route data
     * 
     * @param array $newMessageQueues New message queues from updated route
     * @return self New PublishingLoadBalancer instance (immutable pattern)
     */
    public function update(array $newMessageQueues): self
    {
        return new self($newMessageQueues, $this->topic);
    }
    
    /**
     * Take message queue by message group (for FIFO messages)
     * 
     * Uses SipHash24 to hash the message group and select a queue.
     * This ensures messages with the same messageGroup are always routed to the same queue.
     * 
     * Aligned with Java takeMessageQueueByMessageGroup().
     * 
     * @param string $messageGroup Message group ID
     * @return MessageQueue Selected message queue
     * @throws \InvalidArgumentException If messageGroup is empty
     */
    public function takeMessageQueueByMessageGroup(string $messageGroup): MessageQueue
    {
        if (empty($messageGroup)) {
            throw new \InvalidArgumentException("Message group cannot be empty");
        }
        
        $queueCount = count($this->messageQueues);
        
        if ($queueCount === 1) {
            return $this->messageQueues[0];
        }
        
        // Use SipHash24-like hashing (PHP implementation)
        // Java uses Guava's Hashing.sipHash24(), we use crc32c as alternative
        $hash = $this->sipHash24($messageGroup);
        $index = abs($hash) % $queueCount;
        
        $selectedQueue = $this->messageQueues[$index];
        
        Logger::debug("Selected queue by message group, messageGroup={}, queueIndex={}, brokerName={}, clientId={}", [
            $messageGroup,
            $index,
            $selectedQueue->getBroker()->getName(),
            'producer'
        ]);
        
        return $selectedQueue;
    }
    
    /**
     * Take message queues for sending (with endpoints isolation support)
     * 
     * Selects up to $count message queues, excluding isolated endpoints.
     * Uses round-robin strategy to distribute load across brokers.
     * 
     * Aligned with Java takeMessageQueues().
     * 
     * @param array $excludedEndpoints Array of excluded endpoint strings (isolated brokers)
     * @param int $count Number of queues to select (default: 1)
     * @return array Array of selected MessageQueue objects
     */
    public function takeMessageQueues(array $excludedEndpoints = [], int $count = 1): array
    {
        $next = $this->index++;
        $candidates = [];
        $candidateBrokerNames = [];
        
        $queueCount = count($this->messageQueues);
        
        // First pass: try to find queues from non-isolated endpoints
        for ($i = 0; $i < $queueCount; $i++) {
            $queueIndex = ($next + $i) % $queueCount;
            $messageQueue = $this->messageQueues[$queueIndex];
            
            $broker = $messageQueue->getBroker();
            $brokerName = $broker->getName();
            $brokerEndpoints = $this->getEndpointsString($broker->getEndpoints());
            
            // Check if this broker's endpoints are excluded
            if (!in_array($brokerEndpoints, $excludedEndpoints, true) && 
                !isset($candidateBrokerNames[$brokerName])) {
                $candidateBrokerNames[$brokerName] = true;
                $candidates[] = $messageQueue;
            }
            
            if (count($candidates) >= $count) {
                break;
            }
        }
        
        // Second pass: if all endpoints are isolated, ignore exclusion
        if (empty($candidates)) {
            Logger::warn("All endpoints are isolated, ignoring exclusion for topic={}", [$this->topic]);
            
            for ($i = 0; $i < $queueCount; $i++) {
                $queueIndex = ($next + $i) % $queueCount;
                $messageQueue = $this->messageQueues[$queueIndex];
                
                $broker = $messageQueue->getBroker();
                $brokerName = $broker->getName();
                
                if (!isset($candidateBrokerNames[$brokerName])) {
                    $candidateBrokerNames[$brokerName] = true;
                    $candidates[] = $messageQueue;
                }
                
                if (count($candidates) >= $count) {
                    break;
                }
            }
        }
        
        Logger::debug("Selected {} candidate queues, topic={}, excludedEndpoints={}, clientId={}", [
            count($candidates),
            $this->topic,
            count($excludedEndpoints),
            'producer'
        ]);
        
        return $candidates;
    }
    
    /**
     * Get all writable message queues
     * 
     * @return array Array of MessageQueue objects
     */
    public function getMessageQueues(): array
    {
        return $this->messageQueues;
    }
    
    /**
     * Get queue count
     * 
     * @return int Number of writable master queues
     */
    public function getQueueCount(): int
    {
        return count($this->messageQueues);
    }
    
    /**
     * Check if a message queue is a writable master queue
     * 
     * Aligned with Java: mq.getPermission().isWritable() && Utilities.MASTER_BROKER_ID == mq.getBroker().getId()
     * 
     * @param MessageQueue $mq Message queue to check
     * @return bool True if writable master queue
     */
    private function isWritableMasterQueue(MessageQueue $mq): bool
    {
        // Check permission: must be WRITABLE or READ_WRITE
        $perm = $mq->getPermission();
        $isWritable = ($perm === Permission::WRITE || $perm === Permission::READ_WRITE);
        
        // Check if it's master broker (broker ID = 0, aligned with Java Utilities.MASTER_BROKER_ID)
        $brokerId = $mq->getBroker()->getId();
        $isMaster = ($brokerId === 0);
        
        return $isWritable && $isMaster;
    }
    
    /**
     * Simple SipHash24-like hash function
     * 
     * Note: This is a simplified version. For production use, consider using
     * a proper SipHash implementation or PHP's built-in hash functions.
     * 
     * @param string $input Input string
     * @return int Hash value
     */
    private function sipHash24(string $input): int
    {
        // Use CRC32C as a simple hash (not cryptographically secure, but sufficient for load balancing)
        // For better distribution, you can use murmurhash or xxhash extensions
        if (function_exists('hash')) {
            $hash = hash('crc32c', $input, true);
            return unpack('l', $hash)[1];
        }
        
        // Fallback: use crc32
        return crc32($input);
    }
    
    /**
     * Convert Endpoints object to string for comparison
     * 
     * @param object|null $endpoints Endpoints object
     * @return string Endpoint string representation
     */
    private function getEndpointsString($endpoints): string
    {
        if ($endpoints === null) {
            return '';
        }
        
        // Try to get addresses from endpoints
        if (method_exists($endpoints, 'getAddresses')) {
            $addresses = $endpoints->getAddresses();
            if (is_array($addresses) && !empty($addresses)) {
                // Return first address as string
                $firstAddress = $addresses[0];
                if (method_exists($firstAddress, 'getHost') && method_exists($firstAddress, 'getPort')) {
                    return $firstAddress->getHost() . ':' . $firstAddress->getPort();
                }
            }
        }
        
        // Fallback: convert to string
        return (string)$endpoints;
    }
}
