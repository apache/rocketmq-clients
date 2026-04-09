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

use Apache\Rocketmq\V2\MessageQueue;

/**
 * Load balancing strategy interface
 * 
 * Refer to Java client implementation, provides load balancing strategies for message consumption
 */
interface LoadBalancerStrategy {
    /**
     * Select a message queue from available queues
     * 
     * @param array $messageQueues Available message queues
     * @param string $consumerGroup Consumer group name
     * @param string $clientId Client ID
     * @return MessageQueue|null Selected message queue
     */
    public function select(array $messageQueues, string $consumerGroup, string $clientId): ?MessageQueue;
    
    /**
     * Get strategy name
     * 
     * @return string Strategy name
     */
    public function getName(): string;
}

/**
 * Round robin load balancing strategy
 * 
 * Distributes messages evenly across all available queues in a round-robin manner
 */
class RoundRobinStrategy implements LoadBalancerStrategy {
    /**
     * @var array Current index for each consumer group
     */
    private $indices = [];
    
    /**
     * Select a message queue
     * 
     * @param array $messageQueues Available message queues
     * @param string $consumerGroup Consumer group name
     * @param string $clientId Client ID
     * @return MessageQueue|null Selected message queue
     */
    public function select(array $messageQueues, string $consumerGroup, string $clientId): ?MessageQueue {
        if (empty($messageQueues)) {
            return null;
        }
        
        // Initialize index for consumer group if not exists
        if (!isset($this->indices[$consumerGroup])) {
            $this->indices[$consumerGroup] = 0;
        }
        
        // Get current index and increment
        $index = $this->indices[$consumerGroup];
        $this->indices[$consumerGroup] = ($index + 1) % count($messageQueues);
        
        return $messageQueues[$index];
    }
    
    /**
     * Get strategy name
     * 
     * @return string Strategy name
     */
    public function getName(): string {
        return 'round_robin';
    }
}

/**
 * Random load balancing strategy
 * 
 * Randomly selects a message queue from available queues
 */
class RandomStrategy implements LoadBalancerStrategy {
    /**
     * Select a message queue
     * 
     * @param array $messageQueues Available message queues
     * @param string $consumerGroup Consumer group name
     * @param string $clientId Client ID
     * @return MessageQueue|null Selected message queue
     */
    public function select(array $messageQueues, string $consumerGroup, string $clientId): ?MessageQueue {
        if (empty($messageQueues)) {
            return null;
        }
        
        $index = mt_rand(0, count($messageQueues) - 1);
        return $messageQueues[$index];
    }
    
    /**
     * Get strategy name
     * 
     * @return string Strategy name
     */
    public function getName(): string {
        return 'random';
    }
}

/**
 * Least active load balancing strategy
 * 
 * Selects the message queue with the least active consumption
 */
class LeastActiveStrategy implements LoadBalancerStrategy {
    /**
     * @var array Active count for each message queue
     */
    private $activeCounts = [];
    
    /**
     * Select a message queue
     * 
     * @param array $messageQueues Available message queues
     * @param string $consumerGroup Consumer group name
     * @param string $clientId Client ID
     * @return MessageQueue|null Selected message queue
     */
    public function select(array $messageQueues, string $consumerGroup, string $clientId): ?MessageQueue {
        if (empty($messageQueues)) {
            return null;
        }
        
        // Initialize active counts if not exists
        $key = $consumerGroup . ':' . $clientId;
        if (!isset($this->activeCounts[$key])) {
            $this->activeCounts[$key] = [];
            foreach ($messageQueues as $mq) {
                $mqKey = $this->getMessageQueueKey($mq);
                $this->activeCounts[$key][$mqKey] = 0;
            }
        }
        
        // Find queue with least active count
        $minCount = PHP_INT_MAX;
        $selectedQueue = null;
        
        foreach ($messageQueues as $mq) {
            $mqKey = $this->getMessageQueueKey($mq);
            $count = $this->activeCounts[$key][$mqKey] ?? 0;
            
            if ($count < $minCount) {
                $minCount = $count;
                $selectedQueue = $mq;
            }
        }
        
        // Increment active count for selected queue
        if ($selectedQueue !== null) {
            $mqKey = $this->getMessageQueueKey($selectedQueue);
            $this->activeCounts[$key][$mqKey]++;
        }
        
        return $selectedQueue;
    }
    
    /**
     * Decrement active count for a message queue
     * 
     * @param MessageQueue $messageQueue Message queue
     * @param string $consumerGroup Consumer group name
     * @param string $clientId Client ID
     * @return void
     */
    public function decrementActiveCount(MessageQueue $messageQueue, string $consumerGroup, string $clientId): void {
        $key = $consumerGroup . ':' . $clientId;
        $mqKey = $this->getMessageQueueKey($messageQueue);
        
        if (isset($this->activeCounts[$key][$mqKey]) && $this->activeCounts[$key][$mqKey] > 0) {
            $this->activeCounts[$key][$mqKey]--;
        }
    }
    
    /**
     * Get message queue key
     * 
     * @param MessageQueue $messageQueue Message queue
     * @return string Message queue key
     */
    private function getMessageQueueKey(MessageQueue $messageQueue): string {
        $topic = $messageQueue->getTopic()->getName();
        $broker = $messageQueue->getBroker()->getName();
        $queueId = $messageQueue->getId();
        return "{$topic}:{$broker}:{$queueId}";
    }
    
    /**
     * Get strategy name
     * 
     * @return string Strategy name
     */
    public function getName(): string {
        return 'least_active';
    }
}

/**
 * Hash load balancing strategy
 * 
 * Selects message queue based on hash of message key
 */
class HashStrategy implements LoadBalancerStrategy {
    /**
     * Select a message queue
     * 
     * @param array $messageQueues Available message queues
     * @param string $consumerGroup Consumer group name
     * @param string $clientId Client ID
     * @param string|null $messageKey Message key for hashing
     * @return MessageQueue|null Selected message queue
     */
    public function select(array $messageQueues, string $consumerGroup, string $clientId, ?string $messageKey = null): ?MessageQueue {
        if (empty($messageQueues)) {
            return null;
        }
        
        // Use clientId as hash key if message key not provided
        $hashKey = $messageKey ?? $clientId;
        $hash = crc32($hashKey);
        $index = $hash % count($messageQueues);
        
        return $messageQueues[$index];
    }
    
    /**
     * Get strategy name
     * 
     * @return string Strategy name
     */
    public function getName(): string {
        return 'hash';
    }
}

/**
 * Load balancer factory
 * 
 * Creates load balancer strategies based on configuration
 */
class LoadBalancerFactory {
    /**
     * @var array Strategy instances
     */
    private static $strategies = [];
    
    /**
     * Get load balancer strategy by name
     * 
     * @param string $strategyName Strategy name
     * @return LoadBalancerStrategy Load balancer strategy
     * @throws \InvalidArgumentException If strategy not found
     */
    public static function getStrategy(string $strategyName): LoadBalancerStrategy {
        if (isset(self::$strategies[$strategyName])) {
            return self::$strategies[$strategyName];
        }
        
        switch ($strategyName) {
            case 'round_robin':
                $strategy = new RoundRobinStrategy();
                break;
            case 'random':
                $strategy = new RandomStrategy();
                break;
            case 'least_active':
                $strategy = new LeastActiveStrategy();
                break;
            case 'hash':
                $strategy = new HashStrategy();
                break;
            default:
                throw new \InvalidArgumentException("Unknown load balancing strategy: {$strategyName}");
        }
        
        self::$strategies[$strategyName] = $strategy;
        return $strategy;
    }
    
    /**
     * Get default strategy
     * 
     * @return LoadBalancerStrategy Default load balancer strategy
     */
    public static function getDefaultStrategy(): LoadBalancerStrategy {
        return self::getStrategy('round_robin');
    }
    
    /**
     * Get available strategies
     * 
     * @return array Available strategy names
     */
    public static function getAvailableStrategies(): array {
        return ['round_robin', 'random', 'least_active', 'hash'];
    }
}
