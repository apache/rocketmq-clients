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
     * @param MessageQueue[] $messageQueues Available message queues
     * @param string $consumerGroup Consumer group name
     * @param string $clientId Client ID
     * @return MessageQueue|null Selected message queue
     */
    public function select(array $messageQueues, string $consumerGroup, string $clientId): ?MessageQueue;

    /**
     * Get strategy name
     */
    public function getName(): string;
}

/**
 * Round robin load balancing strategy
 * 
 * Distributes messages evenly across all available queues in a round-robin manner.
 * Uses bounded index tracking to prevent unbounded memory growth.
 */
class RoundRobinStrategy implements LoadBalancerStrategy {
    /**
     * Maximum tracked consumer groups (prevents unbounded memory growth)
     */
    private const MAX_TRACKED_GROUPS = 500;

    /** @var array<string, int> Current index for each consumer group */
    private array $indices = [];

    public function select(array $messageQueues, string $consumerGroup, string $clientId): ?MessageQueue {
        if (empty($messageQueues)) {
            return null;
        }

        if (!isset($this->indices[$consumerGroup])) {
            // Evict oldest entry if at capacity
            if (count($this->indices) >= self::MAX_TRACKED_GROUPS) {
                // Remove first (oldest) entry
                $key = array_key_first($this->indices);
                if ($key !== null) {
                    unset($this->indices[$key]);
                }
            }
            $this->indices[$consumerGroup] = 0;
        }

        $count = count($messageQueues);
        $index = $this->indices[$consumerGroup] % $count;
        $this->indices[$consumerGroup] = $index + 1;

        return $messageQueues[$index];
    }

    public function getName(): string {
        return 'round_robin';
    }
}

/**
 * Random load balancing strategy
 */
class RandomStrategy implements LoadBalancerStrategy {
    public function select(array $messageQueues, string $consumerGroup, string $clientId): ?MessageQueue {
        if (empty($messageQueues)) {
            return null;
        }

        $index = random_int(0, count($messageQueues) - 1);
        return $messageQueues[$index];
    }

    public function getName(): string {
        return 'random';
    }
}

/**
 * Least active load balancing strategy
 * 
 * Selects the message queue with the least active consumption.
 * Uses bounded tracking to prevent unbounded memory growth.
 */
class LeastActiveStrategy implements LoadBalancerStrategy {
    /**
     * Maximum tracked keys (prevents unbounded memory growth)
     */
    private const MAX_TRACKED_KEYS = 500;

    /** @var array<string, array<string, int>> Active count for each consumer group + client */
    private array $activeCounts = [];

    public function select(array $messageQueues, string $consumerGroup, string $clientId): ?MessageQueue {
        if (empty($messageQueues)) {
            return null;
        }

        $key = $consumerGroup . ':' . $clientId;
        if (!isset($this->activeCounts[$key])) {
            // Evict oldest entry if at capacity
            if (count($this->activeCounts) >= self::MAX_TRACKED_KEYS) {
                $oldKey = array_key_first($this->activeCounts);
                if ($oldKey !== null) {
                    unset($this->activeCounts[$oldKey]);
                }
            }
            $this->activeCounts[$key] = [];
        }

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

        if ($selectedQueue !== null) {
            $mqKey = $this->getMessageQueueKey($selectedQueue);
            $this->activeCounts[$key][$mqKey] = ($this->activeCounts[$key][$mqKey] ?? 0) + 1;
        }

        return $selectedQueue;
    }

    /**
     * Decrement active count for a message queue
     */
    public function decrementActiveCount(MessageQueue $messageQueue, string $consumerGroup, string $clientId): void {
        $key = $consumerGroup . ':' . $clientId;
        $mqKey = $this->getMessageQueueKey($messageQueue);

        if (isset($this->activeCounts[$key][$mqKey]) && $this->activeCounts[$key][$mqKey] > 0) {
            $this->activeCounts[$key][$mqKey]--;
        }
    }

    /**
     * Generate unique key for MessageQueue
     * 
     * Format: topic:broker:queueId
     * Used to track active message queues per consumer.
     * 
     * @param MessageQueue $messageQueue Message queue object
     * @return string Unique key
     */
    private function getMessageQueueKey(MessageQueue $messageQueue): string {
        $topic = $messageQueue->getTopic()->getName();
        $broker = $messageQueue->getBroker()->getName() ?? 'unknown';
        $queueId = $messageQueue->getId() ?? -1;
        return "{$topic}:{$broker}:{$queueId}";
    }

    public function getName(): string {
        return 'least_active';
    }
}

/**
 * Hash load balancing strategy
 * 
 * Selects message queue based on hash of client ID.
 * Uses abs() to handle negative crc32 values on 32-bit systems.
 */
class HashStrategy implements LoadBalancerStrategy {
    public function select(array $messageQueues, string $consumerGroup, string $clientId): ?MessageQueue {
        if (empty($messageQueues)) {
            return null;
        }

        $hash = abs(crc32($clientId));
        $index = $hash % count($messageQueues);

        return $messageQueues[$index];
    }

    public function getName(): string {
        return 'hash';
    }
}

/**
 * Load balancer factory
 */
class LoadBalancerFactory {
    /** @var array<string, LoadBalancerStrategy> */
    private static array $strategies = [];

    /**
     * @throws \InvalidArgumentException If strategy not found
     */
    public static function getStrategy(string $strategyName): LoadBalancerStrategy {
        if (isset(self::$strategies[$strategyName])) {
            return self::$strategies[$strategyName];
        }

        $strategy = match ($strategyName) {
            'round_robin' => new RoundRobinStrategy(),
            'random' => new RandomStrategy(),
            'least_active' => new LeastActiveStrategy(),
            'hash' => new HashStrategy(),
            default => throw new \InvalidArgumentException("Unknown load balancing strategy: {$strategyName}"),
        };

        self::$strategies[$strategyName] = $strategy;
        return $strategy;
    }

    public static function getDefaultStrategy(): LoadBalancerStrategy {
        return self::getStrategy('round_robin');
    }

    /**
     * @return string[]
     */
    public static function getAvailableStrategies(): array {
        return ['round_robin', 'random', 'least_active', 'hash'];
    }
}
