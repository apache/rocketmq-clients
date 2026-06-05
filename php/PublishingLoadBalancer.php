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
use Apache\Rocketmq\V2\Permission;

/**
 * PublishingLoadBalancer - Route-level load balancer for message publishing.
 *
 * Referencing Java's PublishingLoadBalancer:
 * 1. Filters to writable queues (WRITE or READ_WRITE permission) on master broker
 * 2. Round-robin index with random initial position
 * 3. takeMessageQueueByMessageGroup() - deterministic queue selection by message group (FIFO)
 * 4. takeMessageQueue() - round-robin with broker name exclusion
 */
class PublishingLoadBalancer
{
    private int $index;
    private array $messageQueues = [];

    /**
     * Initialize the load balancer with writable message queues from route data.
     *
     * @param object $routeData TopicRouteData / QueryRouteResponse with getMessageQueues()
     * @throws \InvalidArgumentException if no writable message queue is found
     */
    public function __construct($routeData)
    {
        $this->index = mt_rand(0, PHP_INT_MAX);

        $allQueues = $routeData->getMessageQueues();
        foreach ($allQueues as $mq) {
            $permission = $mq->getPermission();
            if (($permission === Permission::WRITE || $permission === Permission::READ_WRITE)
            && $mq->getBroker()->getId() === ClientConstants::MASTER_BROKER_ID) {
                $this->messageQueues[] = $mq;
            }
        }

        if (empty($this->messageQueues)) {
            throw new \InvalidArgumentException("No writable message queue found");
        }
    }

    /**
     * Update with new route data while preserving the round-robin index.
     * Matching Java's PublishingLoadBalancer.update() which reuses the AtomicInteger index.
     *
     * @param object $routeData New TopicRouteData
     * @return PublishingLoadBalancer New instance with updated queues and preserved index
     */
    public function update($routeData): PublishingLoadBalancer
    {
        $updated = new self($routeData);
        $updated->index = $this->index;
        return $updated;
    }

    /**
     * Deterministic queue selection by message group (for FIFO messages).
     * Uses hash of message group to ensure same group always maps to same queue.
     *
     * @param string $messageGroup
     * @return object|null MessageQueue
     */
    public function takeMessageQueueByMessageGroup(string $messageGroup): ?object
    {
        if (empty($this->messageQueues)) {
            return null;
        }

        // Simple hash of message group string
        $hash = SipHash24::hash($messageGroup);
        $index = IntMath::mod($hash, count($this->messageQueues));

        return $this->messageQueues[$index];
    }

    /**
     * Round-robin queue selection with broker name exclusion.
     * Excludes brokers in the isolated list on first pass, falls back to all brokers if none available.
     *
     * @param array $excludedBrokerNames Set of broker names to exclude (e.g. isolated/throttled)
     * @param int $count Number of queues to take
     * @return array Array of MessageQueue objects
     */
    public function takeMessageQueue(array $excludedBrokerNames, int $count): array
    {
        if (empty($this->messageQueues)) {
            return [];
        }

        $queueCount = count($this->messageQueues);
        $next = $this->index++;
        $candidates = [];
        $candidateBrokerNames = [];

        // First pass: exclude isolated brokers
        for ($i = 0; $i < $queueCount; $i++) {
            $mq = $this->messageQueues[($next + $i) % $queueCount];
            $brokerName = $mq->getBroker()->getName();
            if (!in_array($brokerName, $excludedBrokerNames, true) && !in_array($brokerName, $candidateBrokerNames, true)) {
                $candidateBrokerNames[] = $brokerName;
                $candidates[] = $mq;
            }
            if (count($candidates) >= $count) {
                return $candidates;
            }
        }

        // Second pass: all brokers (fallback when all endpoints are isolated)
        if (empty($candidates)) {
            for ($i = 0; $i < $queueCount; $i++) {
                $mq = $this->messageQueues[($next + $i) % $queueCount];
                $brokerName = $mq->getBroker()->getName();
                if (!in_array($brokerName, $candidateBrokerNames, true)) {
                    $candidateBrokerNames[] = $brokerName;
                    $candidates[] = $mq;
                }
                if (count($candidates) >= $count) {
                    break;
                }
            }
        }

        return $candidates;
    }

    /**
     * Get all writable message queues.
     *
     * @return array Array of writable MessageQueue objects
     */
    public function getMessageQueues(): array
    {
        return $this->messageQueues;
    }

    /**
     * Get all unique broker names from message queues.
     *
     * @return string[] Array of broker names
     */
    public function getAllBrokerNames(): array
    {
        $brokerNames = [];
        foreach ($this->messageQueues as $mq) {
            $brokerName = $mq->getBroker()->getName();
            if (!in_array($brokerName, $brokerNames, true)) {
                $brokerNames[] = $brokerName;
            }
        }
        return $brokerNames;
    }

    /**
     * Validate message type against queue's accept message types.
     *
     * @param object $messageQueue MessageQueue protobuf object
     * @param int $messageType Message type to validate
     * @param string $topic Topic name for error message
     * @return void
     * @throws \InvalidArgumentException if message type is not accepted by the queue
     */
    public function validateMessageTypeAgainstQueue(object $messageQueue, int $messageType, string $topic): void
    {
        if (!method_exists($messageQueue, 'getAcceptMessageTypes')) {
            return;
        }

        $acceptTypes = $messageQueue->getAcceptMessageTypes();
        if (is_object($acceptTypes) && method_exists($acceptTypes, 'getIterator')) {
            $acceptTypes = iterator_to_array($acceptTypes);
        }

        if (empty($acceptTypes)) {
            return;
        }

        if (!in_array($messageType, $acceptTypes, true)) {
            throw new \InvalidArgumentException(
                "Message type not accepted for topic={$topic}, actual={$messageType}, accept=" .
                implode(',', array_map('strval', $acceptTypes))
            );
        }
    }
}
