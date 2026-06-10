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
 * SubscriptionLoadBalancer - Round-robin load balancer for message consumption.
 *
 * Filters readable queues (READ or READ_WRITE permission) from route data
 * and provides round-robin MessageQueue selection.
 */
class SubscriptionLoadBalancer
{
    private array $messageQueues = [];
    private int $queueIndex = 0;

    /**
     * Initialize with route data, filtering readable master queues.
     *
     * @param object $routeData Route data object containing message queues with broker and permission info
     */
    public function __construct($routeData)
    {
        if ($routeData instanceof \Apache\Rocketmq\V2\QueryRouteResponse) {
            $allQueues = $routeData->getMessageQueues();
            $readableCount = 0;
            $masterCount = 0;

            // Accept READ or READ_WRITE for consumer
            foreach ($allQueues as $queue) {
                $permission = $queue->getPermission();
                $isMaster = $queue->getBroker()->getId() === ClientConstants::MASTER_BROKER_ID;
                if (($permission === Permission::READ || $permission === Permission::READ_WRITE) && $isMaster) {
                    $this->messageQueues[] = $queue;
                    $masterCount++;
                }
                $readableCount++;
            }

            Logger::getInstance('SubscriptionLoadBalancer')->info("Topic queues: {$masterCount} readable / {$readableCount} total");
        }
    }

    /**
     * Get next MessageQueue using round-robin selection.
     *
     * @return object|null The next MessageQueue or null if none available
     */
    public function takeMessageQueue()
    {
        if (empty($this->messageQueues)) {
            Logger::getInstance('SubscriptionLoadBalancer')->warning("No message queues available");
            return null;
        }

        $index = $this->queueIndex++ % count($this->messageQueues);
        $queue = $this->messageQueues[$index];

        Logger::getInstance('SubscriptionLoadBalancer')->debug("Selected queue index: {$index}");

        return $queue;
    }

    /**
     * Get all readable message queues.
     *
     * @return array List of filtered MessageQueue objects
     */
    public function getMessageQueues()
    {
        return $this->messageQueues;
    }
}
