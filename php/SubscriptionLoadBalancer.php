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
    private $messageQueues = [];
    private $queueIndex = 0;

    public function __construct($routeData)
    {
        if ($routeData && method_exists($routeData, 'getMessageQueues')) {
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
     * Get next MessageQueue (round-robin)
     *
     * @return object|null MessageQueue
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
     * Get all MessageQueues
     *
     * @return array
     */
    public function getMessageQueues()
    {
        return $this->messageQueues;
    }
}
