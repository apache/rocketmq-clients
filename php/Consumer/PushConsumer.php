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

use Apache\Rocketmq\Exception\ClientException;

/**
 * Push consumer interface
 * 
 * Push consumer is a thread-safe and fully-managed rocketmq client which is used to consume messages by the group
 */
interface PushConsumer {
    /**
     * Get the load balancing group for the consumer
     * 
     * @return string Consumer load balancing group
     */
    public function getConsumerGroup(): string;
    
    /**
     * List the existed subscription expressions in push consumer
     * 
     * @return array Collections of the subscription expression
     */
    public function getSubscriptionExpressions(): array;
    
    /**
     * Add subscription expression dynamically
     * 
     * @param string $topic Topic name
     * @param FilterExpression $filterExpression Filter expression
     * @return PushConsumer Push consumer instance
     * @throws ClientException If an error occurs
     */
    public function subscribe(string $topic, FilterExpression $filterExpression): PushConsumer;
    
    /**
     * Remove subscription expression dynamically by topic
     * 
     * @param string $topic The topic to remove the subscription
     * @return PushConsumer Push consumer instance
     * @throws ClientException If an error occurs
     */
    public function unsubscribe(string $topic): PushConsumer;
    
    /**
     * Start the push consumer
     * 
     * @return void
     * @throws ClientException If an error occurs
     */
    public function start(): void;
    
    /**
     * Shutdown the push consumer
     * 
     * @return void
     * @throws ClientException If an error occurs
     */
    public function shutdown(): void;
    
    /**
     * Check if the push consumer is running
     * 
     * @return bool True if the push consumer is running, false otherwise
     */
    public function isRunning(): bool;
}
