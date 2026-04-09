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
 * Lite push consumer interface
 * 
 * Similar to PushConsumer but for lite topics
 * 
 * @see PushConsumer
 */
interface LitePushConsumer {
    /**
     * Subscribe to a lite topic
     * 
     * @param string $liteTopic The name of the lite topic to subscribe
     * @param callable $messageListener Message listener callback
     * @param string $filterExpression Filter expression (optional)
     * @param string $filterType Filter type (optional, default: TAG)
     * @throws ClientException If an error occurs during subscription
     */
    public function subscribeLite(string $liteTopic, callable $messageListener, string $filterExpression = '', string $filterType = 'TAG'): void;
    
    /**
     * Subscribe to a lite topic with offset option
     * 
     * @param string $liteTopic The name of the lite topic to subscribe
     * @param callable $messageListener Message listener callback
     * @param string $offsetOption Consume from offset option: EARLIEST, LATEST, TIMESTAMP
     * @param string $filterExpression Filter expression (optional)
     * @param string $filterType Filter type (optional, default: TAG)
     * @throws ClientException If an error occurs during subscription
     */
    public function subscribeLiteWithOffset(string $liteTopic, callable $messageListener, string $offsetOption, string $filterExpression = '', string $filterType = 'TAG'): void;
    
    /**
     * Unsubscribe from a lite topic
     * 
     * @param string $liteTopic The name of the lite topic to unsubscribe from
     * @throws ClientException If an error occurs during unsubscription
     */
    public function unsubscribeLite(string $liteTopic): void;
    
    /**
     * Get the lite topic set
     * 
     * @return array Lite topic set
     */
    public function getLiteTopicSet(): array;
    
    /**
     * Get the consumer group
     * 
     * @return string Consumer group name
     */
    public function getConsumerGroup(): string;
    
    /**
     * Start the consumer
     * 
     * @throws ClientException If an error occurs during startup
     */
    public function start(): void;
    
    /**
     * Shutdown the consumer
     * 
     * @throws ClientException If an error occurs during shutdown
     */
    public function shutdown(): void;
    
    /**
     * Check if the consumer is running
     * 
     * @return bool True if the consumer is running, false otherwise
     */
    public function isRunning(): bool;
}
