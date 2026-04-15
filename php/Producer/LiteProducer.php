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

namespace Apache\Rocketmq\Producer;

use Apache\Rocketmq\Exception\ClientException;
use Apache\Rocketmq\Message\MessageInterface;

/**
 * Lite producer interface
 * 
 * Similar to regular Producer but optimized for lite topics.
 * Lite topics are sub-topics under a parent topic that share resources
 * and have simplified management.
 */
interface LiteProducer {
    /**
     * Start the producer
     * 
     * @throws ClientException If an error occurs during startup
     */
    public function start(): void;
    
    /**
     * Shutdown the producer gracefully
     * 
     * @throws ClientException If an error occurs during shutdown
     */
    public function shutdown(): void;
    
    /**
     * Send a lite message synchronously
     * 
     * @param MessageInterface $message Message to send
     * @return SendReceipt Send receipt containing message ID and other metadata
     * @throws ClientException If sending fails
     * @throws \Apache\Rocketmq\Exception\LiteTopicQuotaExceededException If lite topic quota is exceeded
     */
    public function send(MessageInterface $message): SendReceipt;
    
    /**
     * Send a lite message asynchronously
     * 
     * @param MessageInterface $message Message to send
     * @param callable|null $callback Callback function(receipt, error)
     * @return mixed Async call object or null
     * @throws ClientException If sending fails
     */
    public function sendAsync(MessageInterface $message, ?callable $callback = null);
    
    /**
     * Get the client ID
     * 
     * @return string Client ID
     */
    public function getClientId(): string;
    
    /**
     * Check if the producer is running
     * 
     * @return bool True if running, false otherwise
     */
    public function isRunning(): bool;
}
