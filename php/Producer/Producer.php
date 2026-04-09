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
use Apache\Rocketmq\Message\Message;

/**
 * Producer interface
 * 
 * Producer is a thread-safe and fully-managed rocketmq client which is used to send messages
 */
interface Producer {
    /**
     * Send message
     * 
     * @param Message $message Message to send
     * @return SendReceipt Send receipt
     * @throws ClientException If an error occurs
     */
    public function send(Message $message): SendReceipt;
    
    /**
     * Send transaction message
     * 
     * @param Message $message Message to send
     * @return Transaction Transaction object
     * @throws ClientException If an error occurs
     */
    public function sendTransaction(Message $message): Transaction;
    
    /**
     * Start the producer
     * 
     * @return void
     * @throws ClientException If an error occurs
     */
    public function start(): void;
    
    /**
     * Shutdown the producer
     * 
     * @return void
     * @throws ClientException If an error occurs
     */
    public function shutdown(): void;
    
    /**
     * Check if the producer is running
     * 
     * @return bool True if the producer is running, false otherwise
     */
    public function isRunning(): bool;
}
