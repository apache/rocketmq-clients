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

/**
 * SwooleConsumeService - Swoole-based concurrent message consumption
 * 
 * Integrates with Swoole extension for true concurrent message processing:
 * - Uses Swoole coroutine for async message consumption
 * - Supports configurable concurrency
 * - Better performance than PHP traditional threading
 * - Requires Swoole extension (pecl install swoole)
 * 
 * Usage:
 * if (extension_loaded('swoole')) {
 *     $service = new SwooleConsumeService($clientId, $listener, $consumer, 20);
 *     $service->start();
 * }
 */
class SwooleConsumeService implements ConsumeService
{
    /**
     * @var string Client ID
     */
    private $clientId;
    
    /**
     * @var callable Message listener
     */
    private $messageListener;
    
    /**
     * @var PushConsumerImpl Push consumer
     */
    private $pushConsumer;
    
    /**
     * @var int Concurrency (number of coroutines)
     */
    private $concurrency;
    
    /**
     * @var \Swoole\Coroutine\Channel Message channel
     */
    private $channel;
    
    /**
     * @var array Coroutine IDs
     */
    private $coroutineIds = [];
    
    /**
     * @var bool Whether the service is running
     */
    private $running = false;
    
    /**
     * Constructor
     * 
     * @param string $clientId Client ID
     * @param callable $messageListener Message listener
     * @param PushConsumerImpl $pushConsumer Push consumer
     * @param int $concurrency Number of concurrent coroutines
     * @throws \Exception If Swoole extension is not loaded
     */
    public function __construct(
        string $clientId,
        callable $messageListener,
        PushConsumerImpl $pushConsumer,
        int $concurrency = 20
    ) {
        if (!extension_loaded('swoole')) {
            throw new \Exception("Swoole extension is required for SwooleConsumeService. Install it with: pecl install swoole");
        }
        
        $this->clientId = $clientId;
        $this->messageListener = $messageListener;
        $this->pushConsumer = $pushConsumer;
        $this->concurrency = $concurrency;
    }
    
    /**
     * {@inheritdoc}
     */
    public function start(): void
    {
        if ($this->running) {
            return;
        }
        
        $this->running = true;
        $this->channel = new \Swoole\Coroutine\Channel(1024); // Buffer up to 1024 messages
        
        // Start consumer coroutines
        for ($i = 0; $i < $this->concurrency; $i++) {
            $coroutineId = \Swoole\Coroutine::create(function() use ($i) {
                $this->consumeLoop($i);
            });
            $this->coroutineIds[] = $coroutineId;
        }
        
        error_log("SwooleConsumeService started with {$this->concurrency} coroutines");
    }
    
    /**
     * Consume loop for each coroutine
     * 
     * @param int $workerId Worker ID
     * @return void
     */
    private function consumeLoop(int $workerId): void
    {
        error_log("Coroutine worker {$workerId} started");
        
        while ($this->running) {
            // Pop message from channel with timeout
            $message = $this->channel->pop(1.0); // 1 second timeout
            
            if ($message === false) {
                // Timeout, continue loop
                continue;
            }
            
            // Process message
            $this->processMessage($message, $workerId);
        }
        
        error_log("Coroutine worker {$workerId} stopped");
    }
    
    /**
     * Process a message
     * 
     * @param mixed $message Message
     * @param int $workerId Worker ID
     * @return void
     */
    private function processMessage($message, int $workerId): void
    {
        try {
            $result = call_user_func($this->messageListener, $message);
            
            if ($result === ConsumeResult::SUCCESS || $result === true || $result === null) {
                $this->ackMessage($message);
                $this->pushConsumer->incrementConsumptionOkQuantity(1);
            } else {
                $this->pushConsumer->incrementConsumptionErrorQuantity(1);
            }
        } catch (\Exception $e) {
            error_log("[{$this->clientId}] Worker {$workerId} exception: " . $e->getMessage());
            $this->pushConsumer->incrementConsumptionErrorQuantity(1);
        }
    }
    
    /**
     * Acknowledge a message
     * 
     * @param mixed $message Message
     * @return void
     */
    private function ackMessage($message): void
    {
        try {
            $receiptHandle = $message->getSystemProperties()->getReceiptHandle();
            
            if (empty($receiptHandle)) {
                return;
            }
            
            // Use Swoole coroutine for async ACK
            \Swoole\Coroutine::create(function() use ($message, $receiptHandle) {
                $entry = new \Apache\Rocketmq\V2\AckMessageEntry();
                $entry->setReceiptHandle($receiptHandle);
                $entry->setMessageId($message->getSystemProperties()->getMessageId());
                
                $request = new \Apache\Rocketmq\V2\AckMessageRequest();
                $request->setEntries([$entry]);
                
                $pool = \Apache\Rocketmq\Connection\ConnectionPool::getInstance();
                $client = $pool->getConnection($this->pushConsumer->getConfig());
                
                $call = $client->AckMessage($request);
                $call->wait();
            });
        } catch (\Exception $e) {
            error_log("Failed to ack message: " . $e->getMessage());
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function consume($message): ConsumeResult
    {
        if (!$this->running) {
            return ConsumeResult::FAILURE;
        }
        
        // Push message to channel for coroutine processing
        $pushed = $this->channel->push($message, 0.1); // 100ms timeout
        
        if ($pushed === false) {
            error_log("[" . $this->clientId . "] Failed to push message to channel, channel might be full");
            return ConsumeResult::FAILURE;
        }
        
        return ConsumeResult::SUCCESS;
    }
    
    /**
     * {@inheritdoc}
     */
    public function shutdown(): void
    {
        if (!$this->running) {
            return;
        }
        
        $this->running = false;
        
        // Wait for all coroutines to finish
        foreach ($this->coroutineIds as $coroutineId) {
            \Swoole\Coroutine::join([$coroutineId], 30.0); // 30 seconds timeout
        }
        
        $this->coroutineIds = [];
        
        error_log("SwooleConsumeService shutdown complete");
    }
    
    /**
     * Check if service is running
     * 
     * @return bool
     */
    public function isRunning(): bool
    {
        return $this->running;
    }
    
    /**
     * Get concurrency
     * 
     * @return int
     */
    public function getConcurrency(): int
    {
        return $this->concurrency;
    }
}
