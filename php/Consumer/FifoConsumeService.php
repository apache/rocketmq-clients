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
 * FifoConsumeService - FIFO message consumption service
 * 
 * Reference Java FifoConsumeService:
 * - Processes messages in FIFO order within same message group
 * - Messages with same messageGroup are processed sequentially
 * - Different message groups can be processed in parallel
 * - Supports FIFO consume accelerator for better performance
 */
class FifoConsumeService implements ConsumeService
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
     * @var bool Enable FIFO consume accelerator
     */
    private $enableFifoConsumeAccelerator;
    
    /**
     * @var array<string, bool> Message group locks (for sequential processing)
     */
    private $messageGroupLocks = [];
    
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
     * @param bool $enableFifoConsumeAccelerator Enable FIFO consume accelerator
     */
    public function __construct(
        string $clientId,
        callable $messageListener,
        PushConsumerImpl $pushConsumer,
        bool $enableFifoConsumeAccelerator = false
    ) {
        $this->clientId = $clientId;
        $this->messageListener = $messageListener;
        $this->pushConsumer = $pushConsumer;
        $this->enableFifoConsumeAccelerator = $enableFifoConsumeAccelerator;
    }
    
    /**
     * {@inheritdoc}
     */
    public function start(): void
    {
        $this->running = true;
    }
    
    /**
     * {@inheritdoc}
     */
    public function shutdown(): void
    {
        $this->running = false;
    }
    
    /**
     * {@inheritdoc}
     */
    public function consume($message): ConsumeResult
    {
        if (!$this->running) {
            return ConsumeResult::FAILURE;
        }
        
        // Get message group
        $messageGroup = $this->getMessageGroup($message);
        
        // Acquire lock for this message group
        $this->acquireMessageGroupLock($messageGroup);
        
        try {
            $result = call_user_func($this->messageListener, $message);
            
            if ($result === ConsumeResult::SUCCESS || $result === true || $result === null) {
                return ConsumeResult::SUCCESS;
            }
            
            return ConsumeResult::FAILURE;
        } catch (\Exception $e) {
            error_log("[" . $this->clientId . "] Exception while consuming FIFO message: " . $e->getMessage());
            return ConsumeResult::FAILURE;
        } finally {
            // Release lock for this message group
            $this->releaseMessageGroupLock($messageGroup);
        }
    }
    
    /**
     * Get message group from message
     * 
     * @param mixed $message Message
     * @return string Message group
     */
    private function getMessageGroup($message): string
    {
        try {
            $systemProperties = $message->getSystemProperties();
            if (method_exists($systemProperties, 'getMessageGroup')) {
                $group = $systemProperties->getMessageGroup();
                if (!empty($group)) {
                    return $group;
                }
            }
        } catch (\Exception $e) {
            // Ignore
        }
        
        return '__DEFAULT_GROUP__';
    }
    
    /**
     * Acquire lock for message group
     * 
     * @param string $messageGroup Message group
     * @return void
     */
    private function acquireMessageGroupLock(string $messageGroup): void
    {
        // Simple spin lock implementation
        $maxWaitTime = 30; // 30 seconds timeout
        $startTime = time();
        
        while (isset($this->messageGroupLocks[$messageGroup])) {
            if (time() - $startTime > $maxWaitTime) {
                error_log("[" . $this->clientId . "] Timeout waiting for message group lock: " . $messageGroup);
                break;
            }
            usleep(10000); // 10ms
        }
        
        $this->messageGroupLocks[$messageGroup] = true;
    }
    
    /**
     * Release lock for message group
     * 
     * @param string $messageGroup Message group
     * @return void
     */
    private function releaseMessageGroupLock(string $messageGroup): void
    {
        unset($this->messageGroupLocks[$messageGroup]);
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
     * Check if FIFO consume accelerator is enabled
     * 
     * @return bool
     */
    public function isEnableFifoConsumeAccelerator(): bool
    {
        return $this->enableFifoConsumeAccelerator;
    }
}
