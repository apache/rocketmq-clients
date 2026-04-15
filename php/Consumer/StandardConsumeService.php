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
 * StandardConsumeService - Standard message consumption service
 * 
 * Reference Java StandardConsumeService:
 * - Processes messages in parallel using thread pool
 * - No ordering guarantee
 * - Suitable for most use cases
 */
class StandardConsumeService implements ConsumeService
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
     * @var bool Whether the service is running
     */
    private $running = false;
    
    /**
     * Constructor
     * 
     * @param string $clientId Client ID
     * @param callable $messageListener Message listener
     * @param PushConsumerImpl $pushConsumer Push consumer
     */
    public function __construct(
        string $clientId,
        callable $messageListener,
        PushConsumerImpl $pushConsumer
    ) {
        $this->clientId = $clientId;
        $this->messageListener = $messageListener;
        $this->pushConsumer = $pushConsumer;
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
        
        try {
            $result = call_user_func($this->messageListener, $message);
            
            if ($result === ConsumeResult::SUCCESS || $result === true || $result === null) {
                return ConsumeResult::SUCCESS;
            }
            
            return ConsumeResult::FAILURE;
        } catch (\Exception $e) {
            error_log("[" . $this->clientId . "] Exception while consuming message: " . $e->getMessage());
            return ConsumeResult::FAILURE;
        }
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
}
