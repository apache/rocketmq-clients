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

use Apache\Rocketmq\Connection\ConnectionPool;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;

/**
 * ProcessQueue manages message receiving and caching for a specific message queue
 * 
 * Key responsibilities:
 * - Fetch messages from broker
 * - Cache messages locally
 * - Apply backpressure based on cache thresholds
 * - Track message statistics
 */
class ProcessQueue
{
    /**
     * @var PushConsumerImpl Parent push consumer
     */
    private $pushConsumer;
    
    /**
     * @var MessageQueue Message queue
     */
    private $messageQueue;
    
    /**
     * @var FilterExpression Filter expression
     */
    private $filterExpression;
    
    /**
     * @var bool Whether this process queue is dropped
     */
    private $dropped = false;
    
    /**
     * @var int Cached message count
     */
    private $cachedMessageCount = 0;
    
    /**
     * @var int Cached message size in bytes
     */
    private $cachedMessageSizeInBytes = 0;
    
    /**
     * @var array Cached messages
     */
    private $cachedMessages = [];
    
    /**
     * @var int Last fetch timestamp
     */
    private $lastFetchTimestamp = 0;
    
    /**
     * @var int Receive message count
     */
    private $receiveMessageCount = 0;
    
    /**
     * @var float Last consume timestamp
     */
    private $lastConsumeTimestamp = 0.0;
    
    /**
     * Constructor
     * 
     * @param PushConsumerImpl $pushConsumer Parent push consumer
     * @param MessageQueue $messageQueue Message queue
     * @param FilterExpression $filterExpression Filter expression
     */
    public function __construct(
        PushConsumerImpl $pushConsumer,
        MessageQueue $messageQueue,
        FilterExpression $filterExpression
    ) {
        $this->pushConsumer = $pushConsumer;
        $this->messageQueue = $messageQueue;
        $this->filterExpression = $filterExpression;
        $this->lastFetchTimestamp = time();
    }
    
    /**
     * Get message queue
     * 
     * @return MessageQueue Message queue
     */
    public function getMessageQueue(): MessageQueue
    {
        return $this->messageQueue;
    }
    
    /**
     * Check if this process queue is dropped
     * 
     * @return bool Whether dropped
     */
    public function isDropped(): bool
    {
        return $this->dropped;
    }
    
    /**
     * Drop this process queue
     * 
     * @return void
     */
    public function drop(): void
    {
        $this->dropped = true;
    }
    
    /**
     * Check if this process queue is expired (30 seconds without fetch)
     * 
     * @return bool Whether expired
     */
    public function expired(): bool
    {
        return (time() - $this->lastFetchTimestamp) > 30;
    }
    
    /**
     * Fetch message immediately
     * 
     * @return void
     */
    public function fetchMessageImmediately(): void
    {
        if ($this->dropped) {
            return;
        }
        
        try {
            $this->fetchMessage();
        } catch (\Exception $e) {
            error_log("Failed to fetch message: " . $e->getMessage());
        }
    }
    
    /**
     * Fetch message from broker
     * 
     * @return void
     * @throws \Exception If fetch fails
     */
    private function fetchMessage(): void
    {
        if ($this->dropped) {
            return;
        }
        
        // Check cache thresholds (backpressure)
        $countThreshold = $this->pushConsumer->cacheMessageCountThresholdPerQueue();
        $sizeThreshold = $this->pushConsumer->cacheMessageSizeThresholdPerQueue();
        
        if ($this->cachedMessageCount >= $countThreshold || 
            $this->cachedMessageSizeInBytes >= $sizeThreshold) {
            // Cache is full, wait before fetching more
            return;
        }
        
        $this->lastFetchTimestamp = time();
        
        // Build receive message request
        $request = new ReceiveMessageRequest();
        $request->setMessageQueue($this->messageQueue);
        $request->setFilterExpression($this->filterExpression);
        $request->setBatchSize(32); // Default batch size
        
        $duration = new \Google\Protobuf\Duration();
        $duration->setSeconds(30);
        $request->setInvisibleDuration($duration);
        
        // Send request
        $pool = ConnectionPool::getInstance();
        $client = $pool->getConnection($this->pushConsumer->getConfig());
        
        $call = $client->ReceiveMessage($request);
        
        $messages = [];
        foreach ($call->responses() as $response) {
            if ($response->hasMessage()) {
                $message = $response->getMessage();
                $messages[] = $message;
                
                // Update cache statistics
                $this->cachedMessageCount++;
                $messageSize = strlen($message->getBody() ?? '');
                $this->cachedMessageSizeInBytes += $messageSize;
            }
        }
        
        if (!empty($messages)) {
            $this->receiveMessageCount++;
            $this->pushConsumer->incrementReceptionTimes();
            $this->pushConsumer->incrementReceivedMessagesQuantity(count($messages));
            
            // Process messages
            $this->processMessages($messages);
        }
    }
    
    /**
     * Process received messages
     * 
     * @param array $messages Message list
     * @return void
     */
    private function processMessages(array $messages): void
    {
        $listener = $this->pushConsumer->getMessageListener();
        
        foreach ($messages as $message) {
            if ($this->dropped) {
                break;
            }
            
            try {
                $this->lastConsumeTimestamp = microtime(true);
                
                // Call message listener
                $result = $listener($message);
                
                // Acknowledge message if successful
                if ($result === ConsumeResult::SUCCESS || $result === true || $result === null) {
                    $this->ackMessage($message);
                    $this->pushConsumer->incrementConsumptionOkQuantity(1);
                } else {
                    $this->pushConsumer->incrementConsumptionErrorQuantity(1);
                }
            } catch (\Exception $e) {
                error_log("Failed to process message: " . $e->getMessage());
                $this->pushConsumer->incrementConsumptionErrorQuantity(1);
            } finally {
                // Remove from cache
                $this->cachedMessageCount--;
                $messageSize = strlen($message->getBody() ?? '');
                $this->cachedMessageSizeInBytes -= $messageSize;
            }
        }
    }
    
    /**
     * Acknowledge a message
     * 
     * @param mixed $message Message object
     * @return void
     */
    private function ackMessage($message): void
    {
        try {
            $receiptHandle = $message->getSystemProperties()->getReceiptHandle();
            
            if (empty($receiptHandle)) {
                return;
            }
            
            $entry = new \Apache\Rocketmq\V2\AckMessageEntry();
            $entry->setReceiptHandle($receiptHandle);
            $entry->setMessageId($message->getSystemProperties()->getMessageId());
            
            $request = new \Apache\Rocketmq\V2\AckMessageRequest();
            $request->setEntries([$entry]);
            
            $pool = ConnectionPool::getInstance();
            $client = $pool->getConnection($this->pushConsumer->getConfig());
            
            $call = $client->AckMessage($request);
            $call->wait();
        } catch (\Exception $e) {
            error_log("Failed to ack message: " . $e->getMessage());
        }
    }
    
    /**
     * Get cached message count
     * 
     * @return int Message count
     */
    public function getCachedMessageCount(): int
    {
        return $this->cachedMessageCount;
    }
    
    /**
     * Get cached message size in bytes
     * 
     * @return int Size in bytes
     */
    public function getCachedMessageSizeInBytes(): int
    {
        return $this->cachedMessageSizeInBytes;
    }
    
    /**
     * Get receive message count
     * 
     * @return int Count
     */
    public function getReceiveMessageCount(): int
    {
        return $this->receiveMessageCount;
    }
    
    /**
     * Do stats logging
     * 
     * @return void
     */
    public function doStats(): void
    {
        $topic = $this->messageQueue->getTopic()->getName();
        $broker = $this->messageQueue->getBroker()->getName() ?? 'unknown';
        $id = $this->messageQueue->getId() ?? 0;
        
        error_log(sprintf(
            "ProcessQueue stats: topic=%s, broker=%s, id=%d, cachedCount=%d, cachedSize=%d, receiveCount=%d",
            $topic,
            $broker,
            $id,
            $this->cachedMessageCount,
            $this->cachedMessageSizeInBytes,
            $this->receiveMessageCount
        ));
    }
}
