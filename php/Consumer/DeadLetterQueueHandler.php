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
use Apache\Rocketmq\V2\ForwardMessageToDeadLetterQueueRequest;
use Apache\Rocketmq\V2\ForwardMessageToDeadLetterQueueResponse;
use Apache\Rocketmq\V2\Resource;

/**
 * DeadLetterQueueHandler - Handles forwarding failed messages to DLQ
 * 
 * Reference Java dead letter queue handling:
 * - Forwards messages that exceed max retry attempts to DLQ
 * - Tracks DLQ forwarding statistics
 * - Supports async DLQ forwarding
 */
class DeadLetterQueueHandler
{
    /**
     * @var PushConsumerImpl Push consumer
     */
    private $pushConsumer;
    
    /**
     * @var int DLQ forwarding success count
     */
    private $dlqSuccessCount = 0;
    
    /**
     * @var int DLQ forwarding failure count
     */
    private $dlqFailureCount = 0;
    
    /**
     * Constructor
     * 
     * @param PushConsumerImpl $pushConsumer Push consumer
     */
    public function __construct(PushConsumerImpl $pushConsumer)
    {
        $this->pushConsumer = $pushConsumer;
    }
    
    /**
     * Forward message to dead letter queue
     * 
     * @param mixed $message Message to forward
     * @param int $deliveryAttempt Current delivery attempt
     * @param int $maxDeliveryAttempts Maximum delivery attempts
     * @return bool Whether forwarding was successful
     */
    public function forwardToDeadLetterQueue(
        $message,
        int $deliveryAttempt,
        int $maxDeliveryAttempts
    ): bool {
        try {
            $request = $this->buildForwardRequest(
                $message,
                $deliveryAttempt,
                $maxDeliveryAttempts
            );
            
            $pool = ConnectionPool::getInstance();
            $client = $pool->getConnection($this->pushConsumer->getConfig());
            
            $call = $client->ForwardMessageToDeadLetterQueue($request);
            $response = $call->wait();
            
            if ($response instanceof ForwardMessageToDeadLetterQueueResponse) {
                $status = $response->getStatus();
                if ($status && $status->getCode() === 0) {
                    $this->dlqSuccessCount++;
                    error_log("Message forwarded to DLQ successfully: " . 
                        $message->getSystemProperties()->getMessageId());
                    return true;
                }
            }
            
            $this->dlqFailureCount++;
            error_log("Failed to forward message to DLQ: " . 
                $message->getSystemProperties()->getMessageId());
            return false;
        } catch (\Exception $e) {
            $this->dlqFailureCount++;
            error_log("Exception while forwarding message to DLQ: " . $e->getMessage());
            return false;
        }
    }
    
    /**
     * Build forward to DLQ request
     * 
     * @param mixed $message Message
     * @param int $deliveryAttempt Delivery attempt
     * @param int $maxDeliveryAttempts Max delivery attempts
     * @return ForwardMessageToDeadLetterQueueRequest
     */
    private function buildForwardRequest(
        $message,
        int $deliveryAttempt,
        int $maxDeliveryAttempts
    ): ForwardMessageToDeadLetterQueueRequest {
        $request = new ForwardMessageToDeadLetterQueueRequest();
        
        $systemProperties = $message->getSystemProperties();
        
        // Set topic
        $topicResource = Resource::create();
        $topicResource->setName($systemProperties->getTopic());
        $request->setTopic($topicResource);
        
        // Set group
        $groupResource = Resource::create();
        $groupResource->setName($this->pushConsumer->getConsumerGroup());
        $request->setGroup($groupResource);
        
        // Set message ID and receipt handle
        $request->setMessageId($systemProperties->getMessageId());
        $request->setReceiptHandle($systemProperties->getReceiptHandle());
        
        // Set delivery attempt info
        $request->setDeliveryAttempt($deliveryAttempt);
        $request->setMaxDeliveryAttempts($maxDeliveryAttempts);
        
        return $request;
    }
    
    /**
     * Get DLQ success count
     * 
     * @return int
     */
    public function getDlqSuccessCount(): int
    {
        return $this->dlqSuccessCount;
    }
    
    /**
     * Get DLQ failure count
     * 
     * @return int
     */
    public function getDlqFailureCount(): int
    {
        return $this->dlqFailureCount;
    }
    
    /**
     * Reset counters
     * 
     * @return void
     */
    public function resetCounters(): void
    {
        $this->dlqSuccessCount = 0;
        $this->dlqFailureCount = 0;
    }
}
