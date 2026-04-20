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

/**
 * Send receipt class
 * 
 * Represents the result of sending a message
 */
class SendReceipt {
    /**
     * @var string Message ID
     */
    private $messageId;
    
    /**
     * @var string Topic
     */
    private $topic;
    
    /**
     * @var int Queue ID
     */
    private $queueId;
    
    /**
     * @var int Offset
     */
    private $offset;
    
    /**
     * @var string|null Transaction ID (for transaction messages)
     */
    private $transactionId;
    
    /**
     * @var string|null Recall handle (for message recall)
     */
    private $recallHandle;
    
    /**
     * @var \Apache\Rocketmq\Route\MessageQueue Message queue with broker information
     */
    private $messageQueue;
    
    /**
     * Constructor
     * 
     * Aligned with Java SendReceiptImpl constructor.
     * 
     * @param string $messageId Message ID
     * @param string $topic Topic
     * @param int $queueId Queue ID
     * @param int $offset Offset
     * @param string|null $transactionId Transaction ID (for transaction messages)
     * @param string|null $recallHandle Recall handle (for message recall)
     * @param \Apache\Rocketmq\Route\MessageQueue $messageQueue Message queue with broker information
     */
    public function __construct(
        string $messageId, 
        string $topic, 
        int $queueId, 
        int $offset,
        ?string $transactionId = null,
        ?string $recallHandle = null,
        \Apache\Rocketmq\Route\MessageQueue $messageQueue = null
    ) {
        $this->messageId = $messageId;
        $this->topic = $topic;
        $this->queueId = $queueId;
        $this->offset = $offset;
        $this->transactionId = $transactionId;
        $this->recallHandle = $recallHandle;
        $this->messageQueue = $messageQueue;
    }
    
    /**
     * Get message ID
     * 
     * @return string Message ID
     */
    public function getMessageId(): string {
        return $this->messageId;
    }
    
    /**
     * Get topic
     * 
     * @return string Topic
     */
    public function getTopic(): string {
        return $this->topic;
    }
    
    /**
     * Get queue ID
     * 
     * @return int Queue ID
     */
    public function getQueueId(): int {
        return $this->queueId;
    }
    
    /**
     * Get offset
     * 
     * @return int Offset
     */
    public function getOffset(): int {
        return $this->offset;
    }
    
    /**
     * Get transaction ID
     * 
     * @return string|null Transaction ID (null for non-transaction messages)
     */
    public function getTransactionId(): ?string {
        return $this->transactionId;
    }
    
    /**
     * Get recall handle
     * 
     * @return string|null Recall handle (null if not available)
     */
    public function getRecallHandle(): ?string {
        return $this->recallHandle;
    }
    
    /**
     * Get endpoints from message queue's broker
     * 
     * Aligned with Java: messageQueue.getBroker().getEndpoints()
     * 
     * @return \Apache\Rocketmq\Route\Endpoints|null Endpoints object or null if messageQueue not set
     */
    public function getEndpoints(): ?\Apache\Rocketmq\Route\Endpoints
    {
        return $this->messageQueue !== null ? $this->messageQueue->getBroker()->getEndpoints() : null;
    }
    
    /**
     * Get message queue
     * 
     * @return \Apache\Rocketmq\Route\MessageQueue|null Message queue or null if not set
     */
    public function getMessageQueue(): ?\Apache\Rocketmq\Route\MessageQueue
    {
        return $this->messageQueue;
    }
    
    /**
     * Get string representation
     * 
     * @return string
     */
    public function __toString(): string {
        $str = sprintf("SendReceipt[messageId=%s, topic=%s, queueId=%d, offset=%d", 
            $this->messageId, $this->topic, $this->queueId, $this->offset);
        
        if ($this->transactionId !== null) {
            $str .= ", transactionId=" . $this->transactionId;
        }
        
        if ($this->recallHandle !== null) {
            $str .= ", recallHandle=" . $this->recallHandle;
        }
        
        $str .= "]";
        return $str;
    }
}
