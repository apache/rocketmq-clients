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
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\EndTransactionRequest;
use Apache\Rocketmq\V2\TransactionResolution as V2TransactionResolution;
use Apache\Rocketmq\V2\TransactionSource;
use Google\Protobuf\Timestamp;

/**
 * Transaction implementation class
 * 
 * Implements the Transaction interface for handling transaction messages
 * Based on Java client implementation:
 * - Maintains a set of messages in the transaction
 * - Tracks send receipts for each message
 * - Commits or rollbacks all messages atomically
 */
class TransactionImpl implements Transaction {
    /**
     * @var Producer Producer instance
     */
    private $producer;
    
    /**
     * @var array List of messages in transaction [messageId => ['message' => MessageAdapter, 'receipt' => SendReceipt]]
     */
    private $messages = [];
    
    /**
     * @var bool Whether the transaction is committed
     */
    private $committed = false;
    
    /**
     * @var bool Whether the transaction is rolled back
     */
    private $rolledBack = false;
    
    /**
     * Constructor
     * 
     * @param Producer $producer Producer instance
     */
    public function __construct(Producer $producer) {
        $this->producer = $producer;
    }
    
    /**
     * Try to add message to transaction
     * 
     * @param object $message Message to add
     * @return object The same message object
     * @throws \InvalidArgumentException If transaction already has a message (max 1)
     */
    public function tryAddMessage($message) {
        if (count($this->messages) >= 1) {
            throw new \InvalidArgumentException(
                "Message in transaction has exceeded the threshold: 1"
            );
        }
        
        return $message;
    }
    
    /**
     * Try to add receipt to transaction
     * 
     * @param object $message Message object
     * @param SendReceipt $receipt Send receipt
     * @throws \InvalidArgumentException If message not in transaction
     */
    public function tryAddReceipt($message, SendReceipt $receipt) {
        // For simplicity, we use messageId as key
        $messageId = $receipt->getMessageId();
        $this->messages[$messageId] = [
            'message' => $message,
            'receipt' => $receipt,
        ];
    }
    
    /**
     * {@inheritdoc}
     */
    public function commit(): void {
        if ($this->committed || $this->rolledBack) {
            return; // Already committed or rolled back
        }
        
        if (empty($this->messages)) {
            throw new ClientException("Transactional message has not been sent yet");
        }
        
        try {
            foreach ($this->messages as $msgData) {
                $receipt = $msgData['receipt'];
                $this->endTransaction($receipt, TransactionResolution::COMMIT);
            }
            
            $this->committed = true;
        } catch (\Exception $e) {
            throw new ClientException("Failed to commit transaction: " . $e->getMessage(), 0, $e);
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function rollback(): void {
        if ($this->committed || $this->rolledBack) {
            return; // Already committed or rolled back
        }
        
        if (empty($this->messages)) {
            throw new ClientException("Transactional message has not been sent yet");
        }
        
        try {
            foreach ($this->messages as $msgData) {
                $receipt = $msgData['receipt'];
                $this->endTransaction($receipt, TransactionResolution::ROLLBACK);
            }
            
            $this->rolledBack = true;
        } catch (\Exception $e) {
            throw new ClientException("Failed to rollback transaction: " . $e->getMessage(), 0, $e);
        }
    }
    
    /**
     * End transaction (internal method)
     * 
     * @param SendReceipt $receipt Send receipt
     * @param TransactionResolution $resolution Transaction resolution
     * @throws ClientException If ending transaction fails
     */
    private function endTransaction(SendReceipt $receipt, TransactionResolution $resolution) {
        // Build EndTransactionRequest
        $request = new EndTransactionRequest();
        
        // Set message ID
        $messageId = $receipt->getMessageId();
        $request->setMessageId($messageId);
        
        // Set transaction ID (from receipt, which comes from broker response)
        $transactionId = $receipt->getTransactionId();
        if ($transactionId === null || $transactionId === '') {
            // Fallback to messageId if transactionId is not available
            $transactionId = $messageId;
        }
        $request->setTransactionId($transactionId);
        
        // Set topic
        $topicResource = new Resource();
        $topicResource->setName($receipt->getTopic());
        $request->setTopic($topicResource);
        
        // Set resolution
        if ($resolution === TransactionResolution::COMMIT) {
            $request->setResolution(V2TransactionResolution::COMMIT);
        } else {
            $request->setResolution(V2TransactionResolution::ROLLBACK);
        }
        
        // Set source (client-initiated commit/rollback)
        $request->setSource(TransactionSource::SOURCE_CLIENT);
        
        // Get endpoints from the send receipt
        // This is critical: we must send commit/rollback to the same broker that handled the half message
        $endpoints = null;
        if (method_exists($receipt, 'getEndpoints')) {
            $endpoints = $receipt->getEndpoints();
        }
        
        // Call Producer's internal method to end transaction
        $this->producer->endTransactionInternal(
            $endpoints,  // Pass endpoints to ensure correct broker is targeted
            $messageId,
            $transactionId,
            $resolution,
            $receipt->getTopic()  // Pass topic from receipt
        );
    }
    
    /**
     * Get send receipt
     * 
     * @return SendReceipt|null Send receipt or null if no messages
     */
    public function getSendReceipt(): ?SendReceipt {
        if (empty($this->messages)) {
            return null;
        }
        // Return the first receipt
        $firstMsgData = reset($this->messages);
        return $firstMsgData['receipt'];
    }
    
    /**
     * Check if the transaction is committed
     * 
     * @return bool True if committed, false otherwise
     */
    public function isCommitted(): bool {
        return $this->committed;
    }
    
    /**
     * Check if the transaction is rolled back
     * 
     * @return bool True if rolled back, false otherwise
     */
    public function isRolledBack(): bool {
        return $this->rolledBack;
    }
    
    /**
     * Get string representation
     * 
     * @return string
     */
    public function __toString(): string {
        $status = "PENDING";
        if ($this->committed) {
            $status = "COMMITTED";
        } elseif ($this->rolledBack) {
            $status = "ROLLED_BACK";
        }
        
        $messageCount = count($this->messages);
        return sprintf("Transaction[messageCount=%d, status=%s]", $messageCount, $status);
    }
}
