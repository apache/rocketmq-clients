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

namespace Apache\Rocketmq;

use Apache\Rocketmq\V2\Message as V2Message;

/**
 * Transaction object
 * 
 * Used to manage commit and rollback of transactional messages
 * A transaction can contain multiple half messages
 * 
 * Usage example:
 * $transaction = $producer->beginTransaction();
 * try {
 *     // Send half message
 *     $receipt = $producer->sendHalfMessage($message, $transaction);
 *     
 *     // Execute local transaction
 *     executeLocalTransaction();
 *     
 *     // Commit transaction
 *     $transaction->commit();
 * } catch (\Exception $e) {
 *     // Rollback transaction
 *     $transaction->rollback();
 * }
 */
class Transaction
{
    /**
     * @var Producer|null Producer instance (weak reference)
     */
    private $producer;
    
    /**
     * @var array List of sent half messages [messageId => ['message' => V2Message, 'receipt' => array]]
     */
    private $messages = [];
    
    /**
     * @var string Transaction state: CREATED, RUNNING, COMMITTING, COMMITTED, ROLLING_BACK, ROLLED_BACK
     */
    private $state = 'CREATED';
    
    /**
     * @var string Transaction ID
     */
    private $transactionId;
    
    /**
     * Constructor
     * 
     * @param Producer $producer Producer instance
     */
    public function __construct($producer)
    {
        $this->producer = $producer;
        $this->transactionId = $this->generateTransactionId();
    }
    
    /**
     * Generate transaction ID
     * 
     * @return string Transaction ID
     */
    private function generateTransactionId()
    {
        return sprintf(
            '%s-%s-%d-%s',
            gethostname(),
            date('YmdHis'),
            getmypid(),
            bin2hex(random_bytes(8))
        );
    }
    
    /**
     * Add half message to transaction
     * 
     * @param V2Message $message Message object
     * @param array $receipt Send receipt
     * @return void
     * @throws \Exception If transaction state is incorrect
     */
    public function addMessage($message, $receipt)
    {
        if ($this->state !== 'CREATED' && $this->state !== 'RUNNING') {
            throw new \Exception("Cannot add message to transaction in state: {$this->state}");
        }
        
        $messageId = $receipt['messageId'];
        $this->messages[$messageId] = [
            'message' => $message,
            'receipt' => $receipt,
        ];
        
        $this->state = 'RUNNING';
    }
    
    /**
     * Commit transaction
     * 
     * All half messages will be visible to consumers
     * 
     * @return void
     * @throws \Exception If commit fails
     */
    public function commit()
    {
        if (empty($this->messages)) {
            throw new \Exception("No messages in transaction to commit");
        }
        
        if ($this->state === 'COMMITTED' || $this->state === 'ROLLING_BACK' || $this->state === 'ROLLED_BACK') {
            throw new \Exception("Transaction already in terminal state: {$this->state}");
        }
        
        $this->state = 'COMMITTING';
        
        try {
            foreach ($this->messages as $messageId => $msgData) {
                $receipt = $msgData['receipt'];
                $this->endTransaction($receipt, TransactionResolution::COMMIT);
            }
            
            $this->state = 'COMMITTED';
        } catch (\Exception $e) {
            $this->state = 'RUNNING'; // Restore state, allow retry
            throw new \Exception("Failed to commit transaction: " . $e->getMessage(), 0, $e);
        }
    }
    
    /**
     * Rollback transaction
     * 
     * All half messages will be deleted
     * 
     * @return void
     * @throws \Exception If rollback fails
     */
    public function rollback()
    {
        if (empty($this->messages)) {
            throw new \Exception("No messages in transaction to rollback");
        }
        
        if ($this->state === 'COMMITTED' || $this->state === 'ROLLING_BACK' || $this->state === 'ROLLED_BACK') {
            throw new \Exception("Transaction already in terminal state: {$this->state}");
        }
        
        $this->state = 'ROLLING_BACK';
        
        try {
            foreach ($this->messages as $messageId => $msgData) {
                $receipt = $msgData['receipt'];
                $this->endTransaction($receipt, TransactionResolution::ROLLBACK);
            }
            
            $this->state = 'ROLLED_BACK';
        } catch (\Exception $e) {
            $this->state = 'RUNNING'; // Restore state, allow retry
            throw new \Exception("Failed to rollback transaction: " . $e->getMessage(), 0, $e);
        }
    }
    
    /**
     * End transaction (internal method)
     * 
     * @param array $receipt Send receipt
     * @param string $resolution Transaction resolution (COMMIT/ROLLBACK)
     * @return void
     * @throws \Exception If ending transaction fails
     */
    private function endTransaction($receipt, $resolution)
    {
        if ($this->producer === null) {
            throw new \Exception("Producer is not available");
        }
        
        // Call Producer's endTransaction method
        $this->producer->endTransactionInternal(
            $receipt['target'],
            $receipt['messageId'],
            $receipt['transactionId'],
            $resolution
        );
    }
    
    /**
     * Get transaction ID
     * 
     * @return string Transaction ID
     */
    public function getTransactionId()
    {
        return $this->transactionId;
    }
    
    /**
     * Get transaction state
     * 
     * @return string Transaction state
     */
    public function getState()
    {
        return $this->state;
    }
    
    /**
     * Get message count
     * 
     * @return int Message count
     */
    public function getMessageCount()
    {
        return count($this->messages);
    }
    
    /**
     * Check if transaction is completed
     * 
     * @return bool Whether completed
     */
    public function isCompleted()
    {
        return $this->state === 'COMMITTED' || $this->state === 'ROLLED_BACK';
    }
}
