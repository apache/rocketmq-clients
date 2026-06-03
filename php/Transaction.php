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

/**
 * Transaction - Tracks half-messages and manages commit/rollback lifecycle.
 *
 * Referencing Java TransactionImpl:
 * 1. tryAddMessage() - add a message to this transaction before sending
 * 2. tryAddReceipt() - record the send result after the half-message is sent
 * 3. commit() - commit all half-messages in this transaction
 * 4. rollback() - rollback all half-messages in this transaction
 */
class Transaction
{
    private array $messages = [];
    private array $receipts = [];
    private bool $committed = false;
    private bool $rolledBack = false;

    public function __construct(
        private readonly TransactionCommitter $committer
    ) {}

    /**
     * Add a message to this transaction before sending.
     *
     * @param object $message Message protobuf object
     * @return void
     * @throws \RuntimeException If transaction is already terminated
     * @throws \InvalidArgumentException If a message was already added
     */
    public function tryAddMessage($message): void
    {
        if ($this->committed || $this->rolledBack) {
            throw new \RuntimeException("Transaction is already terminated");
        }

        if (!empty($this->messages)) {
            throw new \InvalidArgumentException("Transaction only supports one message at a time");
        }

        $this->messages[] = $message;
    }

    /**
     * Record the send result after the half-message is sent.
     *
     * @param object $message Message protobuf object
     * @param array $sendResult ['messageId' => ..., 'transactionId' => ...]
     * @param object|null $endpoints Endpoints for the sent message
     * @return void
     * @throws \RuntimeException If transaction is terminated or send result is invalid
     * @throws \InvalidArgumentException If message is not part of this transaction
     */
    public function tryAddReceipt($message, array $sendResult, $endpoints = null): void
    {
        if ($this->committed || $this->rolledBack) {
            throw new \RuntimeException("Transaction is already terminated");
        }

        $messageId = $sendResult['messageId'] ?? '';
        $transactionId = $sendResult['transactionId'] ?? '';

        if (empty($messageId) || empty($transactionId)) {
            throw new \RuntimeException("Invalid send result: messageId and transactionId are required");
        }

        if (!in_array($message, $this->messages, true)) {
            throw new \InvalidArgumentException("Message is not part of this transaction");
        }

        $this->receipts[] = [
            'messageId' => $messageId,
            'transactionId' => $transactionId,
            'topic' => $message->getTopic()->getName(),
            'endpoints' => $endpoints,
        ];
    }

    /**
     * Alias for tryAddReceipt.
     *
     * @param object $message Message protobuf object
     * @param array $sendResult ['messageId' => ..., 'transactionId' => ...]
     * @return void
     * @throws \RuntimeException If transaction is terminated or send result invalid
     * @throws \InvalidArgumentException If message is not part of this transaction
     */
    public function addReceipt($message, array $sendResult): void
    {
        $this->tryAddReceipt($message, $sendResult);
    }

    /**
     * Commit all half-messages in this transaction.
     *
     * @return void
     * @throws \RuntimeException If transaction is already terminated or has no receipts
     */
    public function commit(): void
    {
        if ($this->committed || $this->rolledBack) {
            throw new \RuntimeException("Transaction is already terminated");
        }

        if (empty($this->receipts)) {
            throw new \RuntimeException("No receipts to commit");
        }

        foreach ($this->receipts as $receipt) {
            $this->committer->commitTransaction(
                $receipt['messageId'],
                $receipt['transactionId'],
                $receipt['topic'],
                $receipt['endpoints'] ?? null
            );
        }

        $this->committed = true;
        $this->messages = [];
        $this->receipts = [];
    }

    /**
     * Rollback all half-messages in this transaction.
     *
     * @return void
     * @throws \RuntimeException If transaction is already terminated or has no receipts
     */
    public function rollback(): void
    {
        if ($this->committed || $this->rolledBack) {
            throw new \RuntimeException("Transaction is already terminated");
        }

        if (empty($this->receipts)) {
            throw new \RuntimeException("No receipts to rollback");
        }

        foreach ($this->receipts as $receipt) {
            $this->committer->rollbackTransaction(
                $receipt['messageId'],
                $receipt['transactionId'],
                $receipt['topic'],
                $receipt['endpoints'] ?? null
            );
        }

        $this->rolledBack = true;
        $this->messages = [];
        $this->receipts = [];
    }

    /**
     * Get tracked messages.
     *
     * @return array List of tracked messages
     */
    public function getMessages(): array
    {
        return $this->messages;
    }

    /**
     * Get tracked receipts.
     *
     * @return array List of tracked receipts
     */
    public function getReceipts(): array
    {
        return $this->receipts;
    }

    /**
     * Check if this transaction has been committed.
     *
     * @return bool True if committed, false otherwise
     */
    public function isCommitted(): bool
    {
        return $this->committed;
    }

    /**
     * Check if this transaction has been rolled back.
     *
     * @return bool True if rolled back, false otherwise
     */
    public function isRolledBack(): bool
    {
        return $this->rolledBack;
    }
}
