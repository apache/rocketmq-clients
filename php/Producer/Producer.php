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
 * Producer is a thread-safe rocketmq client which is used to publish messages.
 *
 * <p>On account of network timeout or other reasons, the producer only promised the at-least-once semantics.
 * For the producer, at-least-once semantics means potentially attempts are made at sending it, messages may be
 * duplicated but not lost. Especially, potentially attempts are not made using {@link #send(Message, Transaction)}.
 */
interface Producer {
    /**
     * Sends a message synchronously.
     *
     * <p>This method does not return until it gets the definitive result.
     *
     * @param Message $message the message to send.
     * @param Transaction|null $transaction the transaction to bind, null for normal message.
     * @return SendReceipt the returned receipt.
     * @throws ClientException If an error occurs
     */
    public function send(Message $message, ?Transaction $transaction = null): SendReceipt;

    /**
     * Sends a message asynchronously.
     *
     * <p>This method returns immediately, the result is included in the future;
     *
     * @param Message $message the message to send.
     * @return mixed a future that indicates the send receipt.
     */
    public function sendAsync(Message $message);

    /**
     * Begins a transaction.
     *
     * <p>For example:
     *
     * <pre>{@code
     * $transaction = $producer->beginTransaction();
     * $receipt1 = $producer->send($message1, $transaction);
     * $receipt2 = $producer->send($message2, $transaction);
     * $transaction->commit();
     * }</pre>
     *
     * @return Transaction a transaction entity to execute commit/rollback operation.
     * @throws ClientException If an error occurs
     */
    public function beginTransaction(): Transaction;

    /**
     * Recall message synchronously, only delay message is supported for now.
     *
     * <pre>{@code
     * $receipt = $producer->send($message);
     * $recallHandle = $receipt->getRecallHandle();
     * }</pre>
     *
     * @param string $topic the topic of the operation
     * @param string $recallHandle the handle to identify a message to recall
     * @return RecallReceipt the returned receipt, or throw exception if response status is not OK.
     * @throws ClientException If an error occurs
     */
    public function recallMessage(string $topic, string $recallHandle): RecallReceipt;

    /**
     * Recall message asynchronously.
     *
     * <p>This method returns immediately, the result is included in the future;
     *
     * @param string $topic the topic of the operation
     * @param string $recallHandle the handle to identify a message to recall
     * @return mixed a future that indicates the receipt
     */
    public function recallMessageAsync(string $topic, string $recallHandle);

    /**
     * Closes the producer and releases all related resources.
     *
     * <p>Once producer is closed, <strong>it could not be started once again.</strong> we maintained an FSM
     * (finite-state machine) to record the different states for each producer.
     *
     * @return void
     */
    public function close();
}
