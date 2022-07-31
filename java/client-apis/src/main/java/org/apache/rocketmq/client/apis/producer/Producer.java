/*
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

package org.apache.rocketmq.client.apis.producer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.Message;

/**
 * Producer is a thread-safe rocketmq client which is used to publish messages.
 *
 * <p>On account of network timeout or other reasons, the producer only promised the at-least-once semantics.
 * For the producer, at-least-once semantics means potentially attempts are made at sending it, messages may be
 * duplicated but not lost. Especially, potentially attempts are not made using {@link #send(Message, Transaction)}.
 */
public interface Producer extends Closeable {
    /**
     * Sends a message synchronously.
     *
     * <p>This method does not return until it gets the definitive result.
     *
     * @param message the message to send.
     * @return the returned receipt.
     */
    SendReceipt send(Message message) throws ClientException;

    /**
     * Sends a transactional message synchronously.
     *
     * @param message     the message to send.
     * @param transaction the transaction to bind.
     * @return the returned receipt.
     */
    SendReceipt send(Message message, Transaction transaction) throws ClientException;

    /**
     * Sends a message asynchronously.
     *
     * <p>This method returns immediately, the result is included in the {@link CompletableFuture};
     *
     * @param message the message to send.
     * @return a future that indicates the send receipt.
     */
    CompletableFuture<SendReceipt> sendAsync(Message message);

    /**
     * Begins a transaction.
     *
     * <p>For example:
     *
     * <pre>{@code
     * Transaction transaction = producer.beginTransaction();
     * SendReceipt receipt1 = producer.send(message1, transaction);
     * SendReceipt receipt2 = producer.send(message2, transaction);
     * transaction.commit();
     * }</pre>
     *
     * @return a transaction entity to execute commit/rollback operation.
     */
    Transaction beginTransaction() throws ClientException;

    /**
     * Closes the producer and releases all related resources.
     *
     * <p>Once producer is closed, <strong>it could not be started once again.</strong> we maintained an FSM
     * (finite-state machine) to record the different states for each producer.
     */
    @Override
    void close() throws IOException;
}
