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

use Apache\Rocketmq\V2\TransactionResolution;

/**
 * LocalTransactionExecuter - Executes local business logic within a transaction message flow.
 *
 * Java reference: LocalTransactionExecuter in rocketmq-client-java.
 * The flow is:
 * 1. Client sends half-message to broker (message not visible to consumers)
 * 2. Local transaction is executed via this interface
 * 3. Based on execution result, client commits or rolls back the half-message
 *
 * If the client crashes before step 3, the server sends RecoverOrphanedTransactionCommand,
 * and the TransactionChecker is invoked to resolve the pending transaction.
 */
interface LocalTransactionExecuter
{
    /**
     * Execute local business logic after half-message is sent.
     *
     * @param MessageView $messageView The half-message that was sent
     * @return int TransactionResolution::COMMIT, ::ROLLBACK, or ::UNKNOWN
     */
    public function execute(MessageView $messageView): int;
}
