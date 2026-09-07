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
 * TransactionChecker - Callback for resolving orphaned transactions.
 * When the server sends a RecoverOrphanedTransactionCommand, this checker
 * is invoked to determine whether the transaction should be committed,
 * rolled back, or remain unknown.
 * Referencing Java TransactionChecker.java
 */
interface TransactionChecker
{
    /**
     * Check the resolution of a pending transactional message.
     *
     * @param MessageView $messageView The orphaned transactional message
     * @return int TransactionResolution constant (COMMIT, ROLLBACK, or UNKNOWN)
     */
    public function check(MessageView $messageView): int;
}
