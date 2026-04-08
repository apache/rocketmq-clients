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
 * Transaction resolution enumeration
 */
class TransactionResolution
{
    /**
     * Commit transaction - Message will be visible to consumers
     */
    const COMMIT = 'COMMIT';
    
    /**
     * Rollback transaction - Message will be deleted
     */
    const ROLLBACK = 'ROLLBACK';
    
    /**
     * Unknown status - Check again later
     */
    const UNKNOWN = 'UNKNOWN';
}

/**
 * Transaction checker interface
 * 
 * Used to check transaction status of half messages
 * When the server needs to confirm transaction status, it will call this checker
 * 
 * Usage example:
 * $checker = function($message) {
 *     // Determine transaction status based on business logic
 *     $transactionId = $message->getSystemProperties()->getTransactionId();
 *     
 *     // Query local transaction status
 *     $localTransactionStatus = checkLocalTransaction($transactionId);
 *     
 *     if ($localTransactionStatus === 'SUCCESS') {
 *         return TransactionResolution::COMMIT;
 *     } elseif ($localTransactionStatus === 'FAILED') {
 *         return TransactionResolution::ROLLBACK;
 *     } else {
 *         return TransactionResolution::UNKNOWN;
 *     }
 * };
 */
interface TransactionChecker
{
    /**
     * Check transaction status
     * 
     * @param V2Message $message Half message object
     * @return string Transaction status (COMMIT/ROLLBACK/UNKNOWN)
     * @throws \Exception If check fails
     */
    public function check($message);
}
