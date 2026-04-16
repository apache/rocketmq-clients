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
 * Message Hook Points - Defines when interceptors are invoked
 * 
 * Reference: Java MessageHookPoints enum
 */
class MessageHookPoints {
    /**
     * Before sending message
     */
    const SEND_BEFORE = 1;
    
    /**
     * After sending message
     */
    const SEND_AFTER = 2;
    
    /**
     * Before receiving message
     */
    const RECEIVE_BEFORE = 3;
    
    /**
     * After receiving message
     */
    const RECEIVE_AFTER = 4;
    
    /**
     * Before consuming message
     */
    const CONSUME_BEFORE = 5;
    
    /**
     * After consuming message
     */
    const CONSUME_AFTER = 6;
    
    /**
     * Before acknowledging message
     */
    const ACK_BEFORE = 7;
    
    /**
     * After acknowledging message
     */
    const ACK_AFTER = 8;
    
    /**
     * Before changing invisible duration
     */
    const CHANGE_INVISIBLE_DURATION_BEFORE = 9;
    
    /**
     * After changing invisible duration
     */
    const CHANGE_INVISIBLE_DURATION_AFTER = 10;
    
    /**
     * Before committing transaction
     */
    const COMMIT_TRANSACTION_BEFORE = 11;
    
    /**
     * After committing transaction
     */
    const COMMIT_TRANSACTION_AFTER = 12;
    
    /**
     * Before rolling back transaction
     */
    const ROLLBACK_TRANSACTION_BEFORE = 13;
    
    /**
     * After rolling back transaction
     */
    const ROLLBACK_TRANSACTION_AFTER = 14;
    
    /**
     * Before forwarding message to DLQ
     */
    const FORWARD_TO_DLQ_BEFORE = 15;
    
    /**
     * After forwarding message to DLQ
     */
    const FORWARD_TO_DLQ_AFTER = 16;
    
    /**
     * Get hook point name
     * 
     * @param int $hookPoint Hook point constant
     * @return string Hook point name
     */
    public static function getName(int $hookPoint): string {
        $names = [
            self::SEND_BEFORE => 'SEND_BEFORE',
            self::SEND_AFTER => 'SEND_AFTER',
            self::RECEIVE_BEFORE => 'RECEIVE_BEFORE',
            self::RECEIVE_AFTER => 'RECEIVE_AFTER',
            self::CONSUME_BEFORE => 'CONSUME_BEFORE',
            self::CONSUME_AFTER => 'CONSUME_AFTER',
            self::ACK_BEFORE => 'ACK_BEFORE',
            self::ACK_AFTER => 'ACK_AFTER',
            self::CHANGE_INVISIBLE_DURATION_BEFORE => 'CHANGE_INVISIBLE_DURATION_BEFORE',
            self::CHANGE_INVISIBLE_DURATION_AFTER => 'CHANGE_INVISIBLE_DURATION_AFTER',
            self::COMMIT_TRANSACTION_BEFORE => 'COMMIT_TRANSACTION_BEFORE',
            self::COMMIT_TRANSACTION_AFTER => 'COMMIT_TRANSACTION_AFTER',
            self::ROLLBACK_TRANSACTION_BEFORE => 'ROLLBACK_TRANSACTION_BEFORE',
            self::ROLLBACK_TRANSACTION_AFTER => 'ROLLBACK_TRANSACTION_AFTER',
            self::FORWARD_TO_DLQ_BEFORE => 'FORWARD_TO_DLQ_BEFORE',
            self::FORWARD_TO_DLQ_AFTER => 'FORWARD_TO_DLQ_AFTER',
        ];
        
        return $names[$hookPoint] ?? 'UNKNOWN';
    }
}
