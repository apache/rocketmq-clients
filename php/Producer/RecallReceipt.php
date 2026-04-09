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
 * Recall receipt class
 * 
 * Represents the result of recalling a message
 */
class RecallReceipt {
    /**
     * @var string Message ID
     */
    private $messageId;
    
    /**
     * @var bool Whether the message was successfully recalled
     */
    private $recalled;
    
    /**
     * Constructor
     * 
     * @param string $messageId Message ID
     * @param bool $recalled Whether the message was successfully recalled
     */
    public function __construct(string $messageId, bool $recalled) {
        $this->messageId = $messageId;
        $this->recalled = $recalled;
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
     * Check if the message was successfully recalled
     * 
     * @return bool Whether the message was successfully recalled
     */
    public function isRecalled(): bool {
        return $this->recalled;
    }
    
    /**
     * Get string representation
     * 
     * @return string
     */
    public function __toString(): string {
        return sprintf("RecallReceipt[messageId=%s, recalled=%s]", 
            $this->messageId, $this->recalled ? "true" : "false");
    }
}
