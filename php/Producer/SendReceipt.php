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
 * Send receipt class
 * 
 * Represents the result of sending a message
 */
class SendReceipt {
    /**
     * @var string Message ID
     */
    private $messageId;
    
    /**
     * @var string Topic
     */
    private $topic;
    
    /**
     * @var int Queue ID
     */
    private $queueId;
    
    /**
     * @var int Offset
     */
    private $offset;
    
    /**
     * Constructor
     * 
     * @param string $messageId Message ID
     * @param string $topic Topic
     * @param int $queueId Queue ID
     * @param int $offset Offset
     */
    public function __construct(string $messageId, string $topic, int $queueId, int $offset) {
        $this->messageId = $messageId;
        $this->topic = $topic;
        $this->queueId = $queueId;
        $this->offset = $offset;
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
     * Get topic
     * 
     * @return string Topic
     */
    public function getTopic(): string {
        return $this->topic;
    }
    
    /**
     * Get queue ID
     * 
     * @return int Queue ID
     */
    public function getQueueId(): int {
        return $this->queueId;
    }
    
    /**
     * Get offset
     * 
     * @return int Offset
     */
    public function getOffset(): int {
        return $this->offset;
    }
    
    /**
     * Get string representation
     * 
     * @return string
     */
    public function __toString(): string {
        return sprintf("SendReceipt[messageId=%s, topic=%s, queueId=%d, offset=%d]", 
            $this->messageId, $this->topic, $this->queueId, $this->offset);
    }
}
