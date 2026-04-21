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

namespace Apache\Rocketmq\Consumer;

use Apache\Rocketmq\Route\MessageQueue;

/**
 * Assignment represents a message queue assignment for PushConsumer.
 * 
 * Aligned with Java: org.apache.rocketmq.client.java.impl.consumer.Assignment
 */
class Assignment
{
    /**
     * @var MessageQueue The assigned message queue
     */
    private $messageQueue;
    
    /**
     * Constructor
     * 
     * @param MessageQueue $messageQueue The message queue to assign
     */
    public function __construct(MessageQueue $messageQueue)
    {
        $this->messageQueue = $messageQueue;
    }
    
    /**
     * Get the message queue
     * 
     * @return MessageQueue The assigned message queue
     */
    public function getMessageQueue(): MessageQueue
    {
        return $this->messageQueue;
    }
    
    /**
     * Check equality with another Assignment
     * 
     * @param Assignment $other The other assignment to compare
     * @return bool True if equal
     */
    public function equals(Assignment $other): bool
    {
        // Compare message queues
        return $this->messageQueue === $other->messageQueue || 
               $this->messageQueue == $other->messageQueue;
    }
    
    /**
     * Get string representation
     * 
     * @return string String representation
     */
    public function toString(): string
    {
        return sprintf(
            'Assignment{messageQueue=%s}',
            $this->messageQueue ? $this->messageQueue->toString() : 'null'
        );
    }
}
