<?php
declare(strict_types=1);
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

namespace Apache\Rocketmq\Message;

/**
 * GeneralMessage combines Message and MessageView interfaces
 * 
 * References Java GeneralMessage interface (147 lines)
 * 
 * Used for message interceptors to handle both sending and received messages
 */
interface GeneralMessage
{
    /**
     * Get the unique id of the message
     * 
     * @return MessageId|null Unique id, null means not assigned yet
     */
    public function getMessageId(): ?MessageId;
    
    /**
     * Get the topic of the message
     * 
     * @return string Topic name
     */
    public function getTopic(): string;
    
    /**
     * Get the message body
     * 
     * @return string Message body
     */
    public function getBody(): string;
    
    /**
     * Get message properties (deep copy)
     * 
     * @return array Message properties
     */
    public function getProperties(): array;
    
    /**
     * Get the tag of the message
     * 
     * @return string|null Tag or null
     */
    public function getTag(): ?string;
    
    /**
     * Get the key collection
     * 
     * @return string[] Keys
     */
    public function getKeys(): array;
    
    /**
     * Get the message group (for FIFO)
     * 
     * @return string|null Message group or null
     */
    public function getMessageGroup(): ?string;
    
    /**
     * Get the lite topic (for LITE type)
     * 
     * @return string|null Lite topic or null
     */
    public function getLiteTopic(): ?string;
    
    /**
     * Get the delivery timestamp (for DELAY)
     * 
     * @return int|null Delivery timestamp or null
     */
    public function getDeliveryTimestamp(): ?int;
    
    /**
     * Get the priority (for PRIORITY type)
     * 
     * @return int|null Priority or null
     */
    public function getPriority(): ?int;
    
    /**
     * Get the born host
     * 
     * @return string|null Born host or null
     */
    public function getBornHost(): ?string;
    
    /**
     * Get the born timestamp
     * 
     * @return int|null Born timestamp or null
     */
    public function getBornTimestamp(): ?int;
    
    /**
     * Get the delivery attempt
     * 
     * @return int Delivery attempt (default 0 for new messages)
     */
    public function getDeliveryAttempt(): int;
    
    /**
     * Get decode timestamp
     * 
     * @return int|null Decode timestamp or null
     */
    public function getDecodeTimestamp(): ?int;
    
    /**
     * Get transport delivery timestamp
     * 
     * @return int|null Transport delivery timestamp or null
     */
    public function getTransportDeliveryTimestamp(): ?int;
}
