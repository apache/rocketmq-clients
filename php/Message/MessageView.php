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

namespace Apache\Rocketmq\Message;

/**
 * MessageView provides a read-only view for the message, that's why setters do not exist here. In addition,
 * it only makes sense when Message is sent successfully.
 */
interface MessageView {
    /**
     * Get the unique id of the message
     * 
     * @return MessageId Unique id
     */
    public function getMessageId(): MessageId;
    
    /**
     * Get the topic of the message, which is the first classifier for the message
     * 
     * @return string Topic of the message
     */
    public function getTopic(): string;
    
    /**
     * Get the message body
     * 
     * @return string Message body
     */
    public function getBody(): string;
    
    /**
     * Get the message properties
     * 
     * @return array Message properties
     */
    public function getProperties(): array;
    
    /**
     * Get the tag of the message, which is the second classifier besides the topic
     * 
     * @return string|null The tag of message, null means tag does not exist
     */
    public function getTag(): ?string;
    
    /**
     * Get the key collection of the message
     * 
     * @return array Key collection of the message, empty array means message key is not specified
     */
    public function getKeys(): array;
    
    /**
     * Get the message group, which makes sense only when the topic type is FIFO(First In, First Out)
     * 
     * @return string|null Message group, null means message group is not specified
     */
    public function getMessageGroup(): ?string;
    
    /**
     * Get the lite topic, which makes sense only when the topic type is LITE
     * 
     * @return string|null Lite topic, null means lite topic is not specified
     */
    public function getLiteTopic(): ?string;
    
    /**
     * Get the expected delivery timestamp, which makes sense only when the topic type is delay
     * 
     * @return int|null Message expected delivery timestamp, null means delivery timestamp is not specified
     */
    public function getDeliveryTimestamp(): ?int;
    
    /**
     * Get the priority of the message, which makes sense only when topic type is priority
     * 
     * @return int|null Message priority, null means priority is not specified
     */
    public function getPriority(): ?int;
    
    /**
     * Get the born host of the message
     * 
     * @return string Born host of the message
     */
    public function getBornHost(): string;
    
    /**
     * Get the born timestamp of the message
     * 
     * Born time means the timestamp that the message is prepared to send rather than the timestamp the
     * Message was built.
     * 
     * @return int Born timestamp of the message
     */
    public function getBornTimestamp(): int;
    
    /**
     * Get the delivery attempt for the message
     * 
     * @return int Delivery attempt
     */
    public function getDeliveryAttempt(): int;
}
