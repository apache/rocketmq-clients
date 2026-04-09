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
 * Abstract message only used for Producer
 */
interface Message {
    /**
     * Get the topic of message, which is the first classifier for message
     * 
     * @return string Topic of message
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
     * Get the tag of message, which is the second classifier besides topic
     * 
     * @return string|null The tag of message, null means tag does not exist
     */
    public function getTag(): ?string;
    
    /**
     * Get the key collection of message
     * 
     * @return array Key collection of message, empty array means message key is not specified
     */
    public function getKeys(): array;
    
    /**
     * Get the message group, which make sense only when topic type is fifo
     * 
     * @return string|null Message group, null means message group is not specified
     */
    public function getMessageGroup(): ?string;
    
    /**
     * Get the lite topic, which is used for lite topic message type
     * 
     * @return string|null Lite topic, null means lite topic is not specified
     */
    public function getLiteTopic(): ?string;
    
    /**
     * Get the expected delivery timestamp, which make sense only when topic type is delay
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
}
