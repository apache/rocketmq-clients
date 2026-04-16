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
 * Builder to config Message
 */
interface MessageBuilder {
    /**
     * Set the topic for the message, which is essential for each message
     * 
     * @param string $topic The topic for the message
     * @return MessageBuilder The message builder instance
     */
    public function setTopic(string $topic);
    
    /**
     * Set the body for the message, which is essential for each message
     * 
     * Message will deep-copy the body as its payload, thus any modification to the original body would
     * not affect the message itself.
     * 
     * @param string $body The body for the message
     * @return MessageBuilder The message builder instance
     */
    public function setBody(string $body);
    
    /**
     * Set the tag for the message, which is optional
     * 
     * Tag is a secondary classifier for each message besides the topic
     * 
     * @param string $tag The tag for the message
     * @return MessageBuilder The message builder instance
     */
    public function setTag(string $tag);
    
    /**
     * Set the key collection for the message, which is optional
     * 
     * Message key is another way to locate a message besides the MessageId, so it should be unique for
     * each message usually.
     * 
     * @param string ...$keys Key(s) for the message
     * @return MessageBuilder The message builder instance
     */
    public function setKeys(string ...$keys);
    
    /**
     * Set the group for the message, which is optional
     * 
     * Message group and the delivery timestamp should not be set in the same message
     * 
     * @param string $messageGroup Group for the message
     * @return MessageBuilder The message builder instance
     */
    public function setMessageGroup(string $messageGroup);
    
    /**
     * Set the lite topic for the message, which is optional
     * 
     * @param string $liteTopic Lite topic for the message
     * @return MessageBuilder The message builder instance
     */
    public function setLiteTopic(string $liteTopic);
    
    /**
     * Set the delivery timestamp for the message, which is optional
     * 
     * Delivery timestamp and message group should not be set in the same message
     * 
     * @param int $deliveryTimestamp Delivery timestamp for the message
     * @return MessageBuilder The message builder instance
     */
    public function setDeliveryTimestamp(int $deliveryTimestamp);
    
    /**
     * Set the priority for the message, which is optional
     * 
     * @param int $priority Non-negative number in the range [0, N], regarded as highest priority if exceeds N
     * @return MessageBuilder The message builder instance
     */
    public function setPriority(int $priority);
    
    /**
     * Add user property for the message
     * 
     * @param string $key Single property key
     * @param string $value Single property value
     * @return MessageBuilder The message builder instance
     */
    public function addProperty(string $key, string $value);
    
    /**
     * Finalize the build of the Message instance
     * 
     * Unique MessageId is generated after message building
     * 
     * @return Message The message instance
     */
    public function build(): \Apache\Rocketmq\Message\Message;
}
