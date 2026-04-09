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

namespace Apache\Rocketmq\Builder;

use Apache\Rocketmq\Exception\MessageException;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\MessageType;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;

/**
 * Builder for creating Message instances
 */
class MessageBuilder {
    /**
     * @var string|null
     */
    private $topic;
    
    /**
     * @var string|null
     */
    private $body;
    
    /**
     * @var string|null
     */
    private $tag;
    
    /**
     * @var string|null
     */
    private $keys;
    
    /**
     * @var string|null
     */
    private $messageGroup;
    
    /**
     * @var int|null
     */
    private $deliveryTimestamp;
    
    /**
     * @var string|null
     */
    private $liteTopic;
    
    /**
     * @var int|null
     */
    private $priority;
    
    /**
     * @var array
     */
    private $keys = [];
    
    /**
     * @var array
     */
    private $properties = [];
    
    /**
     * Set topic
     *
     * @param string $topic
     * @return MessageBuilder
     */
    public function setTopic(string $topic) {
        $this->topic = $topic;
        return $this;
    }
    
    /**
     * Set message body
     *
     * @param string $body
     * @return MessageBuilder
     */
    public function setBody(string $body) {
        $this->body = $body;
        return $this;
    }
    
    /**
     * Set message tag
     *
     * @param string $tag
     * @return MessageBuilder
     */
    public function setTag(string $tag) {
        $this->tag = $tag;
        return $this;
    }
    
    /**
     * Set message keys
     *
     * @param string|array $keys
     * @return MessageBuilder
     */
    public function setKeys($keys) {
        if (is_string($keys)) {
            $this->keys = [$keys];
        } elseif (is_array($keys)) {
            $this->keys = $keys;
        }
        return $this;
    }
    
    /**
     * Add a single key to message
     *
     * @param string $key
     * @return MessageBuilder
     */
    public function addKey(string $key) {
        $this->keys[] = $key;
        return $this;
    }
    
    /**
     * Set message group (for FIFO messages)
     *
     * @param string $messageGroup
     * @return MessageBuilder
     */
    public function setMessageGroup(string $messageGroup) {
        $this->messageGroup = $messageGroup;
        return $this;
    }
    
    /**
     * Set delivery timestamp (for scheduled messages)
     *
     * @param int $deliveryTimestamp
     * @return MessageBuilder
     */
    public function setDeliveryTimestamp(int $deliveryTimestamp) {
        $this->deliveryTimestamp = $deliveryTimestamp;
        return $this;
    }
    
    /**
     * Set lite topic (for lite topic messages)
     *
     * @param string $liteTopic
     * @return MessageBuilder
     */
    public function setLiteTopic(string $liteTopic) {
        $this->liteTopic = $liteTopic;
        return $this;
    }
    
    /**
     * Set message priority (for priority messages)
     *
     * @param int $priority
     * @return MessageBuilder
     */
    public function setPriority(int $priority) {
        $this->priority = $priority;
        return $this;
    }
    
    /**
     * Add message property
     *
     * @param string $key
     * @param string $value
     * @return MessageBuilder
     */
    public function addProperty(string $key, string $value) {
        $this->properties[$key] = $value;
        return $this;
    }
    
    /**
     * Set message properties
     *
     * @param array $properties
     * @return MessageBuilder
     */
    public function setProperties(array $properties) {
        $this->properties = $properties;
        return $this;
    }
    
    /**
     * Build the message
     *
     * @return Message
     * @throws MessageException
     */
    public function build() {
        if (empty($this->topic)) {
            throw new MessageException("Topic must be set");
        }
        
        if (empty($this->body)) {
            throw new MessageException("Message body must be set");
        }
        
        $message = new Message();
        
        // Set topic
        $topicResource = new Resource();
        $topicResource->setName($this->topic);
        $message->setTopic($topicResource);
        
        // Set message body
        $message->setBody($this->body);
        
        // Set system properties
        $systemProperties = new SystemProperties();
        
        if ($this->tag !== null) {
            $systemProperties->setTag($this->tag);
        }
        
        if (!empty($this->keys)) {
            $systemProperties->setKeys($this->keys);
        }
        
        if ($this->messageGroup !== null) {
            $systemProperties->setMessageGroup($this->messageGroup);
            $systemProperties->setMessageType(MessageType::FIFO);
        }
        
        if ($this->deliveryTimestamp !== null) {
            // Set scheduled delivery timestamp
            $timestamp = new \Google\Protobuf\Timestamp();
            $timestamp->setSeconds(intval($this->deliveryTimestamp / 1000));
            $timestamp->setNanos(($this->deliveryTimestamp % 1000) * 1000000);
            $systemProperties->setDeliveryTimestamp($timestamp);
        }
        
        if ($this->liteTopic !== null) {
            $systemProperties->setLiteTopic($this->liteTopic);
            $systemProperties->setMessageType(MessageType::LITE);
        }
        
        if ($this->priority !== null) {
            $systemProperties->setPriority($this->priority);
            $systemProperties->setMessageType(MessageType::PRIORITY);
        }
        
        $message->setSystemProperties($systemProperties);
        
        // Set custom properties
        if (!empty($this->properties)) {
            $message->setProperties($this->properties);
        }
        
        return $message;
    }
}
