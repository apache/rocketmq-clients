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
use Apache\Rocketmq\Message\MessageAdapter;
use Apache\Rocketmq\Message\MessageBuilder as MessageBuilderInterface;

/**
 * Builder for creating Message instances
 * 
 * Implements MessageBuilderInterface from Apache\Rocketmq\Message namespace
 */
class MessageBuilder implements MessageBuilderInterface {
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
     * @param string ...$keys Variable number of key strings
     * @return MessageBuilder
     */
    public function setKeys(string ...$keys) {
        $this->keys = $keys;
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
     * MessageGroup cannot be used with:
     * - deliveryTimestamp (delayed messages)
     * - priority (priority messages)
     * - liteTopic (lite messages)
     *
     * Reference: Java MessageBuilderImpl.setMessageGroup() lines 120-130
     *
     * @param string $messageGroup
     * @return MessageBuilder
     * @throws MessageException If validation fails
     */
    public function setMessageGroup(string $messageGroup) {
        // Check mutual exclusivity with deliveryTimestamp
        if ($this->deliveryTimestamp !== null) {
            throw new MessageException(
                "MessageGroup and deliveryTimestamp should not be set at the same time. " .
                "A message cannot be both FIFO and delayed."
            );
        }
        
        // Check mutual exclusivity with priority
        if ($this->priority !== null) {
            throw new MessageException(
                "MessageGroup and priority should not be set at the same time. " .
                "A message cannot be both FIFO and priority."
            );
        }
        
        // Check mutual exclusivity with liteTopic
        if ($this->liteTopic !== null) {
            throw new MessageException(
                "MessageGroup and liteTopic should not be set at the same time. " .
                "A message cannot be both FIFO and lite."
            );
        }
        
        $this->messageGroup = $messageGroup;
        return $this;
    }
    
    /**
     * Set delivery timestamp (for scheduled messages)
     *
     * DeliveryTimestamp cannot be used with:
     * - messageGroup (FIFO messages)
     * - priority (priority messages)
     * - liteTopic (lite messages)
     *
     * Reference: Java MessageBuilderImpl.setDeliveryTimestamp() lines 102-117
     *
     * @param int $deliveryTimestamp Unix timestamp in milliseconds
     * @return MessageBuilder
     * @throws MessageException If validation fails
     */
    public function setDeliveryTimestamp(int $deliveryTimestamp) {
        // Validate timestamp is in the future
        if ($deliveryTimestamp <= time() * 1000) {
            throw new MessageException(
                "Delivery timestamp must be in the future. " .
                "Current time: " . (time() * 1000) . ", provided: {$deliveryTimestamp}"
            );
        }
        
        // Check mutual exclusivity with messageGroup
        if ($this->messageGroup !== null) {
            throw new MessageException(
                "DeliveryTimestamp and messageGroup should not be set at the same time. " .
                "A message cannot be both delayed and FIFO."
            );
        }
        
        // Check mutual exclusivity with priority
        if ($this->priority !== null) {
            throw new MessageException(
                "DeliveryTimestamp and priority should not be set at the same time. " .
                "A message cannot be both delayed and priority."
            );
        }
        
        // Check mutual exclusivity with liteTopic
        if ($this->liteTopic !== null) {
            throw new MessageException(
                "DeliveryTimestamp and liteTopic should not be set at the same time. " .
                "A message cannot be both delayed and lite."
            );
        }
        
        $this->deliveryTimestamp = $deliveryTimestamp;
        return $this;
    }
    
    /**
     * Set lite topic (for lite topic messages)
     *
     * LiteTopic cannot be used with:
     * - deliveryTimestamp (delayed messages)
     * - messageGroup (FIFO messages)
     * - priority (priority messages)
     *
     * Reference: Java MessageBuilderImpl.setLiteTopic() lines 145-153
     *
     * @param string $liteTopic
     * @return MessageBuilder
     * @throws MessageException If validation fails
     */
    public function setLiteTopic(string $liteTopic) {
        // Check mutual exclusivity with deliveryTimestamp
        if ($this->deliveryTimestamp !== null) {
            throw new MessageException(
                "LiteTopic and deliveryTimestamp should not be set at the same time. " .
                "A message cannot be both lite and delayed."
            );
        }
        
        // Check mutual exclusivity with messageGroup
        if ($this->messageGroup !== null) {
            throw new MessageException(
                "LiteTopic and messageGroup should not be set at the same time. " .
                "A message cannot be both lite and FIFO."
            );
        }
        
        // Check mutual exclusivity with priority
        if ($this->priority !== null) {
            throw new MessageException(
                "LiteTopic and priority should not be set at the same time. " .
                "A message cannot be both lite and priority."
            );
        }
        
        $this->liteTopic = $liteTopic;
        return $this;
    }
    
    /**
     * Set message priority (for priority messages)
     *
     * Priority messages cannot be used with:
     * - deliveryTimestamp (delayed messages)
     * - messageGroup (FIFO messages)
     * - liteTopic (lite messages)
     *
     * Reference: Java MessageBuilderImpl.setPriority() lines 136-143
     *
     * @param int $priority Non-negative integer, higher value means higher priority
     * @return MessageBuilder
     * @throws MessageException If validation fails
     */
    public function setPriority(int $priority) {
        // Validate priority is non-negative (Java line 140)
        if ($priority < 0) {
            throw new MessageException("Priority must be greater than or equal to 0, got: {$priority}");
        }
        
        // Check mutual exclusivity with deliveryTimestamp (Java line 137)
        if ($this->deliveryTimestamp !== null) {
            throw new MessageException(
                "Priority and deliveryTimestamp should not be set at the same time. " .
                "A message cannot be both priority and delayed."
            );
        }
        
        // Check mutual exclusivity with messageGroup (Java line 138)
        if ($this->messageGroup !== null) {
            throw new MessageException(
                "Priority and messageGroup should not be set at the same time. " .
                "A message cannot be both priority and FIFO."
            );
        }
        
        // Check mutual exclusivity with liteTopic (Java line 139)
        if ($this->liteTopic !== null) {
            throw new MessageException(
                "Priority and liteTopic should not be set at the same time. " .
                "A message cannot be both priority and lite."
            );
        }
        
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
     * @return \Apache\Rocketmq\Message\Message
     * @throws MessageException
     */
    public function build(): \Apache\Rocketmq\Message\Message {
        if (empty($this->topic)) {
            throw new MessageException("Topic must be set");
        }
        
        if (empty($this->body)) {
            throw new MessageException("Message body must be set");
        }
        
        $v2Message = new Message();
        
        // Set topic
        $topicResource = new Resource();
        $topicResource->setName($this->topic);
        $v2Message->setTopic($topicResource);
        
        // Set message body
        $v2Message->setBody($this->body);
        
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
        
        $v2Message->setSystemProperties($systemProperties);
        
        // Set custom properties
        if (!empty($this->properties)) {
            $v2Message->setProperties($this->properties);
        }
        
        // Wrap V2\Message in adapter to implement Message interface
        return new MessageAdapter($v2Message);
    }
}
