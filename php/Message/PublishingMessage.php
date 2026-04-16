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

use Apache\Rocketmq\Exception\MessageException;

/**
 * PublishingMessage represents a message to be published
 * 
 * References Java PublishingMessageImpl (6.3KB)
 * 
 * Key features:
 * - Validates message before sending
 * - Determines message type automatically
 * - Calculates body size
 */
class PublishingMessage implements GeneralMessage
{
    /**
     * @var MessageId|null Message ID (assigned after send)
     */
    private $messageId;
    
    /**
     * @var string Topic name
     */
    private $topic;
    
    /**
     * @var string Message body
     */
    private $body;
    
    /**
     * @var string|null Tag
     */
    private $tag;
    
    /**
     * @var string[] Keys
     */
    private $keys;
    
    /**
     * @var string|null Message group
     */
    private $messageGroup;
    
    /**
     * @var string|null Lite topic
     */
    private $liteTopic;
    
    /**
     * @var int|null Delivery timestamp
     */
    private $deliveryTimestamp;
    
    /**
     * @var int|null Priority
     */
    private $priority;
    
    /**
     * @var array Properties
     */
    private $properties;
    
    /**
     * @var int Message type
     */
    private $messageType;
    
    /**
     * @var int Body size
     */
    private $bodySize;
    
    /**
     * @var int|null Born timestamp
     */
    private $bornTimestamp;
    
    /**
     * @var bool Whether transaction is enabled
     */
    private $txEnabled;
    
    /**
     * Constructor
     * 
     * @param Message $message Message to publish
     * @param bool $txEnabled Whether this is a transactional message
     * @throws MessageException If validation fails
     */
    public function __construct(Message $message, bool $txEnabled = false)
    {
        $this->txEnabled = $txEnabled;
        $this->validate($message);
        
        $this->topic = $message->getTopic();
        $this->body = $message->getBody();
        $this->tag = $message->getTag();
        $this->keys = $message->getKeys();
        $this->messageGroup = $message->getMessageGroup();
        $this->liteTopic = $message->getLiteTopic();
        $this->deliveryTimestamp = $message->getDeliveryTimestamp();
        $this->priority = $message->getPriority();
        $this->properties = $message->getProperties();
        $this->bodySize = strlen($this->body);
        $this->bornTimestamp = (int)(microtime(true) * 1000);
        
        // Determine message type
        $this->messageType = $this->determineMessageType();
    }
    
    /**
     * Validate message before sending
     * 
     * @param Message $message Message to validate
     * @throws MessageException If validation fails
     */
    private function validate(Message $message): void
    {
        if (empty($message->getTopic())) {
            throw new MessageException("Topic must not be empty");
        }
        
        $body = $message->getBody();
        if (empty($body)) {
            throw new MessageException("Message body must not be empty");
        }
        
        if (strlen($body) > 4194304) {
            throw new MessageException("Message body size exceeds 4MB limit");
        }
        
        if ($message->getMessageGroup() !== null) {
            if (empty($message->getMessageGroup())) {
                throw new MessageException("Message group must not be empty for FIFO messages");
            }
        }
        
        if ($message->getDeliveryTimestamp() !== null) {
            if ($message->getDeliveryTimestamp() <= (int)(microtime(true) * 1000)) {
                throw new MessageException("Delivery timestamp must be in the future");
            }
        }
        
        // Transaction message conflict validation (matches Java PublishingMessageImpl)
        if ($this->txEnabled) {
            if ($message->getMessageGroup() !== null) {
                throw new MessageException("Transactional message should not set message group");
            }
            if ($message->getDeliveryTimestamp() !== null) {
                throw new MessageException("Transactional message should not set delivery timestamp");
            }
            if ($message->getLiteTopic() !== null) {
                throw new MessageException("Transactional message should not set lite topic");
            }
            if ($message->getPriority() !== null) {
                throw new MessageException("Transactional message should not set priority");
            }
        }
    }
    
    /**
     * Determine message type based on properties
     * 
     * Follows Java PublishingMessageImpl type resolution cascade.
     * 
     * @return int Message type
     */
    private function determineMessageType(): int
    {
        if ($this->txEnabled) {
            return MessageType::TRANSACTION;
        }
        
        if ($this->messageGroup !== null) {
            return MessageType::FIFO;
        }
        
        if ($this->liteTopic !== null) {
            return MessageType::LITE;
        }
        
        if ($this->deliveryTimestamp !== null) {
            return MessageType::DELAY;
        }
        
        if ($this->priority !== null) {
            return MessageType::PRIORITY;
        }
        
        return MessageType::NORMAL;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getMessageId(): ?MessageId
    {
        return $this->messageId;
    }
    
    /**
     * Set message ID (called after successful send)
     * 
     * @param MessageId $messageId Message ID
     * @return void
     */
    public function setMessageId(MessageId $messageId): void
    {
        $this->messageId = $messageId;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getTopic(): string
    {
        return $this->topic;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getBody(): string
    {
        return $this->body;
    }
    
    /**
     * Get body size
     * 
     * @return int Body size in bytes
     */
    public function getBodySize(): int
    {
        return $this->bodySize;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getProperties(): array
    {
        return $this->properties;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getTag(): ?string
    {
        return $this->tag;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getKeys(): array
    {
        return $this->keys;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getMessageGroup(): ?string
    {
        return $this->messageGroup;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getLiteTopic(): ?string
    {
        return $this->liteTopic;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getDeliveryTimestamp(): ?int
    {
        return $this->deliveryTimestamp;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getPriority(): ?int
    {
        return $this->priority;
    }
    
    /**
     * Get message type
     * 
     * @return int Message type
     */
    public function getMessageType(): int
    {
        return $this->messageType;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getBornHost(): ?string
    {
        return null; // Will be set by broker
    }
    
    /**
     * {@inheritdoc}
     */
    public function getBornTimestamp(): ?int
    {
        return $this->bornTimestamp;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getDeliveryAttempt(): int
    {
        return 0; // New message, not delivered yet
    }
    
    /**
     * {@inheritdoc}
     */
    public function getDecodeTimestamp(): ?int
    {
        return null;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getTransportDeliveryTimestamp(): ?int
    {
        return null;
    }
}
