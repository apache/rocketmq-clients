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
 * GeneralMessageImpl implements GeneralMessage interface
 * 
 * References Java GeneralMessageImpl (6.2KB)
 * 
 * Wraps both Message (for sending) and MessageView (for receiving)
 * Used by message interceptors
 */
class GeneralMessageImpl implements GeneralMessage
{
    /**
     * @var MessageId|null Message ID
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
     * @var string|null Message tag
     */
    private $tag;
    
    /**
     * @var string[] Message keys
     */
    private $keys = [];
    
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
     * @var array User properties
     */
    private $properties = [];
    
    /**
     * @var string|null Born host
     */
    private $bornHost;
    
    /**
     * @var int|null Born timestamp
     */
    private $bornTimestamp;
    
    /**
     * @var int Delivery attempt
     */
    private $deliveryAttempt = 0;
    
    /**
     * @var int|null Decode timestamp
     */
    private $decodeTimestamp;
    
    /**
     * @var int|null Transport delivery timestamp
     */
    private $transportDeliveryTimestamp;
    
    /**
     * Private constructor
     */
    private function __construct(?MessageViewImpl $messageView = null)
    {
        if ($messageView !== null) {
            $this->messageId = $messageView->getMessageId();
            $this->topic = $messageView->getTopic();
            $this->body = $messageView->getBody();
            $this->tag = $messageView->getTag();
            $this->keys = $messageView->getKeys();
            $this->messageGroup = $messageView->getMessageGroup();
            $this->liteTopic = $messageView->getLiteTopic();
            $this->deliveryTimestamp = $messageView->getDeliveryTimestamp();
            $this->priority = $messageView->getPriority();
            $this->properties = $messageView->getProperties();
            $this->bornHost = $messageView->getBornHost();
            $this->bornTimestamp = $messageView->getBornTimestamp();
            $this->deliveryAttempt = $messageView->getDeliveryAttempt();
            $this->decodeTimestamp = $messageView->getDecodeTimestamp();
            $this->transportDeliveryTimestamp = $messageView->getTransportDeliveryTimestamp();
        }
    }
    
    /**
     * Constructor from Message (for sending)
     * 
     * @param Message $message Message to send
     */
    public static function fromMessage(Message $message): GeneralMessageImpl
    {
        $instance = new self();
        $instance->topic = $message->getTopic();
        $instance->body = $message->getBody();
        $instance->tag = $message->getTag();
        $instance->keys = $message->getKeys();
        $instance->messageGroup = $message->getMessageGroup();
        $instance->liteTopic = $message->getLiteTopic();
        $instance->deliveryTimestamp = $message->getDeliveryTimestamp();
        $instance->priority = $message->getPriority();
        $instance->properties = $message->getProperties();
        $instance->bornTimestamp = (int)(microtime(true) * 1000);
        
        return $instance;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getMessageId(): ?MessageId
    {
        return $this->messageId;
    }
    
    /**
     * Set message ID
     * 
     * @internal Used internally by factory methods and producer.
     * @param MessageId $messageId Message ID
     * @return void
     */
    public function setMessageId(MessageId $messageId): void
    {
        $this->messageId = $messageId;
    }
    
    /**
     * Create from MessageView (for received messages)
     * 
     * Matches Java GeneralMessageImpl(MessageView) constructor.
     * 
     * @param MessageViewImpl $messageView Message view
     * @return GeneralMessageImpl
     */
    public static function fromMessageView(MessageViewImpl $messageView): GeneralMessageImpl
    {
        return new self($messageView);
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
     * {@inheritdoc}
     */
    public function getBornHost(): ?string
    {
        return $this->bornHost;
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
        return $this->deliveryAttempt;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getDecodeTimestamp(): ?int
    {
        return $this->decodeTimestamp;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getTransportDeliveryTimestamp(): ?int
    {
        return $this->transportDeliveryTimestamp;
    }
}
