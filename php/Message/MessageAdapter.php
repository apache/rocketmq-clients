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

use Apache\Rocketmq\V2\Message as V2Message;

/**
 * Adapter that wraps V2\Message to implement Message interface
 */
class MessageAdapter implements Message {
    /**
     * @var V2Message The underlying protobuf message
     */
    private $v2Message;
    
    /**
     * @var string Topic name
     */
    private $topic;
    
    /**
     * @var string Message body
     */
    private $body;
    
    /**
     * @var array Message properties
     */
    private $properties = [];
    
    /**
     * @var string|null Message tag
     */
    private $tag = null;
    
    /**
     * @var array Message keys
     */
    private $keys = [];
    
    /**
     * @var string|null Message group
     */
    private $messageGroup = null;
    
    /**
     * @var string|null Lite topic
     */
    private $liteTopic = null;
    
    /**
     * @var int|null Delivery timestamp
     */
    private $deliveryTimestamp = null;
    
    /**
     * @var int|null Priority
     */
    private $priority = null;
    
    /**
     * Constructor
     * 
     * @param V2Message $v2Message Protobuf message
     */
    public function __construct(V2Message $v2Message) {
        $this->v2Message = $v2Message;
        
        // Extract topic
        if ($v2Message->hasTopic()) {
            $this->topic = $v2Message->getTopic()->getName();
        }
        
        // Extract body
        $this->body = $v2Message->getBody();
        
        // Extract system properties using has*() checks instead of try/catch
        if ($v2Message->hasSystemProperties()) {
            $sysProps = $v2Message->getSystemProperties();
            
            // Tag
            if ($sysProps->hasTag()) {
                $tag = $sysProps->getTag();
                if ($tag !== '') {
                    $this->tag = $tag;
                }
            }
            
            // Keys
            $keysList = $sysProps->getKeys();
            if ($keysList !== null) {
                $this->keys = ($keysList instanceof \Traversable) ? iterator_to_array($keysList) : (array)$keysList;
            }
            
            // Message group
            if ($sysProps->hasMessageGroup()) {
                $messageGroup = $sysProps->getMessageGroup();
                $this->messageGroup = ($messageGroup !== '') ? $messageGroup : null;
            }
            
            // Delivery timestamp (with millisecond precision)
            if ($sysProps->hasDeliveryTimestamp()) {
                $timestamp = $sysProps->getDeliveryTimestamp();
                $this->deliveryTimestamp = (int)($timestamp->getSeconds() * 1000 + intval($timestamp->getNanos() / 1000000));
            }
            
            // Priority
            if ($sysProps->hasPriority()) {
                $this->priority = $sysProps->getPriority();
            }
            
            // Lite topic
            if ($sysProps->hasLiteTopic()) {
                $liteTopic = $sysProps->getLiteTopic();
                $this->liteTopic = ($liteTopic !== '') ? $liteTopic : null;
            }
        }
        
        // Extract custom properties
        if (method_exists($v2Message, 'getUserProperties')) {
            $userProps = $v2Message->getUserProperties();
            if ($userProps !== null) {
                $this->properties = ($userProps instanceof \Traversable) ? iterator_to_array($userProps) : (array)$userProps;
            }
        } elseif (method_exists($v2Message, 'getProperties')) {
            $propertiesList = $v2Message->getProperties();
            if (!empty($propertiesList)) {
                $this->properties = (array)$propertiesList;
            }
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function getTopic(): string {
        return $this->topic ?? '';
    }
    
    /**
     * {@inheritdoc}
     */
    public function getBody(): string {
        return $this->body;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getProperties(): array {
        return $this->properties;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getTag(): ?string {
        return $this->tag;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getKeys(): array {
        return $this->keys;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getMessageGroup(): ?string {
        return $this->messageGroup;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getLiteTopic(): ?string {
        return $this->liteTopic;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getDeliveryTimestamp(): ?int {
        return $this->deliveryTimestamp;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getPriority(): ?int {
        return $this->priority;
    }
    
    /**
     * Get the underlying V2\Message
     * 
     * @return V2Message
     */
    public function getV2Message(): V2Message {
        return $this->v2Message;
    }
}
