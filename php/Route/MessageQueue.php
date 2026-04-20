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

namespace Apache\Rocketmq\Route;

/**
 * MessageQueue represents a message queue with routing information
 * 
 * References Java MessageQueueImpl implementation (107 lines)
 * 
 * Key features:
 * - Wraps topic, broker, and queue ID
 * - Manages queue permissions
 * - Tracks accepted message types
 */
class MessageQueue
{
    /**
     * @var string Topic name
     */
    private $topic;
    
    /**
     * @var Broker Broker information
     */
    private $broker;
    
    /**
     * @var int Queue ID
     */
    private $queueId;
    
    /**
     * @var int Permission (NONE, READ, WRITE, READ_WRITE)
     */
    private $permission;
    
    /**
     * @var int[] Accepted message types
     */
    private $acceptMessageTypes = [];
    
    /**
     * Constructor from Protobuf
     * 
     * @param \Apache\Rocketmq\V2\MessageQueue $messageQueue Protobuf message queue
     */
    public function __construct(\Apache\Rocketmq\V2\MessageQueue $messageQueue)
    {
        $this->topic = $messageQueue->getTopic()->getName();
        $this->queueId = $messageQueue->getId();
        
        $perm = $messageQueue->getPermission();
        $this->permission = Permission::fromProtobuf($perm);
        
        $this->acceptMessageTypes = [];
        foreach ($messageQueue->getAcceptMessageTypes() as $type) {
            $this->acceptMessageTypes[] = $type;
        }
        
        $this->broker = new Broker($messageQueue->getBroker());
    }
    
    /**
     * Get topic name
     * 
     * @return string Topic name
     */
    public function getTopic(): string
    {
        return $this->topic;
    }
    
    /**
     * Get broker information
     * 
     * @return Broker Broker
     */
    public function getBroker(): Broker
    {
        return $this->broker;
    }
    
    /**
     * Get queue ID
     * 
     * @return int Queue ID
     */
    public function getQueueId(): int
    {
        return $this->queueId;
    }
    
    /**
     * Get permission
     * 
     * @return int Permission
     */
    public function getPermission(): int
    {
        return $this->permission;
    }
    
    /**
     * Check if queue is readable
     * 
     * @return bool Whether readable
     */
    public function isReadable(): bool
    {
        return Permission::isReadable($this->permission);
    }
    
    /**
     * Check if queue is writable
     * 
     * @return bool Whether writable
     */
    public function isWritable(): bool
    {
        return Permission::isWritable($this->permission);
    }
    
    /**
     * Get accepted message types
     * 
     * @return int[] Message types
     */
    public function getAcceptMessageTypes(): array
    {
        return $this->acceptMessageTypes;
    }
    
    /**
     * Check if accepts message type
     * 
     * @param int $messageType Message type
     * @return bool Whether accepts
     */
    public function acceptsMessageType(int $messageType): bool
    {
        return in_array($messageType, $this->acceptMessageTypes);
    }
    
    /**
     * Convert to Protobuf
     * 
     * @return \Apache\Rocketmq\V2\MessageQueue Protobuf message queue
     */
    public function toProtobuf(): \Apache\Rocketmq\V2\MessageQueue
    {
        $topicResource = new \Apache\Rocketmq\V2\Resource();
        $topicResource->setName($this->topic);
        
        $messageQueue = new \Apache\Rocketmq\V2\MessageQueue();
        $messageQueue->setTopic($topicResource);
        $messageQueue->setId($this->queueId);
        $messageQueue->setPermission(Permission::toProtobuf($this->permission));
        $messageQueue->setBroker($this->broker->toProtobuf());
        
        foreach ($this->acceptMessageTypes as $type) {
            $messageQueue->addAcceptMessageTypes($type);
        }
        
        return $messageQueue;
    }
    
    /**
     * Generate unique key for this message queue
     * 
     * @return string Unique key
     */
    public function getKey(): string
    {
        return "{$this->topic}@{$this->broker->getName()}:{$this->queueId}";
    }
    
    /**
     * {@inheritdoc}
     */
    public function __toString(): string
    {
        return "{$this->broker->getName()}.{$this->topic}.{$this->queueId}";
    }
    
    /**
     * Check equality
     * 
     * @param MessageQueue $other Other message queue
     * @return bool Whether equal
     */
    public function equals(MessageQueue $other): bool
    {
        return $this->topic === $other->topic
            && $this->queueId === $other->queueId
            && $this->permission === $other->permission
            && $this->broker->getName() === $other->broker->getName();
    }
}
