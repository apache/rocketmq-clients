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

namespace Apache\Rocketmq;

use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\Settings as V2Settings;
use Apache\Rocketmq\V2\Publishing;
use Apache\Rocketmq\V2\Resource;
use Google\Protobuf\Duration;

/**
 * Publishing settings for Producer
 * 
 * Reference: Java PublishingSettings
 */
class PublishingSettings extends ClientSettings {
    /**
     * @var array Topics to publish
     */
    private $topics = [];
    
    /**
     * @var int Max message body size in bytes (default: 4MB)
     */
    private $maxBodySize = 4 * 1024 * 1024;
    
    /**
     * @var bool Whether to validate message type
     */
    private $validateMessageType = true;
    
    /**
     * Constructor
     * 
     * @param string $namespace Namespace
     * @param string $clientId Client ID
     * @param string $endpoints Endpoints
     * @param array $topics Topics to publish
     * @param int $maxAttempts Max retry attempts
     * @param int $requestTimeoutMs Request timeout in milliseconds
     */
    public function __construct(
        string $namespace,
        string $clientId,
        string $endpoints,
        array $topics = [],
        int $maxAttempts = 3,
        int $requestTimeoutMs = 3000
    ) {
        parent::__construct(
            $namespace,
            $clientId,
            ClientType::PRODUCER,
            $endpoints,
            $maxAttempts,
            $requestTimeoutMs
        );
        
        $this->topics = $topics;
    }
    
    /**
     * {@inheritdoc}
     */
    public function toProtobuf(): V2Settings {
        $settings = new V2Settings();
        $settings->setAccessPoint($this->endpoints);
        
        // Set publishing configuration
        $publishing = new Publishing();
        
        // Add topics
        foreach ($this->topics as $topic) {
            $resource = new Resource();
            $resource->setName($topic);
            $resource->setResourceNamespace($this->namespace);
            $publishing->getTopics()[] = $resource;
        }
        
        $settings->setPublishing($publishing);
        
        return $settings;
    }
    
    /**
     * Get topics
     * 
     * @return array Topics
     */
    public function getTopics(): array {
        return $this->topics;
    }
    
    /**
     * Add a topic
     * 
     * @param string $topic Topic name
     * @return PublishingSettings This instance
     */
    public function addTopic(string $topic): PublishingSettings {
        if (!in_array($topic, $this->topics)) {
            $this->topics[] = $topic;
        }
        return $this;
    }
    
    /**
     * Get max body size
     * 
     * @return int Max body size in bytes
     */
    public function getMaxBodySize(): int {
        return $this->maxBodySize;
    }
    
    /**
     * Set max body size
     * 
     * @param int $maxBodySize Max body size in bytes
     * @return PublishingSettings This instance
     */
    public function setMaxBodySize(int $maxBodySize): PublishingSettings {
        $this->maxBodySize = $maxBodySize;
        return $this;
    }
    
    /**
     * Check if message type validation is enabled
     * 
     * @return bool True if enabled
     */
    public function isValidateMessageType(): bool {
        return $this->validateMessageType;
    }
    
    /**
     * Enable or disable message type validation
     * 
     * @param bool $validate Whether to validate
     * @return PublishingSettings This instance
     */
    public function setValidateMessageType(bool $validate): PublishingSettings {
        $this->validateMessageType = $validate;
        return $this;
    }
}
