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
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\FilterExpression;
use Google\Protobuf\Duration;

/**
 * Push subscription settings for PushConsumer
 * 
 * Reference: Java PushSubscriptionSettings
 */
class PushSubscriptionSettings extends ClientSettings {
    /**
     * @var string Consumer group
     */
    private $consumerGroup;
    
    /**
     * @var array Topic => FilterExpression map
     */
    private $subscriptionExpressions = [];
    
    /**
     * @var int Max cache message count
     */
    private $maxCacheMessageCount = 1000;
    
    /**
     * @var int Max cache message size in bytes
     */
    private $maxCacheMessageSize = 64 * 1024 * 1024; // 64MB
    
    /**
     * @var int Consumption thread count
     */
    private $consumptionThreadCount = 20;
    
    /**
     * Constructor
     * 
     * @param string $namespace Namespace
     * @param string $clientId Client ID
     * @param string $endpoints Endpoints
     * @param string $consumerGroup Consumer group
     * @param array $subscriptionExpressions Topic => FilterExpression map
     * @param int $maxAttempts Max retry attempts
     * @param int $requestTimeoutMs Request timeout in milliseconds
     */
    public function __construct(
        string $namespace,
        string $clientId,
        string $endpoints,
        string $consumerGroup,
        array $subscriptionExpressions = [],
        int $maxAttempts = 3,
        int $requestTimeoutMs = 3000
    ) {
        parent::__construct(
            $namespace,
            $clientId,
            ClientType::PUSH_CONSUMER,
            $endpoints,
            $maxAttempts,
            $requestTimeoutMs
        );
        
        $this->consumerGroup = $consumerGroup;
        $this->subscriptionExpressions = $subscriptionExpressions;
    }
    
    /**
     * {@inheritdoc}
     */
    public function toProtobuf(): V2Settings {
        $settings = new V2Settings();
        $settings->setAccessPoint($this->endpoints);
        
        // Set subscription configuration
        $subscription = new Subscription();
        
        // Set consumer group
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $groupResource->setResourceNamespace($this->namespace);
        $subscription->setGroup($groupResource);
        
        // Add subscription expressions
        foreach ($this->subscriptionExpressions as $topic => $filterExpression) {
            $resource = new Resource();
            $resource->setName($topic);
            $resource->setResourceNamespace($this->namespace);
            
            $filterExpr = new FilterExpression();
            $filterExpr->setTopic($resource);
            // Note: FilterExpression type and expression would need to be set based on your implementation
            
            $subscription->getSubscriptions()[] = $filterExpr;
        }
        
        $settings->setSubscription($subscription);
        
        return $settings;
    }
    
    /**
     * Get consumer group
     * 
     * @return string Consumer group
     */
    public function getConsumerGroup(): string {
        return $this->consumerGroup;
    }
    
    /**
     * Get subscription expressions
     * 
     * @return array Topic => FilterExpression map
     */
    public function getSubscriptionExpressions(): array {
        return $this->subscriptionExpressions;
    }
    
    /**
     * Add subscription expression
     * 
     * @param string $topic Topic name
     * @param mixed $filterExpression Filter expression
     * @return PushSubscriptionSettings This instance
     */
    public function addSubscription(string $topic, $filterExpression): PushSubscriptionSettings {
        $this->subscriptionExpressions[$topic] = $filterExpression;
        return $this;
    }
    
    /**
     * Get max cache message count
     * 
     * @return int Max cache message count
     */
    public function getMaxCacheMessageCount(): int {
        return $this->maxCacheMessageCount;
    }
    
    /**
     * Set max cache message count
     * 
     * @param int $count Max cache message count
     * @return PushSubscriptionSettings This instance
     */
    public function setMaxCacheMessageCount(int $count): PushSubscriptionSettings {
        $this->maxCacheMessageCount = $count;
        return $this;
    }
    
    /**
     * Get max cache message size
     * 
     * @return int Max cache message size in bytes
     */
    public function getMaxCacheMessageSize(): int {
        return $this->maxCacheMessageSize;
    }
    
    /**
     * Set max cache message size
     * 
     * @param int $size Max cache message size in bytes
     * @return PushSubscriptionSettings This instance
     */
    public function setMaxCacheMessageSize(int $size): PushSubscriptionSettings {
        $this->maxCacheMessageSize = $size;
        return $this;
    }
    
    /**
     * Get consumption thread count
     * 
     * @return int Consumption thread count
     */
    public function getConsumptionThreadCount(): int {
        return $this->consumptionThreadCount;
    }
    
    /**
     * Set consumption thread count
     * 
     * @param int $count Consumption thread count
     * @return PushSubscriptionSettings This instance
     */
    public function setConsumptionThreadCount(int $count): PushSubscriptionSettings {
        $this->consumptionThreadCount = $count;
        return $this;
    }
}
