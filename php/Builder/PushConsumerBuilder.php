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

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Exception\ClientConfigurationException;
use Apache\Rocketmq\Consumer\PushConsumer;
use Apache\Rocketmq\Consumer\PushConsumerImpl;
use Apache\Rocketmq\Consumer\FilterExpression;

/**
 * Builder for creating PushConsumer instances
 * 
 * References Java PushConsumerBuilder design:
 * - Support multiple topic subscriptions
 * - Configurable cache thresholds (count and size)
 * - Configurable consumption thread pool
 * - FIFO consume accelerator option
 */
class PushConsumerBuilder {
    /**
     * @var ClientConfiguration|null
     */
    private $clientConfiguration;
    
    /**
     * @var string|null
     */
    private $consumerGroup;
    
    /**
     * @var array<string, FilterExpression> Subscription expressions
     */
    private $subscriptionExpressions = [];
    
    /**
     * @var callable|null
     */
    private $messageListener;
    
    /**
     * @var int Maximum cache message count
     */
    private $maxCacheMessageCount = 4096;
    
    /**
     * @var int Maximum cache message size in bytes (64MB)
     */
    private $maxCacheMessageSizeInBytes = 67108864;
    
    /**
     * @var int Consumption thread count
     */
    private $consumptionThreadCount = 20;
    
    /**
     * @var bool Enable FIFO consume accelerator
     */
    private $enableFifoConsumeAccelerator = false;
    
    /**
     * Set client configuration
     *
     * @param ClientConfiguration $clientConfiguration
     * @return PushConsumerBuilder
     */
    public function setClientConfiguration(ClientConfiguration $clientConfiguration) {
        $this->clientConfiguration = $clientConfiguration;
        return $this;
    }
    
    /**
     * Set consumer group
     *
     * @param string $consumerGroup
     * @return PushConsumerBuilder
     */
    public function setConsumerGroup(string $consumerGroup) {
        $this->consumerGroup = $consumerGroup;
        return $this;
    }
    
    /**
     * Set subscription expressions
     *
     * @param array<string, FilterExpression> $subscriptionExpressions
     * @return PushConsumerBuilder
     */
    public function setSubscriptionExpressions(array $subscriptionExpressions) {
        if (empty($subscriptionExpressions)) {
            throw new ClientConfigurationException("subscriptionExpressions should not be empty");
        }
        $this->subscriptionExpressions = $subscriptionExpressions;
        return $this;
    }
    
    /**
     * Add subscription expression
     *
     * @param string $topic Topic name
     * @param FilterExpression $filterExpression Filter expression
     * @return PushConsumerBuilder
     */
    public function addSubscription(string $topic, FilterExpression $filterExpression) {
        $this->subscriptionExpressions[$topic] = $filterExpression;
        return $this;
    }
    
    /**
     * Set message listener
     *
     * @param callable $messageListener
     * @return PushConsumerBuilder
     */
    public function setMessageListener(callable $messageListener) {
        $this->messageListener = $messageListener;
        return $this;
    }
    
    /**
     * Set the maximum number of messages cached locally
     *
     * @param int $count Message count
     * @return PushConsumerBuilder
     */
    public function setMaxCacheMessageCount(int $count) {
        if ($count <= 0) {
            throw new ClientConfigurationException("maxCacheMessageCount should be positive");
        }
        $this->maxCacheMessageCount = $count;
        return $this;
    }
    
    /**
     * Set the maximum bytes of messages cached locally
     *
     * @param int $bytes Message size in bytes
     * @return PushConsumerBuilder
     */
    public function setMaxCacheMessageSizeInBytes(int $bytes) {
        if ($bytes <= 0) {
            throw new ClientConfigurationException("maxCacheMessageSizeInBytes should be positive");
        }
        $this->maxCacheMessageSizeInBytes = $bytes;
        return $this;
    }
    
    /**
     * Set the consumption thread count
     *
     * @param int $count Thread count
     * @return PushConsumerBuilder
     */
    public function setConsumptionThreadCount(int $count) {
        if ($count <= 0) {
            throw new ClientConfigurationException("consumptionThreadCount should be positive");
        }
        $this->consumptionThreadCount = $count;
        return $this;
    }
    
    /**
     * Set enable FIFO consume accelerator
     * If enabled, the consumer will consume messages in parallel by messageGroup
     *
     * @param bool $enable Enable or disable
     * @return PushConsumerBuilder
     */
    public function setEnableFifoConsumeAccelerator(bool $enable) {
        $this->enableFifoConsumeAccelerator = $enable;
        return $this;
    }
    
    /**
     * Set endpoints (convenience method)
     *
     * @param string $endpoints
     * @return PushConsumerBuilder
     */
    public function setEndpoints(string $endpoints) {
        $this->clientConfiguration = new ClientConfiguration($endpoints);
        return $this;
    }
    
    /**
     * Build and start the push consumer
     *
     * @return PushConsumer
     * @throws ClientConfigurationException
     */
    public function build(): PushConsumer {
        if ($this->clientConfiguration === null) {
            throw new ClientConfigurationException("clientConfiguration has not been set yet");
        }
        
        if (empty($this->consumerGroup)) {
            throw new ClientConfigurationException("consumerGroup has not been set yet");
        }
        
        if ($this->messageListener === null) {
            throw new ClientConfigurationException("messageListener has not been set yet");
        }
        
        if (empty($this->subscriptionExpressions)) {
            throw new ClientConfigurationException("subscriptionExpressions have not been set yet");
        }
        
        $consumer = new PushConsumerImpl(
            $this->clientConfiguration,
            $this->consumerGroup,
            $this->subscriptionExpressions,
            $this->messageListener,
            $this->maxCacheMessageCount,
            $this->maxCacheMessageSizeInBytes,
            $this->consumptionThreadCount,
            $this->enableFifoConsumeAccelerator
        );
        
        $consumer->start();
        
        return $consumer;
    }
}
