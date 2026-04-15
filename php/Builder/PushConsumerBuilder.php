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
use Apache\Rocketmq\Consumer\FilterExpression;
use Apache\Rocketmq\Exception\ClientConfigurationException;
use Apache\Rocketmq\PushConsumer;

/**
 * Builder for creating PushConsumer instances
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
     * @var array<string, FilterExpression>|null Subscription expressions map (topic => FilterExpression)
     */
    private $subscriptionExpressions = null;
    
    /**
     * @var callable|null
     */
    private $messageListener;
    
    /**
     * @var int
     */
    private $invisibleDuration = 15;
    
    /**
     * @var int
     */
    private $maxMessageNum = 16;
    
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
     * Set subscription expressions (recommended for multiple topics)
     * 
     * This method allows subscribing to multiple topics with different filter expressions.
     * Example:
     * ```php
     * $builder->setSubscriptionExpressions([
     *     'topicA' => new FilterExpression('*'),  // Subscribe all messages
     *     'topicB' => new FilterExpression('tag1 || tag2'),  // Filter by tags
     * ]);
     * ```
     *
     * @param array<string, FilterExpression|string> $subscriptionExpressions Map of topic to FilterExpression or tag string
     * @return PushConsumerBuilder
     */
    public function setSubscriptionExpressions(array $subscriptionExpressions) {
        $this->subscriptionExpressions = [];
        foreach ($subscriptionExpressions as $topic => $filter) {
            if (is_string($filter)) {
                // Convert string to FilterExpression (default TAG type)
                $this->subscriptionExpressions[$topic] = new FilterExpression($filter);
            } else {
                $this->subscriptionExpressions[$topic] = $filter;
            }
        }
        return $this;
    }
    
    /**
     * Set single topic subscription (backward compatible)
     *
     * @param string $topic Topic name
     * @param FilterExpression|string|null $filterExpression Optional filter expression (default: subscribe all)
     * @return PushConsumerBuilder
     */
    public function setTopic(string $topic, $filterExpression = null) {
        if ($filterExpression === null) {
            $filterExpression = new FilterExpression('*');
        } elseif (is_string($filterExpression)) {
            $filterExpression = new FilterExpression($filterExpression);
        }
        
        $this->subscriptionExpressions = [$topic => $filterExpression];
        return $this;
    }
    
    /**
     * Set message listener
     *
     * @param callable|MessageListener $messageListener
     * @return PushConsumerBuilder
     */
    public function setMessageListener($messageListener) {
        $this->messageListener = $messageListener;
        return $this;
    }
    
    /**
     * Set invisible duration in seconds
     *
     * @param int $invisibleDuration
     * @return PushConsumerBuilder
     */
    public function setInvisibleDuration(int $invisibleDuration) {
        $this->invisibleDuration = $invisibleDuration;
        return $this;
    }
    
    /**
     * Set max message number
     *
     * @param int $maxMessageNum
     * @return PushConsumerBuilder
     */
    public function setMaxMessageNum(int $maxMessageNum) {
        $this->maxMessageNum = $maxMessageNum;
        return $this;
    }
    
    /**
     * Set endpoints
     *
     * @param string $endpoints
     * @return PushConsumerBuilder
     */
    public function setEndpoints(string $endpoints) {
        $this->clientConfiguration = new ClientConfiguration($endpoints);
        return $this;
    }
    
    /**
     * Enable or disable SSL
     *
     * @param bool $enabled Whether to enable SSL
     * @return PushConsumerBuilder
     */
    public function enableSsl(bool $enabled) {
        if ($this->clientConfiguration === null) {
            throw new ClientConfigurationException("Client configuration must be set before enabling/disabling SSL");
        }
        $this->clientConfiguration->withSslEnabled($enabled);
        return $this;
    }
    
    /**
     * Build and start the push consumer
     *
     * @return PushConsumer
     * @throws ClientConfigurationException
     */
    public function build() {
        if ($this->clientConfiguration === null) {
            throw new ClientConfigurationException("Client configuration must be set");
        }
        
        if (empty($this->consumerGroup)) {
            throw new ClientConfigurationException("Consumer group must be set");
        }
        
        if (empty($this->subscriptionExpressions)) {
            throw new ClientConfigurationException("At least one subscription expression must be set");
        }
        
        if ($this->messageListener === null) {
            throw new ClientConfigurationException("Message listener must be set");
        }
        
        // Get the first topic for backward compatibility with PushConsumer::getInstance
        $firstTopic = array_key_first($this->subscriptionExpressions);
        
        $consumer = PushConsumer::getInstance(
            $this->clientConfiguration,
            $this->consumerGroup,
            $firstTopic
        );
        
        // Set all subscription expressions during initialization phase
        // This is allowed before the consumer starts (similar to Java's constructor injection)
        foreach ($this->subscriptionExpressions as $topic => $filterExpression) {
            $consumer->subscribe($topic, $filterExpression);
        }
        
        // Mark subscription as initialized (allows subscribe before start)
        // Note: In PHP, we need to use reflection or a setter to access private property
        // For now, we rely on the check in subscribe() that allows calls when subscriptionInitialized is false
        
        $consumer->setMessageListener($this->messageListener);
        $consumer->setInvisibleDuration($this->invisibleDuration);
        $consumer->setMaxMessageNum($this->maxMessageNum);
        
        return $consumer;
    }
}
