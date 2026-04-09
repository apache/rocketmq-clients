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
use Apache\Rocketmq\Consumer\LiteSimpleConsumer;
use Apache\Rocketmq\Exception\ClientConfigurationException;

/**
 * Lite simple consumer builder
 * 
 * Builds LiteSimpleConsumer instances with configurable options
 * 
 * @see LiteSimpleConsumer
 */
class LiteSimpleConsumerBuilder {
    /**
     * @var ClientConfiguration Client configuration
     */
    private $configuration;
    
    /**
     * @var string Consumer group name
     */
    private $consumerGroup;
    
    /**
     * @var int Max message num per receive
     */
    private $maxMessageNum = 32;
    
    /**
     * @var int Invisible duration in seconds
     */
    private $invisibleDuration = 30;
    
    /**
     * @var int Await duration in seconds
     */
    private $awaitDuration = 30;
    
    /**
     * Set client configuration
     * 
     * @param ClientConfiguration $configuration Client configuration
     * @return LiteSimpleConsumerBuilder This builder instance
     */
    public function setClientConfiguration(ClientConfiguration $configuration): self {
        $this->configuration = $configuration;
        return $this;
    }
    
    /**
     * Set consumer group
     * 
     * @param string $consumerGroup Consumer group name
     * @return LiteSimpleConsumerBuilder This builder instance
     */
    public function setConsumerGroup(string $consumerGroup): self {
        $this->consumerGroup = $consumerGroup;
        return $this;
    }
    
    /**
     * Set max message num per receive
     * 
     * @param int $maxMessageNum Max message num
     * @return LiteSimpleConsumerBuilder This builder instance
     */
    public function setMaxMessageNum(int $maxMessageNum): self {
        $this->maxMessageNum = $maxMessageNum;
        return $this;
    }
    
    /**
     * Set invisible duration
     * 
     * @param int $invisibleDuration Invisible duration in seconds
     * @return LiteSimpleConsumerBuilder This builder instance
     */
    public function setInvisibleDuration(int $invisibleDuration): self {
        $this->invisibleDuration = $invisibleDuration;
        return $this;
    }
    
    /**
     * Set await duration
     * 
     * @param int $awaitDuration Await duration in seconds
     * @return LiteSimpleConsumerBuilder This builder instance
     */
    public function setAwaitDuration(int $awaitDuration): self {
        $this->awaitDuration = $awaitDuration;
        return $this;
    }
    
    /**
     * Build lite simple consumer instance
     * 
     * @return LiteSimpleConsumer Lite simple consumer instance
     * @throws ClientConfigurationException If configuration is invalid
     */
    public function build(): LiteSimpleConsumer {
        // Validate configuration
        if (!$this->configuration) {
            throw new ClientConfigurationException('Client configuration is required');
        }
        if (empty($this->consumerGroup)) {
            throw new ClientConfigurationException('Consumer group is required');
        }
        
        // Create lite simple consumer instance
        $consumer = new \Apache\Rocketmq\Consumer\LiteSimpleConsumerImpl(
            $this->configuration,
            $this->consumerGroup,
            $this->maxMessageNum,
            $this->invisibleDuration,
            $this->awaitDuration
        );
        
        return $consumer;
    }
}
