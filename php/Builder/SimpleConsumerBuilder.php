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
use Apache\Rocketmq\SimpleConsumer;

/**
 * Builder for creating SimpleConsumer instances
 */
class SimpleConsumerBuilder {
    /**
     * @var ClientConfiguration|null
     */
    private $clientConfiguration;
    
    /**
     * @var string|null
     */
    private $consumerGroup;
    
    /**
     * @var string|null
     */
    private $topic;
    
    /**
     * @var int
     */
    private $maxMessageNum = 32;
    
    /**
     * @var int
     */
    private $invisibleDuration = 15;
    
    /**
     * @var int
     */
    private $awaitDuration = 30;
    
    /**
     * Set client configuration
     *
     * @param ClientConfiguration $clientConfiguration
     * @return SimpleConsumerBuilder
     */
    public function setClientConfiguration(ClientConfiguration $clientConfiguration) {
        $this->clientConfiguration = $clientConfiguration;
        return $this;
    }
    
    /**
     * Set consumer group
     *
     * @param string $consumerGroup
     * @return SimpleConsumerBuilder
     */
    public function setConsumerGroup(string $consumerGroup) {
        $this->consumerGroup = $consumerGroup;
        return $this;
    }
    
    /**
     * Set topic
     *
     * @param string $topic
     * @return SimpleConsumerBuilder
     */
    public function setTopic(string $topic) {
        $this->topic = $topic;
        return $this;
    }
    
    /**
     * Set max message number
     *
     * @param int $maxMessageNum
     * @return SimpleConsumerBuilder
     */
    public function setMaxMessageNum(int $maxMessageNum) {
        $this->maxMessageNum = $maxMessageNum;
        return $this;
    }
    
    /**
     * Set invisible duration in seconds
     *
     * @param int $invisibleDuration
     * @return SimpleConsumerBuilder
     */
    public function setInvisibleDuration(int $invisibleDuration) {
        $this->invisibleDuration = $invisibleDuration;
        return $this;
    }
    
    /**
     * Set await duration in seconds
     *
     * @param int $awaitDuration
     * @return SimpleConsumerBuilder
     */
    public function setAwaitDuration(int $awaitDuration) {
        $this->awaitDuration = $awaitDuration;
        return $this;
    }
    
    /**
     * Build and start the simple consumer
     *
     * @return SimpleConsumer
     * @throws ClientConfigurationException
     */
    public function build() {
        if ($this->clientConfiguration === null) {
            throw new ClientConfigurationException("Client configuration must be set");
        }
        
        if (empty($this->consumerGroup)) {
            throw new ClientConfigurationException("Consumer group must be set");
        }
        
        if (empty($this->topic)) {
            throw new ClientConfigurationException("Topic must be set");
        }
        
        $consumer = new SimpleConsumer($this->clientConfiguration, $this->consumerGroup, $this->topic);
        $consumer->setMaxMessageNum($this->maxMessageNum);
        $consumer->setInvisibleDuration($this->invisibleDuration);
        $consumer->setAwaitDuration($this->awaitDuration);
        
        $consumer->start();
        return $consumer;
    }
}
