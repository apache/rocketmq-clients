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

require_once __DIR__ . '/../Producer.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Exception\ClientConfigurationException;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Producer\TransactionChecker;

/**
 * Builder for creating Producer instances
 */
class ProducerBuilder {
    /**
     * @var ClientConfiguration|null
     */
    private $clientConfiguration;
    
    /**
     * @var array Topics to declare
     */
    private $topics = [];
    
    /**
     * @var int Max attempts for message sending
     */
    private $maxAttempts = 3;
    
    /**
     * @var TransactionChecker|null
     */
    private $transactionChecker;
    
    /**
     * Set client configuration
     *
     * @param ClientConfiguration $clientConfiguration
     * @return ProducerBuilder
     */
    public function setClientConfiguration(ClientConfiguration $clientConfiguration) {
        $this->clientConfiguration = $clientConfiguration;
        return $this;
    }
    
    /**
     * Set topics to declare
     *
     * @param string ...$topics
     * @return ProducerBuilder
     */
    public function setTopics(string ...$topics) {
        $this->topics = $topics;
        return $this;
    }
    
    /**
     * Set max attempts for message sending
     *
     * @param int $maxAttempts
     * @return ProducerBuilder
     */
    public function setMaxAttempts(int $maxAttempts) {
        $this->maxAttempts = $maxAttempts;
        return $this;
    }
    
    /**
     * Set transaction checker
     *
     * @param TransactionChecker $transactionChecker
     * @return ProducerBuilder
     */
    public function setTransactionChecker(TransactionChecker $transactionChecker) {
        $this->transactionChecker = $transactionChecker;
        return $this;
    }
    
    /**
     * Set endpoints
     *
     * @param string $endpoints
     * @return ProducerBuilder
     */
    public function setEndpoints(string $endpoints) {
        $this->clientConfiguration = new ClientConfiguration($endpoints);
        return $this;
    }
    
    /**
     * Enable or disable SSL
     *
     * @param bool $enabled Whether to enable SSL
     * @return ProducerBuilder
     */
    public function enableSsl(bool $enabled) {
        if ($this->clientConfiguration === null) {
            throw new ClientConfigurationException("Client configuration must be set before enabling/disabling SSL");
        }
        $this->clientConfiguration->withSslEnabled($enabled);
        return $this;
    }
    
    /**
     * Build and start the producer
     *
     * @return Producer
     * @throws ClientConfigurationException
     */
    public function build() {
        if ($this->clientConfiguration === null) {
            throw new ClientConfigurationException("Client configuration must be set");
        }
        
        if (empty($this->topics)) {
            throw new ClientConfigurationException("At least one topic must be declared");
        }
        
        // For simplicity, we'll use the first topic as the primary topic
        $topic = $this->topics[0];
        
        $producer = Producer::getInstance($this->clientConfiguration, $topic);
        $producer->setMaxAttempts($this->maxAttempts);
        
        if ($this->transactionChecker !== null) {
            $producer = Producer::getTransactionalInstance($this->clientConfiguration, $topic, $this->transactionChecker);
            $producer->setMaxAttempts($this->maxAttempts);
        }
        
        $producer->start();
        return $producer;
    }
}
