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
use Apache\Rocketmq\Producer\LiteProducer;
use Apache\Rocketmq\Producer\LiteProducerImpl;

/**
 * Builder for creating LiteProducer instances
 * 
 * Example usage:
 * ```php
 * $config = new ClientConfiguration('127.0.0.1:8080');
 * $producer = (new LiteProducerBuilder())
 *     ->setClientConfiguration($config)
 *     ->setParentTopic('yourParentTopic')
 *     ->build();
 * ```
 */
class LiteProducerBuilder {
    /**
     * @var ClientConfiguration|null Client configuration
     */
    private $configuration = null;
    
    /**
     * @var string|null Parent topic name
     */
    private $parentTopic = null;
    
    /**
     * Set client configuration
     * 
     * @param ClientConfiguration $configuration Client configuration
     * @return LiteProducerBuilder This builder instance
     */
    public function setClientConfiguration(ClientConfiguration $configuration): LiteProducerBuilder {
        $this->configuration = $configuration;
        return $this;
    }
    
    /**
     * Set parent topic name
     * 
     * The parent topic is the main topic under which lite topics are created.
     * Lite topics are sub-topics that share resources with the parent topic.
     * 
     * @param string $parentTopic Parent topic name
     * @return LiteProducerBuilder This builder instance
     */
    public function setParentTopic(string $parentTopic): LiteProducerBuilder {
        $this->parentTopic = $parentTopic;
        return $this;
    }
    
    /**
     * Build lite producer instance
     * 
     * @return LiteProducer Lite producer instance
     * @throws ClientConfigurationException If configuration is invalid
     */
    public function build(): LiteProducer {
        // Validate configuration
        if (!$this->configuration) {
            throw new ClientConfigurationException('Client configuration is required');
        }
        
        if (empty($this->parentTopic)) {
            throw new ClientConfigurationException('Parent topic is required for lite producer');
        }
        
        // Create lite producer instance
        return new LiteProducerImpl($this->configuration, $this->parentTopic);
    }
}
