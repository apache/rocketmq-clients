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

namespace Apache\Rocketmq\Builder;

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Exception\ClientConfigurationException;
use Apache\Rocketmq\Producer\LiteProducer;
use Apache\Rocketmq\Producer\LiteProducerImpl;

/**
 * Builder for creating LiteProducer instances.
 *
 * References Java LiteProducerBuilderImpl design:
 * - Topic name regex validation
 * - Mandatory configuration checks
 */
class LiteProducerBuilder {

    private const TOPIC_PATTERN = '/^[%a-zA-Z0-9_-]+$/';

    /** @var ClientConfiguration|null */
    private ?ClientConfiguration $configuration = null;

    /** @var string|null */
    private ?string $parentTopic = null;

    /**
     * Set client configuration.
     */
    public function setClientConfiguration(ClientConfiguration $configuration): self {
        $this->configuration = $configuration;
        return $this;
    }

    /**
     * Set parent topic name.
     *
     * @throws \InvalidArgumentException if parentTopic does not match the naming pattern
     */
    public function setParentTopic(string $parentTopic): self {
        if (!preg_match(self::TOPIC_PATTERN, $parentTopic)) {
            throw new \InvalidArgumentException(
                sprintf("parentTopic does not match the regex [regex=%s]", self::TOPIC_PATTERN)
            );
        }
        $this->parentTopic = $parentTopic;
        return $this;
    }

    /**
     * Build lite producer instance.
     *
     * @return LiteProducer
     * @throws ClientConfigurationException if configuration is invalid
     */
    public function build(): LiteProducer {
        if ($this->configuration === null) {
            throw new ClientConfigurationException("clientConfiguration has not been set yet");
        }

        if ($this->parentTopic === null || $this->parentTopic === '') {
            throw new ClientConfigurationException("parentTopic has not been set yet");
        }

        return new LiteProducerImpl($this->configuration, $this->parentTopic);
    }
}
