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
use Apache\Rocketmq\SimpleConsumer;

/**
 * Builder for creating SimpleConsumer instances.
 *
 * References Java SimpleConsumerBuilderImpl design:
 * - consumerGroup regex validation
 * - awaitDuration mandatory check
 * - Positive value checks for numeric parameters
 */
class SimpleConsumerBuilder {

    private const CONSUMER_GROUP_PATTERN = '/^[%a-zA-Z0-9_-]+$/';

    /** @var ClientConfiguration|null */
    private ?ClientConfiguration $clientConfiguration = null;

    /** @var string|null */
    private ?string $consumerGroup = null;

    /** @var string|null */
    private ?string $topic = null;

    /** @var int */
    private int $maxMessageNum = 32;

    /** @var int Invisible duration in seconds */
    private int $invisibleDuration = 15;

    /** @var int Await duration in seconds */
    private int $awaitDuration = 30;

    /**
     * Set client configuration.
     */
    public function setClientConfiguration(ClientConfiguration $clientConfiguration): self {
        $this->clientConfiguration = $clientConfiguration;
        return $this;
    }

    /**
     * Set consumer group.
     *
     * @throws \InvalidArgumentException if consumerGroup does not match the naming pattern
     */
    public function setConsumerGroup(string $consumerGroup): self {
        if (!preg_match(self::CONSUMER_GROUP_PATTERN, $consumerGroup)) {
            throw new \InvalidArgumentException(
                sprintf("consumerGroup does not match the regex [regex=%s]", self::CONSUMER_GROUP_PATTERN)
            );
        }
        $this->consumerGroup = $consumerGroup;
        return $this;
    }

    /**
     * Set topic.
     */
    public function setTopic(string $topic): self {
        $this->topic = $topic;
        return $this;
    }

    /**
     * Set max message number per receive.
     *
     * @throws \InvalidArgumentException if maxMessageNum is not positive
     */
    public function setMaxMessageNum(int $maxMessageNum): self {
        if ($maxMessageNum <= 0) {
            throw new \InvalidArgumentException("maxMessageNum must be greater than 0");
        }
        $this->maxMessageNum = $maxMessageNum;
        return $this;
    }

    /**
     * Set invisible duration in seconds.
     *
     * @throws \InvalidArgumentException if invisibleDuration is not positive
     */
    public function setInvisibleDuration(int $invisibleDuration): self {
        if ($invisibleDuration <= 0) {
            throw new \InvalidArgumentException("invisibleDuration must be greater than 0");
        }
        $this->invisibleDuration = $invisibleDuration;
        return $this;
    }

    /**
     * Set await duration in seconds.
     *
     * @throws \InvalidArgumentException if awaitDuration is not positive
     */
    public function setAwaitDuration(int $awaitDuration): self {
        if ($awaitDuration <= 0) {
            throw new \InvalidArgumentException("awaitDuration must be greater than 0");
        }
        $this->awaitDuration = $awaitDuration;
        return $this;
    }

    /**
     * Build and start the simple consumer.
     *
     * @return SimpleConsumer
     * @throws ClientConfigurationException if required parameters are missing
     */
    public function build(): SimpleConsumer {
        if ($this->clientConfiguration === null) {
            throw new ClientConfigurationException("clientConfiguration has not been set yet");
        }

        if ($this->consumerGroup === null || $this->consumerGroup === '') {
            throw new ClientConfigurationException("consumerGroup has not been set yet");
        }

        if ($this->topic === null || $this->topic === '') {
            throw new ClientConfigurationException("topic has not been set yet");
        }

        $consumer = new SimpleConsumer($this->clientConfiguration, $this->consumerGroup, $this->topic);
        $consumer->setMaxMessageNum($this->maxMessageNum);
        $consumer->setInvisibleDuration($this->invisibleDuration);
        $consumer->setAwaitDuration($this->awaitDuration);

        try {
            $consumer->start();
        } catch (\Throwable $e) {
            throw new ClientConfigurationException("Failed to start simple consumer: " . $e->getMessage(), 0, $e);
        }

        return $consumer;
    }
}
