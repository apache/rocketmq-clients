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
use Apache\Rocketmq\Consumer\PushConsumer;
use Apache\Rocketmq\Consumer\PushConsumerImpl;
use Apache\Rocketmq\Consumer\FilterExpression;

/**
 * Builder for creating PushConsumer instances.
 *
 * References Java PushConsumerBuilderImpl design:
 * - consumerGroup regex validation
 * - Subscription expressions mandatory check
 * - Positive value checks for cache/thread parameters
 * - Startup error handling with resource cleanup
 */
class PushConsumerBuilder {

    private const CONSUMER_GROUP_PATTERN = '/^[%a-zA-Z0-9_-]+$/';

    /** @var ClientConfiguration|null */
    private ?ClientConfiguration $clientConfiguration = null;

    /** @var string|null */
    private ?string $consumerGroup = null;

    /** @var array<string, FilterExpression> Subscription expressions */
    private array $subscriptionExpressions = [];

    /** @var callable|null */
    private $messageListener = null;

    /** @var int Maximum cache message count */
    private int $maxCacheMessageCount = 4096;

    /** @var int Maximum cache message size in bytes (64MB) */
    private int $maxCacheMessageSizeInBytes = 67108864;

    /** @var int Consumption thread count */
    private int $consumptionThreadCount = 20;

    /** @var bool Enable FIFO consume accelerator */
    private bool $enableFifoConsumeAccelerator = false;

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
     * Set subscription expressions.
     *
     * @param array<string, FilterExpression> $subscriptionExpressions
     * @throws ClientConfigurationException if array is empty
     */
    public function setSubscriptionExpressions(array $subscriptionExpressions): self {
        if (empty($subscriptionExpressions)) {
            throw new ClientConfigurationException("subscriptionExpressions should not be empty");
        }
        $this->subscriptionExpressions = $subscriptionExpressions;
        return $this;
    }

    /**
     * Add a single subscription expression.
     */
    public function addSubscription(string $topic, FilterExpression $filterExpression): self {
        $this->subscriptionExpressions[$topic] = $filterExpression;
        return $this;
    }

    /**
     * Set message listener.
     */
    public function setMessageListener(callable $messageListener): self {
        $this->messageListener = $messageListener;
        return $this;
    }

    /**
     * Set the maximum number of messages cached locally.
     *
     * @throws \InvalidArgumentException if count is not positive
     */
    public function setMaxCacheMessageCount(int $count): self {
        if ($count <= 0) {
            throw new \InvalidArgumentException("maxCacheMessageCount should be positive");
        }
        $this->maxCacheMessageCount = $count;
        return $this;
    }

    /**
     * Set the maximum bytes of messages cached locally.
     *
     * @throws \InvalidArgumentException if bytes is not positive
     */
    public function setMaxCacheMessageSizeInBytes(int $bytes): self {
        if ($bytes <= 0) {
            throw new \InvalidArgumentException("maxCacheMessageSizeInBytes should be positive");
        }
        $this->maxCacheMessageSizeInBytes = $bytes;
        return $this;
    }

    /**
     * Set the consumption thread count.
     *
     * @throws \InvalidArgumentException if count is not positive
     */
    public function setConsumptionThreadCount(int $count): self {
        if ($count <= 0) {
            throw new \InvalidArgumentException("consumptionThreadCount should be positive");
        }
        $this->consumptionThreadCount = $count;
        return $this;
    }

    /**
     * Enable or disable FIFO consume accelerator.
     * If enabled, the consumer will consume messages in parallel by messageGroup.
     */
    public function setEnableFifoConsumeAccelerator(bool $enable): self {
        $this->enableFifoConsumeAccelerator = $enable;
        return $this;
    }

    /**
     * Set endpoints (convenience method).
     */
    public function setEndpoints(string $endpoints): self {
        $this->clientConfiguration = new ClientConfiguration($endpoints);
        return $this;
    }

    /**
     * Build and start the push consumer.
     *
     * @return PushConsumer
     * @throws ClientConfigurationException if required parameters are missing
     */
    public function build(): PushConsumer {
        if ($this->clientConfiguration === null) {
            throw new ClientConfigurationException("clientConfiguration has not been set yet");
        }

        if ($this->consumerGroup === null || $this->consumerGroup === '') {
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

        try {
            $consumer->start();
        } catch (\Throwable $e) {
            try {
                $consumer->shutdown();
            } catch (\Throwable $_) {
                // Suppress cleanup errors
            }
            throw new ClientConfigurationException("Failed to start push consumer: " . $e->getMessage(), 0, $e);
        }

        return $consumer;
    }
}
