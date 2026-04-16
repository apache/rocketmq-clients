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

require_once __DIR__ . '/../Producer.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Exception\ClientConfigurationException;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Producer\TransactionChecker;

/**
 * Builder for creating Producer instances.
 *
 * References Java ProducerBuilderImpl design:
 * - Topic name regex validation
 * - maxAttempts positive check
 * - Startup error handling with resource cleanup
 */
class ProducerBuilder {

    private const TOPIC_PATTERN = '/^[%a-zA-Z0-9_-]+$/';

    /** @var ClientConfiguration|null */
    private ?ClientConfiguration $clientConfiguration = null;

    /** @var string[] Topics to declare */
    private array $topics = [];

    /** @var int Max attempts for message sending */
    private int $maxAttempts = 3;

    /** @var TransactionChecker|null */
    private ?TransactionChecker $transactionChecker = null;

    /**
     * Set client configuration.
     */
    public function setClientConfiguration(ClientConfiguration $clientConfiguration): self {
        $this->clientConfiguration = $clientConfiguration;
        return $this;
    }

    /**
     * Set topics to declare.
     * Each topic name must match the pattern [%a-zA-Z0-9_-]+.
     *
     * @param string ...$topics
     * @throws \InvalidArgumentException if any topic name is invalid
     */
    public function setTopics(string ...$topics): self {
        foreach ($topics as $topic) {
            if (!preg_match(self::TOPIC_PATTERN, $topic)) {
                throw new \InvalidArgumentException(
                    sprintf("topic does not match the regex [regex=%s]", self::TOPIC_PATTERN)
                );
            }
        }
        $this->topics = $topics;
        return $this;
    }

    /**
     * Set max attempts for message sending.
     *
     * @throws \InvalidArgumentException if maxAttempts is not positive
     */
    public function setMaxAttempts(int $maxAttempts): self {
        if ($maxAttempts <= 0) {
            throw new \InvalidArgumentException("maxAttempts should be positive");
        }
        $this->maxAttempts = $maxAttempts;
        return $this;
    }

    /**
     * Set transaction checker.
     */
    public function setTransactionChecker(TransactionChecker $transactionChecker): self {
        $this->transactionChecker = $transactionChecker;
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
     * Enable or disable SSL.
     *
     * @throws ClientConfigurationException if clientConfiguration has not been set
     */
    public function enableSsl(bool $enabled): self {
        if ($this->clientConfiguration === null) {
            throw new ClientConfigurationException("Client configuration must be set before enabling/disabling SSL");
        }
        $this->clientConfiguration->withSslEnabled($enabled);
        return $this;
    }

    /**
     * Build and start the producer.
     *
     * @return Producer
     * @throws ClientConfigurationException if required parameters are missing
     */
    public function build(): Producer {
        if ($this->clientConfiguration === null) {
            throw new ClientConfigurationException("clientConfiguration has not been set yet");
        }

        if (empty($this->topics)) {
            throw new ClientConfigurationException("At least one topic must be declared");
        }

        $topic = $this->topics[0];

        if ($this->transactionChecker !== null) {
            $producer = Producer::getTransactionalInstance(
                $this->clientConfiguration,
                $topic,
                $this->transactionChecker
            );
        } else {
            $producer = Producer::getInstance($this->clientConfiguration, $topic);
        }

        $producer->setMaxAttempts($this->maxAttempts);

        try {
            $producer->start();
        } catch (\Throwable $e) {
            try {
                $producer->shutdown();
            } catch (\Throwable $_) {
                // Suppress cleanup errors
            }
            throw new ClientConfigurationException("Failed to start producer: " . $e->getMessage(), 0, $e);
        }

        return $producer;
    }
}
