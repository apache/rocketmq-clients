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

namespace Apache\Rocketmq;

use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\Settings as V2Settings;
use Apache\Rocketmq\V2\Publishing;
use Apache\Rocketmq\V2\Resource;
use Google\Protobuf\Duration;

/**
 * Publishing settings for Producer.
 *
 * References Java PublishingSettings:
 * - sync() applies server-returned configuration (retryPolicy, maxBodySize, validateMessageType)
 * - toProtobuf() serializes local settings for telemetry handshake
 */
class PublishingSettings extends ClientSettings {

    /** @var string[] Topics to publish */
    private array $topics;

    /** @var int Max message body size in bytes (default: 4MB) */
    private int $maxBodySize = 4194304;

    /** @var bool Whether to validate message type */
    private bool $validateMessageType = true;

    public function __construct(
        string $namespace,
        string $clientId,
        string $endpoints,
        array $topics = [],
        int $maxAttempts = 3,
        int $requestTimeoutMs = 3000
    ) {
        parent::__construct(
            $namespace,
            $clientId,
            ClientType::PRODUCER,
            $endpoints,
            $maxAttempts,
            $requestTimeoutMs
        );

        $this->topics = $topics;
    }

    /**
     * {@inheritdoc}
     */
    public function toProtobuf(): V2Settings {
        $settings = new V2Settings();
        $settings->setAccessPoint($this->endpoints);
        $settings->setClientType(ClientType::PRODUCER);
        $settings->setRequestTimeout($this->requestTimeout);

        $publishing = new Publishing();
        $publishing->setValidateMessageType($this->validateMessageType);

        foreach ($this->topics as $topic) {
            $resource = new Resource();
            $resource->setName($topic);
            $resource->setResourceNamespace($this->namespace);
            $publishing->getTopics()[] = $resource;
        }

        $settings->setPublishing($publishing);

        if ($this->retryPolicy !== null) {
            $backoffPolicy = new \Apache\Rocketmq\V2\RetryPolicy();
            $backoffPolicy->setMaxAttempts($this->retryPolicy->getMaxAttempts());

            $exponential = new \Apache\Rocketmq\V2\ExponentialBackoff();
            $initial = new Duration();
            $initial->setNanos($this->retryPolicy->getInitialBackoff() * 1000000);
            $exponential->setInitial($initial);

            $max = new Duration();
            $max->setNanos($this->retryPolicy->getMaxBackoff() * 1000000);
            $exponential->setMax($max);

            $exponential->setMultiplier($this->retryPolicy->getBackoffMultiplier());
            $backoffPolicy->setExponentialBackoff($exponential);

            $settings->setBackoffPolicy($backoffPolicy);
        }

        return $settings;
    }

    /**
     * Sync settings from server-returned protobuf.
     *
     * Applies: retryPolicy, validateMessageType, maxBodySize.
     */
    public function sync(V2Settings $settings): void {
        if (!$settings->hasPublishing()) {
            Logger::error("[Bug] Unexpected pubSub case in publishing settings sync, clientId={$this->clientId}");
            return;
        }

        // Sync retry policy
        if ($settings->hasBackoffPolicy()) {
            $backoffPolicy = $settings->getBackoffPolicy();
            $maxAttempts = $backoffPolicy->getMaxAttempts();

            if ($backoffPolicy->hasExponentialBackoff()) {
                $exponential = $backoffPolicy->getExponentialBackoff();
                $initialMs = 100;
                $maxMs = 5000;
                $multiplier = 2.0;

                if ($exponential->hasInitial()) {
                    $initialMs = (int)($exponential->getInitial()->getSeconds() * 1000
                        + $exponential->getInitial()->getNanos() / 1000000);
                }
                if ($exponential->hasMax()) {
                    $maxMs = (int)($exponential->getMax()->getSeconds() * 1000
                        + $exponential->getMax()->getNanos() / 1000000);
                }
                $multiplier = $exponential->getMultiplier() > 0 ? $exponential->getMultiplier() : 2.0;

                $this->retryPolicy = new ExponentialBackoffRetryPolicy(
                    $maxAttempts > 0 ? $maxAttempts : $this->maxAttempts,
                    $initialMs,
                    $maxMs,
                    $multiplier
                );
            }

            if ($maxAttempts > 0) {
                $this->maxAttempts = $maxAttempts;
            }
        }

        // Sync publishing-specific settings
        $publishing = $settings->getPublishing();
        $this->validateMessageType = $publishing->getValidateMessageType();

        $maxBodySize = $publishing->getMaxBodySize();
        if ($maxBodySize > 0) {
            $this->maxBodySize = $maxBodySize;
        }
    }

    public function getTopics(): array {
        return $this->topics;
    }

    public function addTopic(string $topic): self {
        if (!in_array($topic, $this->topics, true)) {
            $this->topics[] = $topic;
        }
        return $this;
    }

    public function getMaxBodySize(): int {
        return $this->maxBodySize;
    }

    public function setMaxBodySize(int $maxBodySize): self {
        $this->maxBodySize = $maxBodySize;
        return $this;
    }

    public function isValidateMessageType(): bool {
        return $this->validateMessageType;
    }

    public function setValidateMessageType(bool $validate): self {
        $this->validateMessageType = $validate;
        return $this;
    }
}
