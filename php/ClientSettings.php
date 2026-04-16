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
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\BackoffStrategy;
use Google\Protobuf\Duration;

/**
 * Base settings class for client configuration.
 *
 * References Java Settings abstract class:
 * - Defines toProtobuf() and sync() contract
 * - Manages namespace, clientId, clientType, endpoints, retryPolicy, requestTimeout
 */
abstract class ClientSettings {

    /** @var string Namespace */
    protected string $namespace;

    /** @var string Client ID */
    protected string $clientId;

    /** @var int Client type (use V2\ClientType constants) */
    protected int $clientType;

    /** @var string Endpoints */
    protected string $endpoints;

    /** @var int Max attempts for retry */
    protected int $maxAttempts = 3;

    /** @var Duration Request timeout */
    protected Duration $requestTimeout;

    /** @var ExponentialBackoffRetryPolicy|null Retry policy */
    protected ?ExponentialBackoffRetryPolicy $retryPolicy = null;

    /**
     * @param string $namespace
     * @param string $clientId
     * @param int $clientType Client type (use V2\ClientType constants)
     * @param string $endpoints
     * @param int $maxAttempts
     * @param int $requestTimeoutMs Request timeout in milliseconds
     */
    public function __construct(
        string $namespace,
        string $clientId,
        int $clientType,
        string $endpoints,
        int $maxAttempts = 3,
        int $requestTimeoutMs = 3000
    ) {
        $this->namespace = $namespace;
        $this->clientId = $clientId;
        $this->clientType = $clientType;
        $this->endpoints = $endpoints;
        $this->maxAttempts = $maxAttempts;

        $timeout = new Duration();
        $timeout->setSeconds(intdiv($requestTimeoutMs, 1000));
        $timeout->setNanos(($requestTimeoutMs % 1000) * 1000000);
        $this->requestTimeout = $timeout;
    }

    /**
     * Convert to protobuf Settings.
     */
    abstract public function toProtobuf(): V2Settings;

    /**
     * Sync settings from server-returned protobuf.
     *
     * Subclasses must implement this to apply server-side configuration updates
     * (e.g., retry policy, maxBodySize, validateMessageType).
     */
    abstract public function sync(V2Settings $settings): void;

    public function getNamespace(): string {
        return $this->namespace;
    }

    public function getClientId(): string {
        return $this->clientId;
    }

    public function getClientType(): int {
        return $this->clientType;
    }

    public function getEndpoints(): string {
        return $this->endpoints;
    }

    public function getMaxAttempts(): int {
        return $this->maxAttempts;
    }

    public function getRequestTimeout(): Duration {
        return $this->requestTimeout;
    }

    public function getRetryPolicy(): ?ExponentialBackoffRetryPolicy {
        return $this->retryPolicy;
    }
}
