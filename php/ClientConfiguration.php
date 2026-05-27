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

namespace Apache\Rocketmq;

/**
 * ClientConfiguration - Immutable configuration value object.
 * Created exclusively by ClientConfigurationBuilder::build().
 * Referencing Java ClientConfiguration.java
 */
class ClientConfiguration
{
    private $endpoints;
    private $sessionCredentialsProvider;
    private $requestTimeoutMs;
    private $sslEnabled;
    private $namespace;
    private $maxStartupAttempts;
    private $tlsCredentials;

    private function __construct(
        string $endpoints,
        ?SessionCredentials $sessionCredentialsProvider,
        int $requestTimeoutMs,
        bool $sslEnabled,
        string $namespace,
        int $maxStartupAttempts,
        ?TlsCredentials $tlsCredentials = null
    ) {
        $this->endpoints = $endpoints;
        $this->sessionCredentialsProvider = $sessionCredentialsProvider;
        $this->requestTimeoutMs = $requestTimeoutMs;
        $this->sslEnabled = $sslEnabled;
        $this->namespace = $namespace;
        $this->maxStartupAttempts = $maxStartupAttempts;
        $this->tlsCredentials = $tlsCredentials;
    }

    /**
     * Factory method - only callable from ClientConfigurationBuilder.
     *
     * @internal
     */
    public static function create(
        string $endpoints,
        ?SessionCredentials $sessionCredentialsProvider,
        int $requestTimeoutMs,
        bool $sslEnabled,
        string $namespace,
        int $maxStartupAttempts,
        ?TlsCredentials $tlsCredentials = null
    ): ClientConfiguration {
        return new self(
            $endpoints,
            $sessionCredentialsProvider,
            $requestTimeoutMs,
            $sslEnabled,
            $namespace,
            $maxStartupAttempts,
            $tlsCredentials
        );
    }

    public function getEndpoints(): string
    {
        return $this->endpoints;
    }

    public function getSessionCredentialsProvider(): ?SessionCredentials
    {
        return $this->sessionCredentialsProvider;
    }

    public function getRequestTimeoutMs(): int
    {
        return $this->requestTimeoutMs;
    }

    public function isSslEnabled(): bool
    {
        return $this->sslEnabled;
    }

    public function getNamespace(): string
    {
        return $this->namespace;
    }

    public function getMaxStartupAttempts(): int
    {
        return $this->maxStartupAttempts;
    }

    public function getTlsCredentials(): ?TlsCredentials
    {
        return $this->tlsCredentials;
    }
}
