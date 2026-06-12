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
    private readonly string $endpoints;
    private readonly ?SessionCredentials $sessionCredentialsProvider;
    private readonly int $requestTimeoutMs;
    private readonly bool $sslEnabled;
    private readonly string $namespace;
    private readonly int $maxStartupAttempts;
    private readonly ?TlsCredentials $tlsCredentials;

    /**
     * Private constructor - instantiated only via ClientConfiguration::create().
     *
     * @param string                     $endpoints                 Target endpoint addresses.
     * @param SessionCredentials|null    $sessionCredentialsProvider Session credentials provider for authentication.
     * @param int                        $requestTimeoutMs          RPC request timeout in milliseconds.
     * @param bool                       $sslEnabled                Whether SSL is enabled.
     * @param string                     $namespace                 Namespace for the client.
     * @param int                        $maxStartupAttempts        Maximum number of startup retry attempts.
     * @param TlsCredentials|null        $tlsCredentials            TLS credentials for secure connections.
     */
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
     * @param string                     $endpoints                 Target endpoint addresses.
     * @param SessionCredentials|null    $sessionCredentialsProvider Session credentials provider for authentication.
     * @param int                        $requestTimeoutMs          RPC request timeout in milliseconds.
     * @param bool                       $sslEnabled                Whether SSL is enabled.
     * @param string                     $namespace                 Namespace for the client.
     * @param int                        $maxStartupAttempts        Maximum number of startup retry attempts.
     * @param TlsCredentials|null        $tlsCredentials            TLS credentials for secure connections.
     * @return ClientConfiguration       New immutable configuration instance.
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

    /**
     * Get the target endpoints address.
     *
     * @return string Target endpoint addresses.
     */
    public function getEndpoints(): string
    {
        return $this->endpoints;
    }

    /**
     * Get the session credentials provider.
     *
     * @return SessionCredentials|null Session credentials provider, or null if not set.
     */
    public function getSessionCredentialsProvider(): ?SessionCredentials
    {
        return $this->sessionCredentialsProvider;
    }

    /**
     * Get the RPC request timeout in milliseconds.
     *
     * @return int RPC request timeout in milliseconds.
     */
    public function getRequestTimeoutMs(): int
    {
        return $this->requestTimeoutMs;
    }

    /**
     * Check whether SSL is enabled.
     *
     * @return bool True if SSL is enabled, false otherwise.
     */
    public function isSslEnabled(): bool
    {
        return $this->sslEnabled;
    }

    /**
     * Get the namespace.
     *
     * @return string Namespace for the client.
     */
    public function getNamespace(): string
    {
        return $this->namespace;
    }

    /**
     * Get the maximum number of startup retry attempts.
     *
     * @return int Maximum number of startup retry attempts.
     */
    public function getMaxStartupAttempts(): int
    {
        return $this->maxStartupAttempts;
    }

    /**
     * Get the TLS credentials for gRPC connections.
     *
     * @return TlsCredentials|null TLS credentials, or null if not set.
     */
    public function getTlsCredentials(): ?TlsCredentials
    {
        return $this->tlsCredentials;
    }
}
