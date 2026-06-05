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
 * ClientConfigurationBuilder - Fluent builder for ClientConfiguration.
 * Referencing Java ClientConfigurationBuilder.java
 */
class ClientConfigurationBuilder
{
    private ?string $endpoints = null;
    private ?SessionCredentials $sessionCredentialsProvider = null;
    private int $requestTimeoutMs = 3000; // default 3s
    private bool $sslEnabled = true;
    private string $namespace = '';
    private int $maxStartupAttempts = 3;
    private ?TlsCredentials $tlsCredentials = null;

    /**
     * Set the service endpoint address (required).
     *
     * @param string $endpoints e.g. "127.0.0.1:8080"
     * @return $this
     */
    public function setEndpoints(string $endpoints): self
    {
        $this->endpoints = $endpoints;
        return $this;
    }

    /**
     * Set authentication credentials provider.
     *
     * @param SessionCredentials|null $credentials
     * @return $this
     */
    public function setCredentialProvider(?SessionCredentials $credentials): self
    {
        $this->sessionCredentialsProvider = $credentials;
        return $this;
    }

    /**
     * Set RPC request timeout.
     *
     * @param int $timeoutMs Timeout in milliseconds (default 3000)
     * @return $this
     * @throws \InvalidArgumentException if timeout is zero or negative
     */
    public function setRequestTimeout(int $timeoutMs): self
    {
        if ($timeoutMs <= 0) {
            throw new \InvalidArgumentException("Request timeout must be > 0");
        }
        $this->requestTimeoutMs = $timeoutMs;
        return $this;
    }

    /**
     * Enable or disable SSL.
     *
     * @param bool $enabled Default true
     * @return $this
     */
    public function enableSsl(bool $enabled = true): self
    {
        $this->sslEnabled = $enabled;
        return $this;
    }

    /**
     * Set namespace.
     *
     * @param string $namespace Default empty
     * @return $this
     */
    public function setNamespace(string $namespace): self
    {
        $this->namespace = $namespace;
        return $this;
    }

    /**
     * Set max startup retry attempts.
     *
     * @param int $attempts Must be > 0
     * @return $this
     * @throws \InvalidArgumentException if attempts is zero or negative
     */
    public function setMaxStartupAttempts(int $attempts): self
    {
        if ($attempts <= 0) {
            throw new \InvalidArgumentException("Max startup attempts must be > 0");
        }
        $this->maxStartupAttempts = $attempts;
        return $this;
    }

    /**
     * Set TLS credentials for the gRPC connection.
     *
     * @param TlsCredentials $tlsCredentials
     * @return $this
     */
    public function setTlsCredentials(TlsCredentials $tlsCredentials): self
    {
        $this->tlsCredentials = $tlsCredentials;
        $this->sslEnabled = true;
        return $this;
    }

    /**
     * Enable TLS with default system CA bundle.
     *
     * @return $this
     */
    public function enableTls(): self
    {
        $this->tlsCredentials = TlsCredentials::createDefault();
        $this->sslEnabled = true;
        return $this;
    }

    /**
     * Enable mutual TLS (mTLS) with client certificate and key.
     *
     * @param string $clientCertPath Path to client certificate file
     * @param string $clientKeyPath Path to client private key file
     * @param string|null $caCertPath Optional CA certificate path
     * @return $this
     */
    public function enableMutualTls(
        string $clientCertPath,
        string $clientKeyPath,
        ?string $caCertPath = null
    ): self {
        $this->tlsCredentials = TlsCredentials::createMtls(
            $clientCertPath,
            $clientKeyPath,
            $caCertPath
        );
        $this->sslEnabled = true;
        return $this;
    }

    /**
     * Enable TLS but skip peer verification (development only).
     *
     * ⚠️ SECURITY WARNING: This method is for internal testing use ONLY.
     * It should NOT be used in production code as it allows man-in-the-middle attacks.
     *
     * For development/testing, use environment variables or configuration files instead:
     * - Set ROCKETMQ_TLS_SKIP_VERIFY=true in your .env file
     * - Use TlsCredentials::createInsecureDev() directly with explicit awareness of risks
     *
     * @return $this
     * @internal Not intended for public API usage
     * @deprecated Will be removed in future versions. Use environment-based configuration instead.
     */
    public function disableTlsVerification(): self
    {
        trigger_error(
            "SECURITY WARNING: ClientConfigurationBuilder::disableTlsVerification() is deprecated and will be removed. " .
            "This method bypasses TLS certificate verification and allows man-in-the-middle attacks. " .
            "For development/testing, use TlsCredentials::createInsecureDev() explicitly with full awareness of security risks. " .
            "NEVER use this in production!",
            E_USER_DEPRECATED
        );
        
        Logger::getInstance('ClientConfiguration')->error(
            "CRITICAL SECURITY WARNING: TLS certificate verification is being disabled! " .
            "This makes the connection vulnerable to man-in-the-middle attacks. " .
            "This should ONLY be used in isolated development/testing environments. " .
            "DO NOT use this in production under any circumstances!"
        );
        
        $this->tlsCredentials = TlsCredentials::createInsecureDev();
        $this->sslEnabled = true;
        return $this;
    }

    /**
     * Build the immutable ClientConfiguration object.
     *
     * @return ClientConfiguration
     * @throws \InvalidArgumentException if endpoints not set
     */
    public function build(): ClientConfiguration
    {
        if ($this->endpoints === null || $this->endpoints === '') {
            throw new \InvalidArgumentException("Endpoints must be set");
        }
        if ($this->requestTimeoutMs <= 0) {
            throw new \InvalidArgumentException("Request timeout must be > 0");
        }

        return ClientConfiguration::create(
            $this->endpoints,
            $this->sessionCredentialsProvider,
            $this->requestTimeoutMs,
            $this->sslEnabled,
            $this->namespace,
            $this->maxStartupAttempts,
            $this->tlsCredentials
        );
    }
}
