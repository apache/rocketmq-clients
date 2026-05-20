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
    private $endpoints = null;
    private $sessionCredentialsProvider = null;
    private $requestTimeoutMs = 3000; // default 3s
    private $sslEnabled = true;
    private $namespace = '';
    private $maxStartupAttempts = 3;

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
            $this->maxStartupAttempts
        );
    }
}
