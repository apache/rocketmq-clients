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
 * ProducerBuilder - Fluent builder for Producer.
 * Referencing Java ProducerBuilder.java
 */
class ProducerBuilder
{
    private $endpoints = '';
    private $credentials = null;
    private $topics = [];
    private $maxAttempts = 3;
    private $requestTimeout = 3000;
    private $namespace = '';
    private $transactionChecker = null;
    private $validateMessageType = true;
    private $maxBodySizeBytes = 4194304;

    /**
     * Set client configuration.
     *
     * @return $this
     */
    public function setClientConfiguration(ClientConfiguration $config): self
    {
        $this->endpoints = $config->getEndpoints();
        $this->credentials = $config->getSessionCredentialsProvider();
        $this->requestTimeout = $config->getRequestTimeoutMs();
        $this->namespace = $config->getNamespace();
        return $this;
    }

    /**
     * Set the gRPC endpoint address.
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
     * Set AK/SK credentials.
     *
     * @return $this
     */
    public function setCredentials(SessionCredentials $credentials): self
    {
        $this->credentials = $credentials;
        return $this;
    }

    /**
     * Declare topics to send messages to.
     *
     * @param string ...$topics
     * @return $this
     */
    public function setTopics(string ...$topics): self
    {
        $this->topics = $topics;
        return $this;
    }

    /**
     * Set max retry attempts for send failures.
     *
     * @param int $maxAttempts Default 3
     * @return $this
     */
    public function setMaxAttempts(int $maxAttempts): self
    {
        if ($maxAttempts <= 0) {
            throw new \InvalidArgumentException("maxAttempts must be > 0");
        }
        $this->maxAttempts = $maxAttempts;
        return $this;
    }

    /**
     * Set transaction checker for orphaned transaction recovery.
     *
     * @return $this
     */
    public function setTransactionChecker(TransactionChecker $checker): self
    {
        $this->transactionChecker = $checker;
        return $this;
    }

    /**
     * Set whether to validate message type against route accept types.
     *
     * @return $this
     */
    public function setValidateMessageType(bool $validate): self
    {
        $this->validateMessageType = $validate;
        return $this;
    }

    /**
     * Set max message body size in bytes.
     *
     * @return $this
     */
    public function setMaxBodySizeBytes(int $bytes): self
    {
        if ($bytes <= 0) {
            throw new \InvalidArgumentException("maxBodySizeBytes must be > 0");
        }
        $this->maxBodySizeBytes = $bytes;
        return $this;
    }

    /**
     * Set request timeout in milliseconds.
     *
     * @return $this
     */
    public function setRequestTimeout(int $timeoutMs): self
    {
        if ($timeoutMs <= 0) {
            throw new \InvalidArgumentException("requestTimeout must be > 0");
        }
        $this->requestTimeout = $timeoutMs;
        return $this;
    }

    /**
     * Set namespace.
     *
     * @return $this
     */
    public function setNamespace(string $namespace): self
    {
        $this->namespace = $namespace;
        return $this;
    }

    /**
     * Build and start the Producer.
     *
     * @return Producer
     * @throws \RuntimeException if endpoints not set
     */
    public function build(): Producer
    {
        if ($this->endpoints === '') {
            throw new \RuntimeException("Producer endpoints must be set");
        }

        $producer = new Producer($this->endpoints, [
            'topics' => $this->topics,
            'maxAttempts' => $this->maxAttempts,
            'requestTimeout' => $this->requestTimeout,
            'namespace' => $this->namespace,
            'credentials' => $this->credentials,
            'validateMessageType' => $this->validateMessageType,
            'maxBodySizeBytes' => $this->maxBodySizeBytes,
        ]);

        if ($this->transactionChecker !== null) {
            $producer->setTransactionChecker($this->transactionChecker);
        }

        $producer->start();
        return $producer;
    }
}
