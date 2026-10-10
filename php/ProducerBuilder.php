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
 * ProducerBuilder — Fluent builder for constructing and starting a {@see Producer}.
 *
 * Provides a type-safe, chainable API to configure all producer options before
 * building and starting the underlying Producer instance. Using this builder is
 * the recommended way to create a Producer; constructing Producer directly with
 * raw $options arrays is deprecated.
 *
 * Required settings:
 *   - endpoints (via setEndpoints() or setClientConfiguration())
 *
 * Optional settings with defaults:
 *   - topics: []                        Topics to pre-warm publishing routes for
 *   - maxAttempts: 3                    Max retry attempts on transient send failures
 *   - requestTimeout: 3000              gRPC request timeout in milliseconds
 *   - namespace: ''                     Resource namespace prefix
 *   - credentials: null                 AK/SK SessionCredentials for authentication
 *   - validateMessageType: true         Validate message type against route accept types
 *   - maxBodySizeBytes: 4194304         Max allowed message body size (4 MB)
 *   - tlsCredentials: null              TlsCredentials for custom TLS configuration
 *   - sslEnabled: true                  Enable SSL/TLS on the gRPC channel
 *   - transactionChecker: null          TransactionChecker for orphaned-tx recovery
 *   - localTransactionExecuter: null     Executor for auto commit/rollback in sendWithTransaction()
 *
 * Usage example — basic producer:
 * ```php
 * $producer = (new ProducerBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setTopics('TopicA', 'TopicB')
 *     ->setMaxAttempts(5)
 *     ->build();
 *
 * $producer->send($message);
 * $producer->shutdown();
 * ```
 *
 * Usage example — transaction producer:
 * ```php
 * $producer = (new ProducerBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setTopics('TxTopic')
 *     ->setTransactionChecker(new MyTxChecker())
 *     ->setLocalTransactionExecuter(new MyTxExecutor())
 *     ->build();
 *
 * $tx = $producer->beginTransaction();
 * $producer->sendWithTransaction($message, $tx);
 * $tx->commit();
 * ```
 *
 * Usage example — with ClientConfiguration:
 * ```php
 * $config = (new ClientConfigurationBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setSessionCredentialsProvider(new SessionCredentials('ak', 'sk'))
 *     ->build();
 *
 * $producer = (new ProducerBuilder())
 *     ->setClientConfiguration($config)
 *     ->setTopics('TopicA')
 *     ->build();
 * ```
 *
 * @see Producer
 * @see ClientConfiguration
 * @see TransactionChecker
 * @see LocalTransactionExecuter
 */
class ProducerBuilder
{
    private string $endpoints = '';
    private ?SessionCredentials $credentials = null;
    private array $topics = [];
    private int $maxAttempts = 3;
    private int $requestTimeout = 3000;
    private string $namespace = '';
    private ?TransactionChecker $transactionChecker = null;
    private ?LocalTransactionExecuter $localTransactionExecuter = null;
    private bool $validateMessageType = true;
    private int $maxBodySizeBytes = 4194304;
    private ?TlsCredentials $tlsCredentials = null;
    private bool $sslEnabled = true;

    /**
     * Bulk-import settings from a {@see ClientConfiguration} instance.
     *
     * Copies endpoints, credentials, requestTimeout, namespace, sslEnabled, and
     * tlsCredentials from the config object.  Individual setter calls made
     * **after** this method will override the imported values.
     *
     * @param ClientConfiguration $config Pre-built client configuration
     * @return $this For method chaining
     */
    public function setClientConfiguration(ClientConfiguration $config): self
    {
        $this->endpoints = $config->getEndpoints();
        $this->credentials = $config->getSessionCredentialsProvider();
        $this->requestTimeout = $config->getRequestTimeoutMs();
        $this->namespace = $config->getNamespace();
        $this->sslEnabled = $config->isSslEnabled();
        if ($config->getTlsCredentials() !== null) {
            $this->tlsCredentials = $config->getTlsCredentials();
        }
        return $this;
    }

    /**
     * Set the gRPC endpoint address.
     *
     * Format: "host:port" or comma-separated list "host1:port1,host2:port2".
     * This is a required setting; build() will throw if not set (and not
     * provided via setClientConfiguration()).
     *
     * @param string $endpoints gRPC endpoint, e.g. "127.0.0.1:8081"
     * @return $this For method chaining
     */
    public function setEndpoints(string $endpoints): self
    {
        $this->endpoints = $endpoints;
        return $this;
    }

    /**
     * Set AK/SK authentication credentials.
     *
     * When set, every gRPC request is signed with the provided Access Key and
     * Secret Key. Pass null to disable authentication (only suitable for
     * development/testing environments).
     *
     * @param SessionCredentials $credentials AK/SK credential pair
     * @return $this For method chaining
     * @default null (no authentication)
     */
    public function setCredentials(SessionCredentials $credentials): self
    {
        $this->credentials = $credentials;
        return $this;
    }

    /**
     * Declare topics to pre-warm publishing routes for.
     *
     * On start(), the producer fetches routing information for each declared
     * topic so the first send() does not pay a route-lookup penalty.
     * Topics can also be omitted here and resolved lazily on first send.
     *
     * @param string ...$topics One or more topic names (variadic)
     * @return $this For method chaining
     * @default [] (no pre-warming)
     */
    public function setTopics(string ...$topics): self
    {
        $this->topics = $topics;
        return $this;
    }

    /**
     * Set max retry attempts for transient send failures.
     *
     * Controls how many times the producer retries when SendMessage fails due
     * to transient errors (network timeout, broker unavailability, etc.).
     * Each retry may target a different broker queue via round-robin.
     *
     * @param int $maxAttempts Max retry count
     * @return $this For method chaining
     * @default 3
     * @valid-range Must be > 0
     * @throws \InvalidArgumentException if $maxAttempts <= 0
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
     * Set transaction checker for orphaned transaction recovery (2PC Phase 4).
     *
     * Required when using beginTransaction() / sendWithTransaction().
     * The server sends a RecoverOrphanedTransaction telemetry command when it
     * detects an unresolved half-message; the checker inspects the local
     * transaction state and returns COMMIT or ROLLBACK.
     *
     * @param TransactionChecker $checker Implementation of TransactionChecker
     * @return $this For method chaining
     * @default null (transaction messaging disabled)
     */
    public function setTransactionChecker(TransactionChecker $checker): self
    {
        $this->transactionChecker = $checker;
        return $this;
    }

    /**
     * Set local transaction executor for auto commit/rollback (2PC Phase 2).
     *
     * When provided, sendWithTransaction() will automatically execute the local
     * transaction after the half-message is sent, and commit or rollback based
     * on the executor's return value (TransactionResolution::COMMIT or ROLLBACK).
     * If not set, the caller must manually call $transaction->commit() or
     * $transaction->rollback() after sendWithTransaction() returns.
     *
     * @param LocalTransactionExecuter $executer Implementation of LocalTransactionExecuter
     * @return $this For method chaining
     * @default null (manual commit/rollback required)
     */
    public function setLocalTransactionExecuter(LocalTransactionExecuter $executer): self
    {
        $this->localTransactionExecuter = $executer;
        return $this;
    }

    /**
     * Set whether to validate message type against route accept types before send.
     *
     * When enabled, send() / sendBatch() / sendWithTransaction() will check that
     * the detected message type (NORMAL, FIFO, DELAY, PRIORITY, TRANSACTION, LITE)
     * is accepted by the target broker queue. A mismatch throws before the gRPC
     * call is made. This setting can also be toggled dynamically by the server
     * via TelemetrySession settings push.
     *
     * @param bool $validate true to enable validation, false to skip
     * @return $this For method chaining
     * @default true
     */
    public function setValidateMessageType(bool $validate): self
    {
        $this->validateMessageType = $validate;
        return $this;
    }

    /**
     * Set max allowed message body size in bytes.
     *
     * Messages exceeding this limit are rejected with \InvalidArgumentException
     * before any network call. The server may push a different limit via
     * TelemetrySession settings (publishing.maxBodySize); the larger of the
     * client-side and server-side values does NOT apply — the server value
     * overwrites the client value when received.
     *
     * @param int $bytes Max body size in bytes
     * @return $this For method chaining
     * @default 4194304 (4 MB)
     * @valid-range Must be > 0
     * @throws \InvalidArgumentException if $bytes <= 0
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
     * Set gRPC request timeout in milliseconds.
     *
     * Applies to all unary gRPC calls (SendMessage, RecallMessage,
     * EndTransaction, etc.). For SendMessage specifically, the actual deadline
     * is derived from getOperationTimeout('SEND_MESSAGE') which may differ.
     *
     * @param int $timeoutMs Timeout in milliseconds
     * @return $this For method chaining
     * @default 3000 (3 seconds)
     * @valid-range Must be > 0
     * @throws \InvalidArgumentException if $timeoutMs <= 0
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
     * Set the resource namespace prefix.
     *
     * When set, all topic and consumer-group resource names are prefixed with
     * this namespace on the server side. Used in multi-tenant deployments to
     * isolate resources across environments (e.g. "dev", "staging", "prod").
     *
     * @param string $namespace Namespace string (empty string = no namespace)
     * @return $this For method chaining
     * @default '' (no namespace)
     */
    public function setNamespace(string $namespace): self
    {
        $this->namespace = $namespace;
        return $this;
    }

    /**
     * Set custom TLS credentials for the gRPC connection.
     *
     * When provided, the gRPC channel uses the specified CA certificate (and
     * optional client cert/key) instead of the system default trust store.
     * sslEnabled must be true for TLS to take effect.
     *
     * @param TlsCredentials $tlsCredentials TLS certificate configuration
     * @return $this For method chaining
     * @default null (use system trust store)
     */
    public function setTlsCredentials(TlsCredentials $tlsCredentials): self
    {
        $this->tlsCredentials = $tlsCredentials;
        return $this;
    }

    /**
     * Build, configure, and start the Producer.
     *
     * Behavior:
     *   1. Validates that endpoints is set (throws \RuntimeException if empty).
     *   2. Constructs a Producer with all accumulated settings.
     *   3. Attaches TransactionChecker and LocalTransactionExecuter if set.
     *   4. Calls Producer::start() which establishes the TelemetrySession,
     *      registers settings/transaction callbacks, warms up topic routes,
     *      and starts the HeartbeatManager.
     *   5. Returns a fully started, ready-to-send Producer.
     *
     * After build(), the builder instance should NOT be reused — create a new
     * builder for each Producer.
     *
     * @return Producer A started Producer instance
     * @throws \RuntimeException If endpoints is not set (empty string)
     * @throws \RuntimeException If Producer::start() fails (e.g. TelemetrySession
     *         cannot be established, gRPC connection refused)
     * @throws \InvalidArgumentException If maxAttempts, requestTimeout, or
     *         maxBodySizeBytes was set to an invalid value
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
            'tlsCredentials' => $this->tlsCredentials,
            'sslEnabled' => $this->sslEnabled,
        ]);

        if ($this->transactionChecker !== null) {
            $producer->setTransactionChecker($this->transactionChecker);
        }

        if ($this->localTransactionExecuter !== null) {
            $producer->setLocalTransactionExecuter($this->localTransactionExecuter);
        }

        $producer->start();
        return $producer;
    }
}
