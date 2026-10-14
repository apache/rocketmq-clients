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
 * PushConsumerBuilder — Fluent builder for constructing and starting a {@see PushConsumer}.
 *
 * Provides a type-safe, chainable API to configure a push-based consumer that
 * automatically receives and dispatches messages via a registered listener callback.
 * The consumer uses long-polling internally and manages message caching, FIFO
 * ordering, and lifecycle automatically.
 *
 * Required settings:
 *   - endpoints        (via setEndpoints() or setClientConfiguration())
 *   - consumerGroup    (via setConsumerGroup())
 *   - messageListener  (via setMessageListener())
 *   - subscriptions    (via setSubscriptionExpressions() or subscribe())
 *
 * Optional settings with defaults:
 *   - subscriptionExpressions: []       ['topic' => 'filterExpression'] map
 *   - maxCacheMessageCount: 4096        Max number of messages buffered in memory
 *   - maxCacheMessageSizeInBytes: 67108864  Max total size of buffered messages (64 MB)
 *   - awaitDuration: 30                Long-poll wait time in seconds
 *   - consumptionThreadCount: 1        Thread count (no-op in PHP, stored for API parity)
 *   - fifo: false                      Enable strict FIFO consumption ordering
 *   - enableFifoConsumeAccelerator: false  Parallel processing by messageGroup
 *   - enableMessageInterceptorFiltering: false  Client-side interceptor filtering
 *   - namespace: ''                     Resource namespace prefix
 *   - credentials: null                 AK/SK SessionCredentials for authentication
 *   - tlsCredentials: null              TlsCredentials for custom TLS configuration
 *
 * Usage example — basic push consumer:
 * ```php
 * $consumer = (new PushConsumerBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setConsumerGroup('my-group')
 *     ->subscribe('TopicA', '*')
 *     ->setMessageListener(function (MessageView $mv): int {
 *         echo $mv->getBody();
 *         return ConsumeResult::SUCCESS;
 *     })
 *     ->build();
 * ```
 *
 * Usage example — FIFO consumer with bounded duration:
 * ```php
 * (new PushConsumerBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setConsumerGroup('fifo-group')
 *     ->setFifo(true)
 *     ->setEnableFifoConsumeAccelerator(true)
 *     ->subscribe('FifoTopic')
 *     ->setMessageListener(function (MessageView $mv): int {
 *         return ConsumeResult::SUCCESS;
 *     })
 *     ->startFor(60); // run for 60 seconds, then shutdown
 * ```
 *
 * Usage example — with ClientConfiguration and async start:
 * ```php
 * $config = (new ClientConfigurationBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setSessionCredentialsProvider(new SessionCredentials('ak', 'sk'))
 *     ->build();
 *
 * $consumer = (new PushConsumerBuilder())
 *     ->setClientConfiguration($config)
 *     ->setConsumerGroup('my-group')
 *     ->subscribe('TopicA')
 *     ->setMessageListener(fn(MessageView $mv) => ConsumeResult::SUCCESS)
 *     ->buildAsync();
 * ```
 *
 * @see PushConsumer
 * @see ClientConfiguration
 * @see ConsumeResult
 */
class PushConsumerBuilder
{
    private string $endpoints = '';
    private string $consumerGroup = '';
    private ?SessionCredentials $credentials = null;
    private array $subscriptionExpressions = [];
    /** @var callable|null */
    private $messageListener = null;
    private int $maxCacheMessageCount = 4096;
    private int $maxCacheMessageSizeInBytes = 67108864;
    private int $awaitDuration = 30;
    private int $consumptionThreadCount = 1; // no-op in PHP, stored for API parity
    private bool $enableFifoConsumeAccelerator = false;
    private bool $enableMessageInterceptorFiltering = false;
    private string $namespace = '';
    private bool $fifo = false;
    private ?TlsCredentials $tlsCredentials = null;

    /**
     * Bulk-import settings from a {@see ClientConfiguration} instance.
     *
     * Copies endpoints, credentials, namespace, and tlsCredentials from the
     * config object. Individual setter calls made **after** this method will
     * override the imported values.
     *
     * @param ClientConfiguration $config Pre-built client configuration
     * @return $this For method chaining
     */
    public function setClientConfiguration(ClientConfiguration $config): self
    {
        $this->endpoints = $config->getEndpoints();
        $this->credentials = $config->getSessionCredentialsProvider();
        $this->namespace = $config->getNamespace();
        if ($config->getTlsCredentials() !== null) {
            $this->tlsCredentials = $config->getTlsCredentials();
        }
        return $this;
    }

    /**
     * Enable or disable strict FIFO consumption ordering.
     *
     * When FIFO mode is enabled, messages within the same messageGroup are
     * delivered to the listener in order, and the next message in a group is
     * not dispatched until the current one is acknowledged. This is essential
     * for ordered message processing (e.g. trade order state machine).
     *
     * Typically paired with setEnableFifoConsumeAccelerator(true) for parallel
     * processing across different messageGroups.
     *
     * @param bool $fifo true to enable FIFO ordering, false for best-effort
     * @return $this For method chaining
     * @default false
     */
    public function setFifo(bool $fifo): self
    {
        $this->fifo = $fifo;
        return $this;
    }
    /**
     * Set the consumer group name.
     *
     * The consumer group identifies this consumer on the server side. All
     * consumers sharing the same group name will load-balance messages;
     * consumers in different groups each receive a full copy of every message
     * (broadcast semantics). This is a required setting.
     *
     * @param string $consumerGroup Consumer group name (must match server-side group config)
     * @return $this For method chaining
     * @default '' (empty — buildWithoutStart() will throw)
     */
    public function setConsumerGroup(string $consumerGroup): self
    {
        $this->consumerGroup = $consumerGroup;
        return $this;
    }

    /**
     * Bulk-set subscription expressions (topic => filter expression map).
     *
     * Replaces any previously registered subscriptions. Filter expressions
     * follow the RocketMQ SQL92 or tag syntax:
     *   - '*': match all messages
     *   - 'tagA': match messages with tag "tagA"
     *   - 'tagA || tagB': match messages with tag "tagA" or "tagB"
     *   - SQL92: 'color = "red" AND price > 100'
     *
     * At least one subscription is required; buildWithoutStart() throws if empty.
     *
     * @param array $expressions Associative array ['topicName' => 'filterExpression']
     * @return $this For method chaining
     * @default [] (no subscriptions)
     * @see subscribe() for adding subscriptions one by one
     */
    public function setSubscriptionExpressions(array $expressions): self
    {
        $this->subscriptionExpressions = $expressions;
        return $this;
    }

    /**
     * Register a subscription for a single topic.
     *
     * Convenience method that adds to (not replaces) the subscription map.
     * Calling subscribe('TopicA', '*') then subscribe('TopicB', 'tagX') is
     * equivalent to setSubscriptionExpressions(['TopicA' => '*', 'TopicB' => 'tagX']).
     * Subscribing to the same topic twice overwrites the previous expression.
     *
     * @param string $topic       Topic name to subscribe to
     * @param string $expression  Filter expression (tag or SQL92)
     * @return $this For method chaining
     * @default expression is '*' (match all)
     */
    public function subscribe(string $topic, string $expression = '*'): self
    {
        $this->subscriptionExpressions[$topic] = $expression;
        return $this;
    }

    /**
     * Set the message listener callback invoked for each received message.
     *
     * The listener receives a {@see MessageView} and must return a ConsumeResult:
     *   - ConsumeResult::SUCCESS — message processed, will be acknowledged
     *   - ConsumeResult::FAILURE — processing failed, message will be retried
     *     according to the server-side retry policy
     *
     * The listener should be fast and non-blocking; heavy work should be
     * dispatched to a separate worker pool. This is a required setting.
     *
     * @param callable $listener Signature: function(MessageView $mv): int
     * @return $this For method chaining
     * @default null (buildWithoutStart() will throw)
     */
    public function setMessageListener(callable $listener): self
    {
        $this->messageListener = $listener;
        return $this;
    }

    /**
     * Set max number of messages buffered in memory awaiting listener dispatch.
     *
     * Controls the prefetch buffer size. A larger value increases throughput
     * at the cost of higher memory usage and potential message redelivery on
     * consumer crash. When the buffer is full, long-poll ReceiveMessage calls
     * are paused until messages are consumed.
     *
     * @param int $count Max cached message count
     * @return $this For method chaining
     * @default 4096
     * @valid-range Must be > 0
     * @throws \InvalidArgumentException if $count <= 0
     */
    public function setMaxCacheMessageCount(int $count): self
    {
        if ($count <= 0) {
            throw new \InvalidArgumentException("maxCacheMessageCount must be > 0");
        }
        $this->maxCacheMessageCount = $count;
        return $this;
    }

    /**
     * Set max total size of messages buffered in memory (in bytes).
     *
     * Secondary flow-control limit alongside maxCacheMessageCount. When the
     * cumulative body size of cached messages exceeds this threshold, the
     * consumer pauses prefetching until messages are consumed.
     *
     * @param int $bytes Max cached message size in bytes
     * @return $this For method chaining
     * @default 67108864 (64 MB)
     * @valid-range Must be > 0
     * @throws \InvalidArgumentException if $bytes <= 0
     */
    public function setMaxCacheMessageSizeInBytes(int $bytes): self
    {
        if ($bytes <= 0) {
            throw new \InvalidArgumentException("maxCacheMessageSizeInBytes must be > 0");
        }
        $this->maxCacheMessageSizeInBytes = $bytes;
        return $this;
    }

    /**
     * Set consumption thread count (stored for API parity, no-op in PHP).
     *
     * In the Java/Go SDK this controls the thread pool size for concurrent
     * message dispatch. PHP uses an event-loop model (Swoole coroutines or
     * synchronous dispatch), so this value is stored but does not create
     * OS threads. It is kept for cross-language configuration compatibility.
     *
     * @param int $count Thread count hint
     * @return $this For method chaining
     * @default 1
     */
    public function setConsumptionThreadCount(int $count): self
    {
        $this->consumptionThreadCount = $count;
        return $this;
    }

    /**
     * Enable FIFO consume accelerator for parallel processing by messageGroup.
     *
     * When enabled, messages from different messageGroups are dispatched to
     * the listener concurrently (each group still processes in order). This
     * significantly improves throughput for FIFO topics with many distinct
     * message groups. Only effective when setFifo(true) is also set.
     *
     * @param bool $enable true to enable parallel group processing
     * @return $this For method chaining
     * @default false
     */
    public function setEnableFifoConsumeAccelerator(bool $enable): self
    {
        $this->enableFifoConsumeAccelerator = $enable;
        return $this;
    }

    /**
     * Enable client-side message interceptor filtering.
     *
     * When enabled, registered MessageInterceptors are applied during the
     * receive path before the listener is invoked. Interceptors can inspect,
     * transform, or filter messages (e.g. drop messages that don't match
     * custom business rules). When disabled, interceptors only run on the
     * send/ack path.
     *
     * @param bool $enable true to apply interceptors during receive
     * @return $this For method chaining
     * @default false
     */
    public function setEnableMessageInterceptorFiltering(bool $enable): self
    {
        $this->enableMessageInterceptorFiltering = $enable;
        return $this;
    }

    /**
     * Set the resource namespace prefix.
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
     * Build the PushConsumer without starting it.
     *
     * Validates all required fields and constructs a PushConsumer instance.
     * The returned consumer is fully configured but NOT running — call
     * start(), startAsync(), or startWithTimeout() separately.
     *
     * This is useful when you need to register additional interceptors or
     * perform setup before message delivery begins.
     *
     * Validation rules (all throw \RuntimeException):
     *   - endpoints must be set (non-empty)
     *   - consumerGroup must be set (non-empty)
     *   - messageListener must be set (non-null callable)
     *   - at least one subscription must be registered
     *
     * @return PushConsumer A configured but unstarted PushConsumer
     * @throws \RuntimeException If any required field is missing
     */
    public function buildWithoutStart(): PushConsumer
    {
        if ($this->endpoints === '') {
            throw new \RuntimeException("PushConsumer endpoints must be set");
        }
        if ($this->consumerGroup === '') {
            throw new \RuntimeException("PushConsumer consumerGroup must be set");
        }
        if ($this->messageListener === null) {
            throw new \RuntimeException("PushConsumer messageListener must be set");
        }
        if (empty($this->subscriptionExpressions)) {
            throw new \RuntimeException("PushConsumer must have at least one subscription");
        }

        return new PushConsumer($this->endpoints, $this->consumerGroup, [
            'subscriptionExpressions' => $this->subscriptionExpressions,
            'messageListener' => $this->messageListener,
            'maxCacheMessageCount' => $this->maxCacheMessageCount,
            'maxCacheMessageSizeInBytes' => $this->maxCacheMessageSizeInBytes,
            'awaitDuration' => $this->awaitDuration,
            'fifo' => $this->fifo,
            'enableFifoConsumeAccelerator' => $this->enableFifoConsumeAccelerator,
            'namespace' => $this->namespace,
            'credentials' => $this->credentials,
            'tlsCredentials' => $this->tlsCredentials,
        ]);
    }

    /**
     * Build and start the PushConsumer synchronously (blocking).
     *
     * Equivalent to:
     *   $consumer = $builder->buildWithoutStart();
     *   $consumer->start();  // blocks until TelemetrySession is established
     *   return $consumer;
     *
     * The returned consumer is actively receiving and dispatching messages.
     *
     * @return PushConsumer A started, message-receiving PushConsumer
     * @throws \RuntimeException If any required field is missing
     * @throws \RuntimeException If start() fails (e.g. gRPC connection refused,
     *         TelemetrySession cannot be established)
     */
    public function build(): PushConsumer
    {
        $consumer = $this->buildWithoutStart();
        $consumer->start();
        return $consumer;
    }

    /**
     * Build, start, run for a fixed duration, then shutdown — all in one call.
     *
     * Convenient for short-lived consumers, CLI tools, and integration tests.
     * Blocks the current thread for the specified duration, then gracefully
     * shuts down the consumer. Messages in-flight at shutdown time are
     * negatively acknowledged and redelivered to other consumers.
     *
     * @param int $seconds Duration in seconds to consume messages
     * @return void
     * @throws \RuntimeException If any required field is missing
     * @throws \RuntimeException If startWithTimeout() fails
     */
    public function startFor(int $seconds):  void
    {
        $consumer = $this->buildWithoutStart();
        $consumer->startWithTimeout($seconds);
        $consumer->shutdown();
    }

    /**
     * Build and start the PushConsumer asynchronously (non-blocking).
     *
     * Starts the consumer in a Swoole coroutine (when available) or returns
     * immediately. The optional $onDone callback is invoked once the startup
     * sequence completes. Messages begin arriving as soon as the TelemetrySession
     * is established.
     *
     * Use this when you need to start multiple consumers or run other logic
     * concurrently in the same process.
     *
     * @param callable|null $onDone Optional callback invoked after start completes.
     *                              Signature: function(): void
     * @return PushConsumer The consumer (may not be fully started yet)
     * @throws \RuntimeException If any required field is missing
     */
    public function buildAsync(?callable $onDone = null): PushConsumer
    {
        $consumer = $this->buildWithoutStart();
        $consumer->startAsync($onDone);
        return $consumer;
    }
}
