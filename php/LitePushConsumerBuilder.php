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
 * LitePushConsumerBuilder — Fluent builder for constructing and starting a {@see LitePushConsumer}.
 *
 * LitePushConsumer is a lightweight push-based consumer designed for the Lite
 * messaging model. It binds to a single parent topic and subscribes to one or
 * more lite (child) topics within it. Messages are delivered via a registered
 * listener callback, with optional per-lite-topic filtering.
 *
 * Required settings:
 *   - endpoints        (via setEndpoints() or setClientConfiguration())
 *   - consumerGroup    (via setConsumerGroup())
 *   - parentTopic      (via bindTopic())
 *   - messageListener  (via setMessageListener())
 *   - liteTopics       (via subscriptionLite())
 *
 * Optional settings with defaults:
 *   - maxCacheMessageCount: 4096        Max number of messages buffered in memory
 *   - maxCacheMessageSizeInBytes: 67108864  Max total size of buffered messages (64 MB)
 *   - consumptionThreadCount: 1        Thread count (no-op in PHP, stored for API parity)
 *   - enableFifoConsumeAccelerator: true   Parallel processing by messageGroup (default on)
 *   - namespace: ''                     Resource namespace prefix
 *   - credentials: null                 AK/SK SessionCredentials for authentication
 *   - tlsCredentials: null              TlsCredentials for custom TLS configuration
 *
 * Usage example — basic lite push consumer:
 * ```php
 * $consumer = (new LitePushConsumerBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setConsumerGroup('lite-group')
 *     ->bindTopic('ParentTopic')
 *     ->subscriptionLite('lite-topic-a')
 *     ->subscriptionLite('lite-topic-b')
 *     ->setMessageListener(function (MessageView $mv): int {
 *         echo $mv->getBody();
 *         return ConsumeResult::SUCCESS;
 *     })
 *     ->build();
 * ```
 *
 * Usage example — bounded run with startFor():
 * ```php
 * (new LitePushConsumerBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setConsumerGroup('lite-group')
 *     ->bindTopic('ParentTopic')
 *     ->subscriptionLite('lite-topic-a')
 *     ->setMessageListener(fn(MessageView $mv) => ConsumeResult::SUCCESS)
 *     ->startFor(120); // run for 120 seconds, then shutdown
 * ```
 *
 * Usage example — with ClientConfiguration and async start:
 * ```php
 * $config = (new ClientConfigurationBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setSessionCredentialsProvider(new SessionCredentials('ak', 'sk'))
 *     ->build();
 *
 * $consumer = (new LitePushConsumerBuilder())
 *     ->setClientConfiguration($config)
 *     ->setConsumerGroup('lite-group')
 *     ->bindTopic('ParentTopic')
 *     ->subscriptionLite('lite-topic-a')
 *     ->setMessageListener(fn(MessageView $mv) => ConsumeResult::SUCCESS)
 *     ->buildAsync();
 * ```
 *
 * @see LitePushConsumer
 * @see ClientConfiguration
 * @see ConsumeResult
 */
class LitePushConsumerBuilder
{
    private string $endpoints = '';
    private string $consumerGroup = '';
    private string $parentTopic = '';
    private ?SessionCredentials $credentials = null;
    /** @var callable|null */
    private $messageListener = null;
    private int $maxCacheMessageCount = 4096;
    private int $maxCacheMessageSizeInBytes = 67108864;
    private int $consumptionThreadCount = 1;
    private bool $enableFifoConsumeAccelerator = true;
    private string $namespace = '';
    private array $liteTopics = [];
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
     * Subscribe to a lite (child) topic within the parent topic.
     *
     * Lite topics are lightweight sub-topics within a parent topic. Messages
     * sent to a lite topic are stored in the parent topic's queue but tagged
     * with the lite topic name, enabling fine-grained subscription filtering.
     * Multiple lite topics can be subscribed; each call adds to the list.
     *
     * At least one lite topic must be subscribed; buildWithoutStart() throws
     * if the list is empty.
     *
     * @param string $liteTopic Lite topic name to subscribe to
     * @return $this For method chaining
     * @default [] (no lite topics)
     * @see bindTopic() for setting the parent topic
     */
    public function subscriptionLite(string $liteTopic): self
    {
        $this->liteTopics[$liteTopic] = null;
        return $this;
    }

    /**
     * Set the consumer group name.
     *
     * The consumer group identifies this consumer on the server side. All
     * consumers sharing the same group name will load-balance messages;
     * consumers in different groups each receive a full copy of every message.
     * This is a required setting.
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
     * Bind a single parent topic for lite messaging.
     *
     * The parent topic is the physical storage topic on the broker. All lite
     * (child) topics subscribed via subscriptionLite() are logically partitioned
     * within this parent topic. Only one parent topic can be bound; calling
     * bindTopic() again overwrites the previous value.
     *
     * This is a required setting — buildWithoutStart() throws if not set.
     *
     * @param string $parentTopic Parent topic name on the broker
     * @return $this For method chaining
     * @default '' (empty — buildWithoutStart() will throw)
     */
    public function bindTopic(string $parentTopic): self
    {
        $this->parentTopic = $parentTopic;
        return $this;
    }

    /**
     * Set the message listener callback invoked for each received message.
     *
     * The listener receives a {@see MessageView} and must return a ConsumeResult:
     *   - ConsumeResult::SUCCESS — message processed, will be acknowledged
     *   - ConsumeResult::FAILURE — processing failed, message will be retried
     *
     * This listener serves as the default handler for all lite topics.
     * Per-lite-topic listeners can be registered via subscribeLite() on the
     * consumer after buildWithoutStart(). This is a required setting.
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
     * consumer crash.
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
     * message dispatch. PHP uses an event-loop model, so this value is stored
     * but does not create OS threads.
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
     * When enabled (default for LitePushConsumer), messages from different
     * messageGroups are dispatched concurrently while maintaining order within
     * each group. This is the default for LitePushConsumer because lite topics
     * typically involve many concurrent message groups.
     *
     * @param bool $enable true to enable parallel group processing
     * @return $this For method chaining
     * @default true (enabled by default for LitePushConsumer)
     */
    public function setEnableFifoConsumeAccelerator(bool $enable): self
    {
        $this->enableFifoConsumeAccelerator = $enable;
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
     * Build the LitePushConsumer without starting it.
     *
     * Validates all required fields and constructs a LitePushConsumer instance.
     * All lite topics registered via subscriptionLite() are bound to the consumer.
     * The returned consumer is NOT running — call start(), startAsync(), or
     * startWithTimeout() separately.
     *
     * Validation rules (all throw \RuntimeException):
     *   - endpoints must be set (non-empty)
     *   - consumerGroup must be set (non-empty)
     *   - messageListener must be set (non-null callable)
     *   - parentTopic must be bound via bindTopic()
     *   - at least one lite topic must be subscribed via subscriptionLite()
     *
     * @return LitePushConsumer A configured but unstarted LitePushConsumer
     * @throws \RuntimeException If any required field is missing
     */
    public function buildWithoutStart(): LitePushConsumer
    {
        if ($this->endpoints === '') {
            throw new \RuntimeException("LitePushConsumer endpoints must be set");
        }
        if ($this->consumerGroup === '') {
            throw new \RuntimeException("LitePushConsumer consumerGroup must be set");
        }
        if ($this->messageListener === null) {
            throw new \RuntimeException("LitePushConsumer messageListener must be set");
        }
        if ($this->parentTopic === '') {
            throw new \RuntimeException("LitePushConsumer parent topic must be set");
        }
        if (empty($this->liteTopics)) {
            throw new \RuntimeException("LitePushConsumer must have at least one lite topic");
        }

        $consumer = new LitePushConsumer($this->endpoints, $this->consumerGroup, $this->parentTopic, [
            'messageListener' => $this->messageListener,
            'maxCacheMessageCount' => $this->maxCacheMessageCount,
            'maxCacheMessageSizeInBytes' => $this->maxCacheMessageSizeInBytes,
            'enableFifoConsumeAccelerator' => $this->enableFifoConsumeAccelerator,
            'namespace' => $this->namespace,
            'credentials' => $this->credentials,
            'tlsCredentials' => $this->tlsCredentials,
        ]);

        foreach ($this->liteTopics as $liteTopic =>$listener) {
            if ($listener !== null) {
                $consumer->subscribeLite($liteTopic, $listener);
            } else {
                $consumer->subscribeLite($liteTopic);
            }
        }
        return $consumer;
    }

    /**
     * Build and start the LitePushConsumer synchronously (blocking).
     *
     * Equivalent to:
     *   $consumer = $builder->buildWithoutStart();
     *   $consumer->start();  // blocks until TelemetrySession is established
     *   return $consumer;
     *
     * The returned consumer is actively receiving and dispatching lite messages.
     *
     * @return LitePushConsumer A started, message-receiving LitePushConsumer
     * @throws \RuntimeException If any required field is missing
     * @throws \RuntimeException If start() fails (e.g. gRPC connection refused)
     */
    public function build(): LitePushConsumer
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
     * shuts down the consumer.
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
     * Build and start the LitePushConsumer asynchronously (non-blocking).
     *
     * Starts the consumer in a Swoole coroutine (when available) or returns
     * immediately. The optional $onDone callback is invoked once the startup
     * sequence completes.
     *
     * @param callable|null $onDone Optional callback invoked after start completes.
     *                              Signature: function(): void
     * @return LitePushConsumer The consumer (may not be fully started yet)
     * @throws \RuntimeException If any required field is missing
     */
    public function buildAsync(?callable $onDone = null): LitePushConsumer
    {
        $consumer = $this->buildWithoutStart();
        $consumer->startAsync($onDone);
        return $consumer;
    }
}
