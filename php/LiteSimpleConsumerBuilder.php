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
 * LiteSimpleConsumerBuilder — Fluent builder for constructing and starting a {@see LiteSimpleConsumer}.
 *
 * LiteSimpleConsumer is the pull-based (explicit receive/ack) consumer for the Lite
 * messaging model. It binds to a single parent topic and subscribes to one or more
 * lite (child) topics within it. Messages are fetched explicitly via receive() and
 * acknowledged via ack() / changeInvisibleDuration() — there is no message listener.
 *
 * Required settings:
 *   - endpoints        (via setEndpoints() or setClientConfiguration())
 *   - consumerGroup    (via setConsumerGroup())
 *   - parentTopic      (via bindTopic())
 *   - liteTopics       (via subscriptionLite())
 *
 * Optional settings with defaults:
 *   - namespace: ''                     Resource namespace prefix
 *   - requestTimeout: 3000              gRPC request timeout in ms
 *   - awaitDuration: 30                 Long-polling timeout in seconds
 *   - clientId: ''                      Custom client identifier (auto-generated if empty)
 *   - credentials: null                 AK/SK SessionCredentials for authentication
 *   - tlsCredentials: null              TlsCredentials for custom TLS configuration
 *   - sslEnabled: true                  Enable SSL for the gRPC channel
 *
 * Usage example — basic lite simple consumer:
 * ```php
 * $consumer = (new LiteSimpleConsumerBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setConsumerGroup('lite-group')
 *     ->bindTopic('ParentTopic')
 *     ->subscriptionLite('lite-topic-a')
 *     ->subscriptionLite('lite-topic-b')
 *     ->build();
 *
 * $messages = $consumer->receive(32, 30);
 * foreach ($messages as $message) {
 *     echo $message->getBody();
 * }
 * $consumer->ack($messages);
 * ```
 *
 * @see LiteSimpleConsumer
 * @see ClientConfiguration
 */
class LiteSimpleConsumerBuilder
{
    private string $endpoints = '';
    private string $consumerGroup = '';
    private string $parentTopic = '';
    private ?SessionCredentials $credentials = null;
    private string $namespace = '';
    private array $liteTopics = [];
    private ?TlsCredentials $tlsCredentials = null;
    private int $requestTimeout = 3000;
    private int $awaitDuration = 30;
    private string $clientId = '';
    private bool $sslEnabled = true;

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
     * Set the gRPC server endpoint (host:port).
     *
     * @param string $endpoints Endpoint address, e.g. '127.0.0.1:8081'
     * @return $this For method chaining
     * @default '' (empty — buildWithoutStart() will throw)
     */
    public function setEndpoints(string $endpoints): self
    {
        $this->endpoints = $endpoints;
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
     * Set the gRPC request timeout in milliseconds.
     *
     * @param int $requestTimeout Request timeout (ms)
     * @return $this For method chaining
     * @default 3000
     */
    public function setRequestTimeout(int $requestTimeout): self
    {
        $this->requestTimeout = $requestTimeout;
        return $this;
    }

    /**
     * Set the long-polling await duration in seconds.
     *
     * @param int $awaitDuration Long-polling timeout (seconds)
     * @return $this For method chaining
     * @default 30
     */
    public function setAwaitDuration(int $awaitDuration): self
    {
        $this->awaitDuration = $awaitDuration;
        return $this;
    }

    /**
     * Set a custom client identifier.
     *
     * @param string $clientId Client identifier (auto-generated if empty)
     * @return $this For method chaining
     * @default '' (auto-generated)
     */
    public function setClientId(string $clientId): self
    {
        $this->clientId = $clientId;
        return $this;
    }

    /**
     * Enable or disable SSL for the gRPC channel.
     *
     * @param bool $sslEnabled true to enable SSL (default)
     * @return $this For method chaining
     * @default true
     */
    public function setSslEnabled(bool $sslEnabled): self
    {
        $this->sslEnabled = $sslEnabled;
        return $this;
    }

    /**
     * Build the LiteSimpleConsumer without starting it.
     *
     * Validates all required fields and constructs a LiteSimpleConsumer instance.
     * All lite topics registered via subscriptionLite() are bound to the consumer.
     * The returned consumer is NOT running — call start() separately.
     *
     * Validation rules (all throw \RuntimeException):
     *   - endpoints must be set (non-empty)
     *   - consumerGroup must be set (non-empty)
     *   - parentTopic must be bound via bindTopic()
     *   - at least one lite topic must be subscribed via subscriptionLite()
     *
     * @return LiteSimpleConsumer A configured but unstarted LiteSimpleConsumer
     * @throws \RuntimeException If any required field is missing
     */
    public function buildWithoutStart(): LiteSimpleConsumer
    {
        if ($this->endpoints === '') {
            throw new \RuntimeException("LiteSimpleConsumer endpoints must be set");
        }
        if ($this->consumerGroup === '') {
            throw new \RuntimeException("LiteSimpleConsumer consumerGroup must be set");
        }
        if ($this->parentTopic === '') {
            throw new \RuntimeException("LiteSimpleConsumer parent topic must be set");
        }
        if (empty($this->liteTopics)) {
            throw new \RuntimeException("LiteSimpleConsumer must have at least one lite topic");
        }

        $consumer = new LiteSimpleConsumer($this->endpoints, $this->consumerGroup, $this->parentTopic, [
            'namespace' => $this->namespace,
            'requestTimeout' => $this->requestTimeout,
            'awaitDuration' => $this->awaitDuration,
            'clientId' => $this->clientId,
            'credentials' => $this->credentials,
            'tlsCredentials' => $this->tlsCredentials,
            'sslEnabled' => $this->sslEnabled,
        ]);

        foreach ($this->liteTopics as $liteTopic => $listener) {
            $consumer->subscribeLite($liteTopic);
        }
        return $consumer;
    }

    /**
     * Build and start the LiteSimpleConsumer synchronously (blocking).
     *
     * Equivalent to:
     *   $consumer = $builder->buildWithoutStart();
     *   $consumer->start();
     *   return $consumer;
     *
     * The returned consumer is ready to receive() messages from its lite topics.
     *
     * @return LiteSimpleConsumer A started LiteSimpleConsumer
     * @throws \RuntimeException If any required field is missing
     * @throws \RuntimeException If start() fails (e.g. gRPC connection refused)
     */
    public function build(): LiteSimpleConsumer
    {
        $consumer = $this->buildWithoutStart();
        $consumer->start();
        return $consumer;
    }
}
