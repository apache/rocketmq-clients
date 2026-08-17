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
 * SimpleConsumerBuilder — Fluent builder for constructing and starting a {@see SimpleConsumer}.
 *
 * SimpleConsumer implements a pull-based consumption model where the application
 * explicitly calls receive() to fetch messages, then acknowledges or negatively
 * acknowledges them. This gives the application full control over message
 * processing order and concurrency, at the cost of managing the polling loop.
 *
 * Required settings:
 *   - endpoints        (via setEndpoints() or setClientConfiguration())
 *   - consumerGroup    (via setConsumerGroup())
 *   - subscriptions    (via setSubscriptionExpressions() or subscribe())
 *
 * Optional settings with defaults:
 *   - subscriptionExpressions: []       ['topic' => 'filterExpression'] map
 *   - awaitDuration: 30                Long-poll wait time in seconds for receive()
 *   - namespace: ''                     Resource namespace prefix
 *   - credentials: null                 AK/SK SessionCredentials for authentication
 *   - tlsCredentials: null              TlsCredentials for custom TLS configuration
 *
 * Usage example — basic simple consumer:
 * ```php
 * $consumer = (new SimpleConsumerBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setConsumerGroup('my-group')
 *     ->subscribe('TopicA', '*')
 *     ->setAwaitDuration(15)
 *     ->build();
 *
 * while (true) {
 *     $messages = $consumer->receive(16, 15);
 *     foreach ($messages as $mv) {
 *         // process message
 *         $consumer->ack($mv);
 *     }
 * }
 * ```
 *
 * Usage example — with ClientConfiguration:
 * ```php
 * $config = (new ClientConfigurationBuilder())
 *     ->setEndpoints('127.0.0.1:8081')
 *     ->setSessionCredentialsProvider(new SessionCredentials('ak', 'sk'))
 *     ->build();
 *
 * $consumer = (new SimpleConsumerBuilder())
 *     ->setClientConfiguration($config)
 *     ->setConsumerGroup('my-group')
 *     ->subscribe('TopicA')
 *     ->subscribe('TopicB', 'tagX || tagY')
 *     ->build();
 * ```
 *
 * @see SimpleConsumer
 * @see ClientConfiguration
 */
class SimpleConsumerBuilder
{
    private string $endpoints = '';
    private string $consumerGroup = '';
    private ?SessionCredentials $credentials = null;
    private array $subscriptionExpressions = [];
    private int $awaitDuration = 30;
    private string $namespace = '';
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
     * Set the consumer group name.
     *
     * The consumer group identifies this consumer on the server side. All
     * consumers sharing the same group name will load-balance messages;
     * consumers in different groups each receive a full copy of every message.
     * This is a required setting.
     *
     * @param string $consumerGroup Consumer group name (must match server-side group config)
     * @return $this For method chaining
     * @default '' (empty — build() will throw)
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
     * At least one subscription is required; build() throws if empty.
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
     * Subscribe to a single topic.
     *
     * Convenience method that adds to (not replaces) the subscription map.
     * Calling subscribe('TopicA', '*') then subscribe('TopicB', 'tagX') is
     * equivalent to setSubscriptionExpressions(['TopicA' => '*', 'TopicB' => 'tagX']).
     * Subscribing to the same topic twice overwrites the previous expression.
     *
     * @param string $topic       Topic name to subscribe to
     * @param string $expression  Filter expression (tag or SQL92 syntax)
     * @return $this For method chaining
     * @default expression is '*' (match all)
     */
    public function subscribe(string $topic, string $expression = '*'): self
    {
        $this->subscriptionExpressions[$topic] = $expression;
        return $this;
    }

    /**
     * Set long-polling await duration in seconds.
     *
     * Controls how long the receive() call blocks waiting for new messages.
     * The consumer sends a ReceiveMessageRequest to the broker with this
     * timeout; if no messages are available within this window, the broker
     * returns an empty response and the consumer retries.
     *
     * Longer durations reduce network round-trips and CPU usage at the cost
     * of slightly higher latency for detecting new messages. Recommended
     * range: 10–30 seconds. Values below 5 seconds cause excessive polling.
     *
     * This value is also used as the per-request timeout for each
     * long-poll ReceiveMessage gRPC call.
     *
     * @param int $seconds Await duration in seconds
     * @return $this For method chaining
     * @default 30
     * @valid-range Must be > 0
     * @throws \InvalidArgumentException if $seconds <= 0
     */
    public function setAwaitDuration(int $seconds): self
    {
        if ($seconds <= 0) {
            throw new \InvalidArgumentException("awaitDuration must be > 0");
        }
        $this->awaitDuration = $seconds;
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
     * Build and start the SimpleConsumer.
     *
     * Behavior:
     *   1. Validates all required fields (endpoints, consumerGroup, subscriptions).
     *   2. Constructs a SimpleConsumer with all accumulated settings.
     *   3. Calls SimpleConsumer::start() which establishes the TelemetrySession,
     *      registers settings callbacks, and starts the HeartbeatManager.
     *   4. Returns a started consumer ready for receive() calls.
     *
     * Validation rules (all throw \RuntimeException):
     *   - endpoints must be set (non-empty)
     *   - consumerGroup must be set (non-empty)
     *   - at least one subscription must be registered via setSubscriptionExpressions()
     *     or subscribe()
     *
     * @return SimpleConsumer A started SimpleConsumer ready for receive()
     * @throws \RuntimeException If endpoints is not set
     * @throws \RuntimeException If consumerGroup is not set
     * @throws \RuntimeException If no subscriptions are registered
     * @throws \RuntimeException If start() fails (e.g. gRPC connection refused)
     * @throws \InvalidArgumentException If awaitDuration was set to an invalid value
     */
    public function build(): SimpleConsumer
    {
        if ($this->endpoints === '') {
            throw new \RuntimeException("SimpleConsumer endpoints must be set");
        }
        if ($this->consumerGroup === '') {
            throw new \RuntimeException("SimpleConsumer consumerGroup must be set");
        }
        if (empty($this->subscriptionExpressions)) {
            throw new \RuntimeException("SimpleConsumer must have at least one subscription");
        }

        $consumer = new SimpleConsumer($this->endpoints, $this->consumerGroup, [
            'subscriptionExpressions' => $this->subscriptionExpressions,
            'awaitDuration' => $this->awaitDuration,
            'namespace' => $this->namespace,
            'credentials' => $this->credentials,
            'tlsCredentials' => $this->tlsCredentials,
        ]);

        $consumer->start();
        return $consumer;
    }
}
