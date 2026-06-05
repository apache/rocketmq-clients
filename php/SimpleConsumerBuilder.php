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
 * SimpleConsumerBuilder - Fluent builder for SimpleConsumer.
 * Referencing Java SimpleConsumerBuilder.java
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
     * Set client configuration.
     *
     * @param ClientConfiguration $config Client configuration
     * @return $this
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
     * Set the consumer group.
     *
     * @param string $consumerGroup Consumer group name
     * @return $this
     */
    public function setConsumerGroup(string $consumerGroup): self
    {
        $this->consumerGroup = $consumerGroup;
        return $this;
    }

    /**
     * Set subscription expressions.
     *
     * @param array $expressions ['topic' => 'expression', ...]
     * @return $this
     */
    public function setSubscriptionExpressions(array $expressions): self
    {
        $this->subscriptionExpressions = $expressions;
        return $this;
    }

    /**
     * Subscribe to a topic.
     *
     * @param string $topic
     * @param string $expression Filter expression (default "*")
     * @return $this
     */
    public function subscribe(string $topic, string $expression = '*'): self
    {
        $this->subscriptionExpressions[$topic] = $expression;
        return $this;
    }

    /**
     * Set long-polling await duration in seconds.
     *
     * @param int $seconds Await duration in seconds
     * @return $this
     * @throws \InvalidArgumentException if seconds is not positive
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
     * Set namespace.
     *
     * @param string $namespace Namespace string
     * @return $this
     */
    public function setNamespace(string $namespace): self
    {
        $this->namespace = $namespace;
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
        return $this;
    }

    /**
     * Build and start the SimpleConsumer.
     *
     * @return SimpleConsumer
     * @throws \RuntimeException if required fields not set
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
