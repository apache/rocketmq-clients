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
 * PushConsumerBuilder - Fluent builder for PushConsumer.
 * Referencing Java PushConsumerBuilder.java
 */
class PushConsumerBuilder
{
    private $endpoints = '';
    private $consumerGroup = '';
    private $credentials = null;
    private $subscriptionExpressions = [];
    private $messageListener = null;
    private $maxCacheMessageCount = 4096;
    private $maxCacheMessageSizeInBytes = 67108864;
    private $awaitDuration = 30;
    private $consumptionThreadCount = 1; // no-op in PHP, stored for API parity
    private $enableFifoConsumeAccelerator = false;
    private $enableMessageInterceptorFiltering = false;
    private $namespace = '';
    private $fifo = false;
    private $tlsCredentials = null;

    /**
     * Set client configuration.
     *
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

    public function setFifo(bool $fifo): self
    {
        $this->fifo = $fifo;
        return $this;
    }
    /**
     * Set the consumer group.
     *
     * @return $this
     */
    public function setConsumerGroup(string $consumerGroup): self
    {
        $this->consumerGroup = $consumerGroup;
        return $this;
    }

    /**
     * Set subscription expressions (topic => filter expression).
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
     * Register a subscription for a topic.
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
     * Set the message listener callback.
     *
     * @param callable $listener function($messageView): int
     * @return $this
     */
    public function setMessageListener(callable $listener): self
    {
        $this->messageListener = $listener;
        return $this;
    }

    /**
     * Set max cached message count.
     *
     * @return $this
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
     * Set max cached message size in bytes.
     *
     * @return $this
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
     * Set consumption thread count (no-op in PHP, stored for API parity).
     *
     * @return $this
     */
    public function setConsumptionThreadCount(int $count): self
    {
        $this->consumptionThreadCount = $count;
        return $this;
    }

    /**
     * Enable FIFO consume accelerator (parallel processing by messageGroup).
     *
     * @return $this
     */
    public function setEnableFifoConsumeAccelerator(bool $enable): self
    {
        $this->enableFifoConsumeAccelerator = $enable;
        return $this;
    }

    /**
     * Enable client-side message interceptor filtering.
     *
     * @return $this
     */
    public function setEnableMessageInterceptorFiltering(bool $enable): self
    {
        $this->enableMessageInterceptorFiltering = $enable;
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
     * Build and start the PushConsumer.
     *
     * @return PushConsumer
     * @throws \RuntimeException if required fields not set
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

    public function build(): PushConsumer
    {
        $consumer = $this->buildWithoutStart();
        $consumer->start();
        return $consumer;
    }

    public function startFor(int $seconds):  void
    {
        $consumer = $this->buildWithoutStart();
        $consumer->startWithTimeout($seconds);
        $consumer->shutdown();
    }

    public function buildAsync(?callable $onDone = null): PushConsumer
    {
        $consumer = $this->buildWithoutStart();
        $consumer->startAsync($onDone);
        return $consumer;
    }
}
