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
 * LitePushConsumerBuilder - Fluent builder for LitePushConsumer.
 * Referencing Java LitePushConsumerBuilder.java
 */
class LitePushConsumerBuilder
{
    private $endpoints = '';
    private $consumerGroup = '';
    private $parentTopic = '';
    private $credentials = null;
    private $messageListener = null;
    private $maxCacheMessageCount = 4096;
    private $maxCacheMessageSizeInBytes = 67108864;
    private $consumptionThreadCount = 1;
    private $enableFifoConsumeAccelerator = true;
    private $namespace = '';
    private $liteTopics = [];
    private $tlsCredentials = null;

    /**
     * Set client configuration.
     *
     * @param ClientConfiguration $config
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
     * Subscribe to a lite topic.
     *
     * @param string $liteTopic
     * @return $this
     */
    public function subscriptionLite(string $liteTopic): self
    {
        $this->liteTopics[$liteTopic] = null;
        return $this;
    }

    /**
     * Set the consumer group.
     *
     * @param string $consumerGroup
     * @return $this
     */
    public function setConsumerGroup(string $consumerGroup): self
    {
        $this->consumerGroup = $consumerGroup;
        return $this;
    }

    /**
     * Bind a single parent topic.
     *
     * @param string $parentTopic
     * @return $this
     */
    public function bindTopic(string $parentTopic): self
    {
        $this->parentTopic = $parentTopic;
        return $this;
    }

    /**
     * Set the message listener callback.
     *
     * @param callable $listener
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
     * @param int $count Must be > 0
     * @return $this
     * @throws \InvalidArgumentException if count <= 0
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
     * @param int $bytes Must be > 0
     * @return $this
     * @throws \InvalidArgumentException if bytes <= 0
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
     * Set consumption thread count (no-op in PHP).
     *
     * @param int $count
     * @return $this
     */
    public function setConsumptionThreadCount(int $count): self
    {
        $this->consumptionThreadCount = $count;
        return $this;
    }

    /**
     * Enable FIFO consume accelerator.
     *
     * @param bool $enable
     * @return $this
     */
    public function setEnableFifoConsumeAccelerator(bool $enable): self
    {
        $this->enableFifoConsumeAccelerator = $enable;
        return $this;
    }

    /**
     * Set namespace.
     *
     * @param string $namespace
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
     * Build and start the LitePushConsumer.
     *
     * @return LitePushConsumer
     * @throws \RuntimeException if required fields not set
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
     * Build and start the LitePushConsumer.
     *
     * @return LitePushConsumer
     * @throws \RuntimeException if required fields not set
     */
    public function build(): LitePushConsumer
    {
        $consumer = $this->buildWithoutStart();
        $consumer->start();
        return $consumer;
    }

    /**
     * Build, start, run for a given duration, then shutdown.
     *
     * @param int $seconds Duration in seconds to consume
     * @throws \RuntimeException if required fields not set
     */
    public function startFor(int $seconds):  void
    {
        $consumer = $this->buildWithoutStart();
        $consumer->startWithTimeout($seconds);
        $consumer->shutdown();
    }

    /**
     * Build and start the LitePushConsumer asynchronously.
     *
     * @param callable|null $onDone Optional callback invoked after start completes
     * @return LitePushConsumer
     * @throws \RuntimeException if required fields not set
     */
    public function buildAsync(?callable $onDone = null): LitePushConsumer
    {
        $consumer = $this->buildWithoutStart();
        $consumer->startAsync($onDone);
        return $consumer;
    }
}
