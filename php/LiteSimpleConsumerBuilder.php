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
 * LiteSimpleConsumerBuilder - Fluent builder for LiteSimpleConsumer.
 * Referencing Java LiteSimpleConsumerBuilder.java
 */
class LiteSimpleConsumerBuilder
{
    private $endpoints = '';
    private $consumerGroup = '';
    private $parentTopic = '';
    private $credentials = null;
    private $awaitDuration = 30;
    private $namespace = '';
    private $liteTopics = [];

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
        return $this;
    }

    public function subscribeLite(string $liteTopic): self
    {
        $this->liteTopics[$liteTopic] = true;
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
     * Set long-polling await duration in seconds.
     *
     * @return $this
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
     * @return $this
     */
    public function setNamespace(string $namespace): self
    {
        $this->namespace = $namespace;
        return $this;
    }

    /**
     * Build and start the LiteSimpleConsumer.
     *
     * @return LiteSimpleConsumer
     * @throws \RuntimeException if required fields not set
     */
    public function build(): LiteSimpleConsumer
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

        $consumer = new LiteSimpleConsumer($this->endpoints, $this->consumerGroup, $this->parentTopic, [
            'awaitDuration' => $this->awaitDuration,
            'namespace' => $this->namespace,
            'credentials' => $this->credentials,
        ]);

        foreach (array_keys($this->liteTopics) as $liteTopic) {
            $consumer->subscribeLite($liteTopic);
        }
        $consumer->start();
        return $consumer;
    }
}
