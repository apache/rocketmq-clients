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
 * SimpleConsumerBuilder - Fluent builder for SimpleConsumerOptimized.
 * Referencing Java SimpleConsumerBuilder.java
 */
class SimpleConsumerBuilder
{
    private $endpoints = '';
    private $consumerGroup = '';
    private $credentials = null;
    private $subscriptionExpressions = [];
    private $awaitDuration = 30;
    private $namespace = '';

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
     * Build and start the SimpleConsumerOptimized.
     *
     * @return SimpleConsumerOptimized
     * @throws \RuntimeException if required fields not set
     */
    public function build(): SimpleConsumerOptimized
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

        $consumer = new SimpleConsumerOptimized($this->endpoints, $this->consumerGroup, [
            'subscriptionExpressions' => $this->subscriptionExpressions,
            'awaitDuration' => $this->awaitDuration,
            'namespace' => $this->namespace,
            'credentials' => $this->credentials,
        ]);

        $consumer->start();
        return $consumer;
    }
}
