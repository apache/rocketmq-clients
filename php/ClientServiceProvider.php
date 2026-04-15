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

use Apache\Rocketmq\Builder\LiteProducerBuilder;
use Apache\Rocketmq\Builder\LitePushConsumerBuilder;
use Apache\Rocketmq\Builder\LiteSimpleConsumerBuilder;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Builder\ProducerBuilder;
use Apache\Rocketmq\Builder\PushConsumerBuilder;
use Apache\Rocketmq\Builder\SimpleConsumerBuilder;

/**
 * Service provider interface for RocketMQ client components.
 * 
 * This interface provides a unified entry point for creating various RocketMQ client components
 * using the Builder pattern. It follows the Service Provider Interface (SPI) design pattern,
 * similar to Java's ClientServiceProvider.
 * 
 * Usage example:
 * ```php
 * $provider = ClientServiceProvider::loadService();
 * 
 * // Create producer
 * $producer = $provider->newProducerBuilder()
 *     ->setClientConfiguration($config)
 *     ->setTopics('my-topic')
 *     ->build();
 * 
 * // Create push consumer
 * $consumer = $provider->newPushConsumerBuilder()
 *     ->setClientConfiguration($config)
 *     ->setConsumerGroup('my-group')
 *     ->setTopic('my-topic')
 *     ->setMessageListener($listener)
 *     ->build();
 * ```
 */
interface ClientServiceProvider
{
    /**
     * Get a new producer builder instance.
     *
     * @return ProducerBuilder The producer builder instance
     */
    public function newProducerBuilder(): ProducerBuilder;

    /**
     * Get a new message builder instance.
     *
     * @return MessageBuilder The message builder instance
     */
    public function newMessageBuilder(): MessageBuilder;

    /**
     * Get a new push consumer builder instance.
     *
     * @return PushConsumerBuilder The push consumer builder instance
     */
    public function newPushConsumerBuilder(): PushConsumerBuilder;

    /**
     * Get a new lite push consumer builder instance.
     *
     * @return LitePushConsumerBuilder The lite push consumer builder instance
     */
    public function newLitePushConsumerBuilder(): LitePushConsumerBuilder;

    /**
     * Get a new lite simple consumer builder instance.
     *
     * @return LiteSimpleConsumerBuilder The lite simple consumer builder instance
     */
    public function newLiteSimpleConsumerBuilder(): LiteSimpleConsumerBuilder;

    /**
     * Get a new simple consumer builder instance.
     *
     * @return SimpleConsumerBuilder The simple consumer builder instance
     */
    public function newSimpleConsumerBuilder(): SimpleConsumerBuilder;

    /**
     * Get a new lite producer builder instance.
     *
     * @return LiteProducerBuilder The lite producer builder instance
     */
    public function newLiteProducerBuilder(): LiteProducerBuilder;

    /**
     * Load the default service provider implementation.
     * 
     * This method uses lazy initialization with caching to avoid potential
     * concurrency issues. On the first call, it creates and caches the default
     * provider instance. Subsequent calls return the cached instance.
     *
     * @return ClientServiceProvider The service provider instance
     */
    public static function loadService(): ClientServiceProvider;
}

// Load the default implementation
require_once __DIR__ . '/DefaultClientServiceProvider.php';
