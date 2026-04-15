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
 * Default implementation of ClientServiceProvider.
 * 
 * This class provides the default implementations for all builder factories.
 * It follows the singleton pattern to ensure only one provider instance exists.
 */
class DefaultClientServiceProvider implements ClientServiceProvider
{
    /**
     * @var DefaultClientServiceProvider|null Singleton instance
     */
    private static $instance = null;

    /**
     * Private constructor to prevent direct instantiation.
     */
    private function __construct()
    {
        // Private constructor for singleton pattern
    }

    /**
     * {@inheritdoc}
     */
    public function newProducerBuilder(): ProducerBuilder
    {
        return new ProducerBuilder();
    }

    /**
     * {@inheritdoc}
     */
    public function newMessageBuilder(): MessageBuilder
    {
        return new MessageBuilder();
    }

    /**
     * {@inheritdoc}
     */
    public function newPushConsumerBuilder(): PushConsumerBuilder
    {
        return new PushConsumerBuilder();
    }

    /**
     * {@inheritdoc}
     */
    public function newLitePushConsumerBuilder(): LitePushConsumerBuilder
    {
        return new LitePushConsumerBuilder();
    }

    /**
     * {@inheritdoc}
     */
    public function newLiteSimpleConsumerBuilder(): LiteSimpleConsumerBuilder
    {
        return new LiteSimpleConsumerBuilder();
    }

    /**
     * {@inheritdoc}
     */
    public function newSimpleConsumerBuilder(): SimpleConsumerBuilder
    {
        return new SimpleConsumerBuilder();
    }

    /**
     * {@inheritdoc}
     */
    public function newLiteProducerBuilder(): LiteProducerBuilder
    {
        return new LiteProducerBuilder();
    }

    /**
     * {@inheritdoc}
     */
    public static function loadService(): ClientServiceProvider
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        
        return self::$instance;
    }
}
