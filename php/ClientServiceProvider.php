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

use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Builder\ProducerBuilder;
use Apache\Rocketmq\Builder\PushConsumerBuilder;
use Apache\Rocketmq\Builder\SimpleConsumerBuilder;
use Apache\Rocketmq\Builder\LitePushConsumerBuilder;
use Apache\Rocketmq\Builder\LiteSimpleConsumerBuilder;

/**
 * Service provider to create client instances, following the same pattern as Java client.
 */
interface ClientServiceProvider {
    /**
     * Get the producer builder by the current provider.
     *
     * @return ProducerBuilder The producer builder instance
     */
    public function newProducerBuilder();
    
    /**
     * Get the message builder by the current provider.
     *
     * @return MessageBuilder The message builder instance
     */
    public function newMessageBuilder();
    
    /**
     * Get the push consumer builder by the current provider.
     *
     * @return PushConsumerBuilder The push consumer builder instance
     */
    public function newPushConsumerBuilder();
    
    /**
     * Get the lite push consumer builder by the current provider.
     *
     * @return LitePushConsumerBuilder The lite push consumer builder instance
     */
    public function newLitePushConsumerBuilder();
    
    /**
     * Get the lite simple consumer builder by the current provider.
     *
     * @return LiteSimpleConsumerBuilder The lite simple consumer builder instance
     */
    public function newLiteSimpleConsumerBuilder();
    
    /**
     * Get the simple consumer builder by the current provider.
     *
     * @return SimpleConsumerBuilder The simple consumer builder instance
     */
    public function newSimpleConsumerBuilder();
    
    /**
     * Load client service provider instance
     *
     * @return ClientServiceProvider
     */
    public static function loadService();
    
    /**
     * Do load client service provider instance
     *
     * @return ClientServiceProvider
     */
    public static function doLoad();
}

/**
 * Default implementation of ClientServiceProvider
 */
class ClientServiceProviderImpl implements ClientServiceProvider {
    /**
     * @var ClientServiceProvider|null Singleton instance
     */
    private static $instance = null;
    
    /**
     * Private constructor to prevent direct instantiation
     */
    private function __construct() {
    }
    
    /**
     * {@inheritdoc}
     */
    public function newProducerBuilder() {
        return new ProducerBuilder();
    }
    
    /**
     * {@inheritdoc}
     */
    public function newMessageBuilder() {
        return new MessageBuilder();
    }
    
    /**
     * {@inheritdoc}
     */
    public function newPushConsumerBuilder() {
        return new PushConsumerBuilder();
    }
    
    /**
     * {@inheritdoc}
     */
    public function newLitePushConsumerBuilder() {
        return new LitePushConsumerBuilder();
    }
    
    /**
     * {@inheritdoc}
     */
    public function newLiteSimpleConsumerBuilder() {
        return new LiteSimpleConsumerBuilder();
    }
    
    /**
     * {@inheritdoc}
     */
    public function newSimpleConsumerBuilder() {
        return new SimpleConsumerBuilder();
    }
    
    /**
     * {@inheritdoc}
     */
    public static function loadService() {
        if (self::$instance === null) {
            self::$instance = self::doLoad();
        }
        return self::$instance;
    }
    
    /**
     * {@inheritdoc}
     */
    public static function doLoad() {
        return new self();
    }
}
