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
class ClientServiceProvider {
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
     * Get singleton instance of ClientServiceProvider
     *
     * @return ClientServiceProvider
     */
    public static function getInstance() {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }
    
    /**
     * Create a new producer builder
     *
     * @return ProducerBuilder
     */
    public function newProducerBuilder() {
        return new ProducerBuilder();
    }
    
    /**
     * Create a new simple consumer builder
     *
     * @return SimpleConsumerBuilder
     */
    public function newSimpleConsumerBuilder() {
        return new SimpleConsumerBuilder();
    }
    
    /**
     * Create a new push consumer builder
     *
     * @return PushConsumerBuilder
     */
    public function newPushConsumerBuilder() {
        return new PushConsumerBuilder();
    }
    
    /**
     * Create a new lite push consumer builder
     *
     * @return LitePushConsumerBuilder
     */
    public function newLitePushConsumerBuilder() {
        return new LitePushConsumerBuilder();
    }
    
    /**
     * Create a new lite simple consumer builder
     *
     * @return LiteSimpleConsumerBuilder
     */
    public function newLiteSimpleConsumerBuilder() {
        return new LiteSimpleConsumerBuilder();
    }
    
    /**
     * Create a new message builder
     *
     * @return MessageBuilder
     */
    public function newMessageBuilder() {
        return new MessageBuilder();
    }
}
