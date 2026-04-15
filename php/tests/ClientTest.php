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
 * Unless required by applicable law or required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache\Rocketmq\Tests;

use PHPUnit\Framework\TestCase;
use Apache\Rocketmq\Producer;

// SimpleConsumer and PushConsumer are defined in Consumer.php
require_once __DIR__ . '/../Consumer.php';

class ProducerTest extends TestCase
{
    public function testProducerInstance()
    {
        $endpoints = 'test-endpoints:8080';
        $topic = 'test-topic';
        
        $producer1 = Producer::getInstance($endpoints, $topic);
        $producer2 = Producer::getInstance($endpoints, $topic);
        
        // Verify it's an instance of the same class (don't compare clientId, as it's different each time)
        $this->assertInstanceOf(Producer::class, $producer1);
        $this->assertInstanceOf(Producer::class, $producer2);
        $this->assertEquals($producer1->getState(), $producer2->getState());
    }
    
    public function testClientIdGeneration()
    {
        $producer = Producer::getInstance('test:8080', 'test');
        
        // Verify Client ID format
        $reflection = new \ReflectionClass($producer);
        $property = $reflection->getProperty('clientId');
        $property->setAccessible(true);
        $clientId = $property->getValue($producer);
        
        $this->assertNotEmpty($clientId);
        $this->assertStringContainsString('@', $clientId);
    }
}

class SimpleConsumerTest extends TestCase
{
    public function testConsumerInstance()
    {
        $endpoints = 'test-endpoints:8080';
        $consumerGroup = 'GID_test';
        $topic = 'test-topic';
        
        $consumer1 = \Apache\Rocketmq\SimpleConsumer::getInstance($endpoints, $consumerGroup, $topic);
        $consumer2 = \Apache\Rocketmq\SimpleConsumer::getInstance($endpoints, $consumerGroup, $topic);
        
        // Verify it's an instance of the same class
        $this->assertInstanceOf(\Apache\Rocketmq\SimpleConsumer::class, $consumer1);
        $this->assertInstanceOf(\Apache\Rocketmq\SimpleConsumer::class, $consumer2);
    }
}

class PushConsumerTest extends TestCase
{
    public function testMessageListener()
    {
        $endpoints = 'test-endpoints:8080';
        $consumerGroup = 'GID_test';
        $topic = 'test-topic';
        
        $consumer = \Apache\Rocketmq\PushConsumer::getInstance($endpoints, $consumerGroup, $topic);
        
        // Set message listener
        $listenerCalled = false;
        $consumer->setMessageListener(function($message) use (&$listenerCalled) {
            $listenerCalled = true;
            return true;
        });
        
        // Verify listener is set
        $reflection = new \ReflectionClass($consumer);
        $property = $reflection->getProperty('messageListener');
        $property->setAccessible(true);
        $listener = $property->getValue($consumer);
        
        $this->assertIsCallable($listener);
    }
}
