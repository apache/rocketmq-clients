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

namespace Apache\Rocketmq\Tests;

use PHPUnit\Framework\TestCase;
use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Producer;

/**
 * Simple metrics test class
 * 
 * Tests one-line metrics enablement for Producer
 */
class SimpleMetricsTest extends TestCase
{
    /**
     * Test producer has enableMetrics method
     */
    public function testProducerHasEnableMetricsMethod()
    {
        $config = new ClientConfiguration('127.0.0.1:8080');
        $producer = Producer::getInstance($config, 'test-topic');
        
        $this->assertTrue(method_exists($producer, 'enableMetrics'));
    }
    
    /**
     * Test enableMetrics returns producer instance for chaining
     */
    public function testEnableMetricsReturnsInstance()
    {
        $config = new ClientConfiguration('127.0.0.1:8080');
        $producer = Producer::getInstance($config, 'test-topic');
        
        $result = $producer->enableMetrics();
        
        $this->assertSame($producer, $result);
    }
    
    /**
     * Test enableMetrics initializes meter manager
     */
    public function testEnableMetricsInitializesMeterManager()
    {
        $config = new ClientConfiguration('127.0.0.1:8080');
        $producer = Producer::getInstance($config, 'test-topic');
        
        $producer->enableMetrics();
        
        $meterManager = $producer->getMeterManager();
        $this->assertNotNull($meterManager);
        $this->assertTrue($meterManager->isEnabled());
    }
    
    /**
     * Test enableMetrics with custom endpoint
     */
    public function testEnableMetricsWithCustomEndpoint()
    {
        $config = new ClientConfiguration('127.0.0.1:8080');
        $producer = Producer::getInstance($config, 'test-topic');
        
        $producer->enableMetrics('http://localhost:4317', 30);
        
        $meterManager = $producer->getMeterManager();
        $this->assertNotNull($meterManager);
    }
    
    /**
     * Test disableMetrics method exists
     */
    public function testDisableMetricsMethodExists()
    {
        $config = new ClientConfiguration('127.0.0.1:8080');
        $producer = Producer::getInstance($config, 'test-topic');
        
        $this->assertTrue(method_exists($producer, 'disableMetrics'));
    }
    
    /**
     * Test getMeterManager method exists
     */
    public function testGetMeterManagerMethodExists()
    {
        $config = new ClientConfiguration('127.0.0.1:8080');
        $producer = Producer::getInstance($config, 'test-topic');
        
        $this->assertTrue(method_exists($producer, 'getMeterManager'));
    }
}
