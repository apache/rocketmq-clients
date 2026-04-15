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
use Apache\Rocketmq\MetricsSupportTrait;

/**
 * Consumer metrics test class
 * 
 * Tests consumer metrics functionality using MetricsSupportTrait
 */
class ConsumerMetricsTest extends TestCase
{
    /**
     * Test MetricsSupportTrait basic functionality
     */
    public function testMetricsSupportTraitBasic()
    {
        $consumer = new class('test-client', 'test-group') {
            use MetricsSupportTrait;
            
            private $clientId;
            private $consumerGroup;
            
            public function __construct(string $clientId, string $consumerGroup) {
                $this->clientId = $clientId;
                $this->consumerGroup = $consumerGroup;
                $this->initMetrics($clientId);
            }
            
            public function getClientId(): string {
                return $this->clientId;
            }
            
            public function getConsumerGroup(): string {
                return $this->consumerGroup;
            }
        };
        
        $this->assertEquals('test-client', $consumer->getClientId());
        $this->assertEquals('test-group', $consumer->getConsumerGroup());
    }
    
    /**
     * Test enableMetrics method
     */
    public function testEnableMetrics()
    {
        $consumer = new class('test-client', 'test-group') {
            use MetricsSupportTrait;
            
            private $clientId;
            private $consumerGroup;
            
            public function __construct(string $clientId, string $consumerGroup) {
                $this->clientId = $clientId;
                $this->consumerGroup = $consumerGroup;
                $this->initMetrics($clientId);
            }
            
            public function getClientId(): string {
                return $this->clientId;
            }
            
            public function getConsumerGroup(): string {
                return $this->consumerGroup;
            }
        };
        
        // Enable metrics
        $result = $consumer->enableMetrics();
        
        $this->assertSame($consumer, $result);
        
        $meterManager = $consumer->getMeterManager();
        $this->assertNotNull($meterManager);
        $this->assertTrue($meterManager->isEnabled());
    }
    
    /**
     * Test disableMetrics method
     */
    public function testDisableMetrics()
    {
        $consumer = new class('test-client', 'test-group') {
            use MetricsSupportTrait;
            
            private $clientId;
            
            public function __construct(string $clientId) {
                $this->clientId = $clientId;
                $this->initMetrics($clientId);
            }
            
            public function getClientId(): string {
                return $this->clientId;
            }
            
            public function getConsumerGroup(): string {
                return 'test-group';
            }
        };
        
        $consumer->enableMetrics();
        $this->assertTrue($consumer->getMeterManager()->isEnabled());
        
        $consumer->disableMetrics();
        $this->assertFalse($consumer->getMeterManager()->isEnabled());
    }
    
    /**
     * Test metrics collection
     */
    public function testMetricsCollection()
    {
        $consumer = new class('test-client', 'test-group') {
            use MetricsSupportTrait;
            
            private $clientId;
            
            public function __construct(string $clientId) {
                $this->clientId = $clientId;
                $this->initMetrics($clientId);
            }
            
            public function getClientId(): string {
                return $this->clientId;
            }
            
            public function getConsumerGroup(): string {
                return 'test-group';
            }
        };
        
        $consumer->enableMetrics();
        
        // Record some metrics
        $meterManager = $consumer->getMeterManager();
        $meterManager->setGauge(
            \Apache\Rocketmq\GaugeEnum::CONSUMER_CACHED_MESSAGES,
            [
                \Apache\Rocketmq\MetricLabels::TOPIC => 'test-topic',
                \Apache\Rocketmq\MetricLabels::CONSUMER_GROUP => 'test-group',
            ],
            10.0
        );
        
        // Export and verify
        $result = $meterManager->exportMetrics();
        $this->assertTrue($result['success']);
        $this->assertGreaterThan(0, $result['count']);
    }
    
    /**
     * Test shutdownMetrics method
     */
    public function testShutdownMetrics()
    {
        $consumer = new class('test-client', 'test-group') {
            use MetricsSupportTrait;
            
            private $clientId;
            
            public function __construct(string $clientId) {
                $this->clientId = $clientId;
                $this->initMetrics($clientId);
            }
            
            public function getClientId(): string {
                return $this->clientId;
            }
            
            public function getConsumerGroup(): string {
                return 'test-group';
            }
            
            // Public wrapper for protected method
            public function shutdownMetricsPublic(): void {
                $this->shutdownMetrics();
            }
        };
        
        $consumer->enableMetrics();
        $this->assertTrue($consumer->getMeterManager()->isEnabled());
        
        $consumer->shutdownMetricsPublic();
        $this->assertFalse($consumer->getMeterManager()->isEnabled());
    }
}
