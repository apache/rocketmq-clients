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
use Apache\Rocketmq\ClientMeterManager;
use Apache\Rocketmq\MetricsCollector;

/**
 * Client meter manager unit test class
 * 
 * Tests ClientMeterManager core functionality
 */
class ClientMeterManagerUnitTest extends TestCase
{
    /**
     * Test client meter manager creation
     */
    public function testClientMeterManagerCreation()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        
        $this->assertInstanceOf(ClientMeterManager::class, $meterManager);
        $this->assertEquals('test-client', $meterManager->getClientId());
    }
    
    /**
     * Test enable/disable
     */
    public function testEnableDisable()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        
        $this->assertFalse($meterManager->isEnabled());
        
        $meterManager->enable();
        $this->assertTrue($meterManager->isEnabled());
        
        $meterManager->disable();
        $this->assertFalse($meterManager->isEnabled());
    }
    
    /**
     * Test record histogram
     */
    public function testRecordHistogram()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        $meterManager->enable();
        
        $meterManager->record(
            \Apache\Rocketmq\HistogramEnum::SEND_COST_TIME,
            [\Apache\Rocketmq\MetricLabels::TOPIC => 'test-topic'],
            100.5
        );
        
        $this->assertTrue(true);
    }
    
    /**
     * Test increment counter
     */
    public function testIncrementCounter()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        $meterManager->enable();
        
        $meterManager->incrementCounter(
            'test_counter',
            ['label' => 'value'],
            5
        );
        
        $this->assertTrue(true);
    }
    
    /**
     * Test set gauge
     */
    public function testSetGauge()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        $meterManager->enable();
        
        $meterManager->setGauge(
            'test_gauge',
            ['label' => 'value'],
            42.0
        );
        
        $this->assertTrue(true);
    }
    
    /**
     * Test export metrics
     */
    public function testExportMetrics()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        $meterManager->enable();
        
        // Record some metrics
        $meterManager->record(
            \Apache\Rocketmq\HistogramEnum::SEND_COST_TIME,
            [\Apache\Rocketmq\MetricLabels::TOPIC => 'test-topic'],
            100.0
        );
        
        $result = $meterManager->exportMetrics();
        
        $this->assertTrue($result['success']);
        $this->assertArrayHasKey('count', $result);
        $this->assertArrayHasKey('metrics', $result);
    }
    
    /**
     * Test shutdown
     */
    public function testShutdown()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        $meterManager->enable();
        
        $meterManager->shutdown();
        
        $this->assertFalse($meterManager->isEnabled());
    }
}
