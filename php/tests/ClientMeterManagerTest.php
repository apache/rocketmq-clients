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
 * Client meter manager integration test class
 * 
 * Tests MeterManager integration with Producer/Consumer
 */
class ClientMeterManagerTest extends TestCase
{
    /**
     * Test meter manager integration with metrics collector
     */
    public function testMeterManagerIntegration()
    {
        $clientId = 'test-client';
        $metricsCollector = new MetricsCollector($clientId);
        $meterManager = new ClientMeterManager($clientId, $metricsCollector);
        
        $this->assertEquals($clientId, $meterManager->getClientId());
        $this->assertFalse($meterManager->isEnabled());
    }
    
    /**
     * Test enable metrics flow
     */
    public function testEnableMetricsFlow()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        
        $meterManager->enable();
        
        $this->assertTrue($meterManager->isEnabled());
    }
    
    /**
     * Test record and export metrics
     */
    public function testRecordAndExportMetrics()
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
        
        // Export
        $result = $meterManager->exportMetrics();
        
        $this->assertTrue($result['success']);
        $this->assertGreaterThan(0, $result['count']);
    }
    
    /**
     * Test disable metrics
     */
    public function testDisableMetrics()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        
        $meterManager->enable();
        $this->assertTrue($meterManager->isEnabled());
        
        $meterManager->disable();
        $this->assertFalse($meterManager->isEnabled());
    }
}
