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
 * Advanced metrics features test class
 * 
 * Tests custom gauge observers and advanced metrics
 */
class AdvancedMetricsTest extends TestCase
{
    /**
     * Test custom gauge observer
     */
    public function testCustomGaugeObserver()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        $meterManager->enable();
        
        $observerCalled = false;
        $meterManager->setGaugeObserver(function() use (&$observerCalled) {
            $observerCalled = true;
            return [];
        });
        
        $meterManager->updateGauges();
        
        $this->assertTrue($observerCalled);
    }
    
    /**
     * Test OTLP export concept
     */
    public function testOtlpExport()
    {
        $this->markTestSkipped('OTLP export requires external endpoint');
    }
}
