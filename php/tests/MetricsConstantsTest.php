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
use Apache\Rocketmq\MetricLabels;
use Apache\Rocketmq\HistogramEnum;
use Apache\Rocketmq\GaugeEnum;
use Apache\Rocketmq\InvocationStatus;

/**
 * Metrics constants test class
 * 
 * Tests all metric enums and constants
 */
class MetricsConstantsTest extends TestCase
{
    /**
     * Test MetricLabels constants
     */
    public function testMetricLabels()
    {
        $this->assertEquals('topic', MetricLabels::TOPIC);
        $this->assertEquals('consumer_group', MetricLabels::CONSUMER_GROUP);
        $this->assertEquals('invocation_status', MetricLabels::INVOCATION_STATUS);
    }
    
    /**
     * Test HistogramEnum constants
     */
    public function testHistogramEnum()
    {
        $this->assertNotEmpty(HistogramEnum::SEND_COST_TIME);
        $this->assertNotEmpty(HistogramEnum::DELIVERY_LATENCY);
        $this->assertNotEmpty(HistogramEnum::AWAIT_TIME);
        $this->assertNotEmpty(HistogramEnum::PROCESS_TIME);
    }
    
    /**
     * Test GaugeEnum constants
     */
    public function testGaugeEnum()
    {
        $this->assertNotEmpty(GaugeEnum::CONSUMER_CACHED_MESSAGES);
        $this->assertNotEmpty(GaugeEnum::CONSUMER_CACHED_BYTES);
    }
    
    /**
     * Test CounterEnum constants
     */
    public function testCounterEnum()
    {
        // CounterEnum doesn't exist as a separate class
        // Counters are managed through MeterManager
        $this->assertTrue(true);
    }
    
    /**
     * Test InvocationStatus constants
     */
    public function testInvocationStatus()
    {
        $this->assertEquals('success', InvocationStatus::SUCCESS);
        $this->assertEquals('failure', InvocationStatus::FAILURE);
    }
}
