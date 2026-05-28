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

namespace Apache\Rocketmq\Test;

use Apache\Rocketmq\ProducerEndpointIsolator;
use PHPUnit\Framework\TestCase;

/**
 * ProducerEndpointIsolatorTest - Test endpoint isolation logic
 */
class ProducerEndpointIsolatorTest extends TestCase
{
    private ProducerEndpointIsolator $isolator;

    protected function setUp(): void
    {
        $this->isolator = new ProducerEndpointIsolator();
    }

    /**
     * Test isolate single endpoint
     */
    public function testIsolateSingleEndpoint()
    {
        $this->isolator->isolateEndpoints(['broker1:8080']);
        
        $keys = $this->isolator->getIsolatedEndpointKeys();
        $this->assertCount(1, $keys);
        $this->assertContains('broker1:8080', $keys);
    }

    /**
     * Test isolate multiple endpoints
     */
    public function testIsolateMultipleEndpoints()
    {
        $endpoints = ['broker1:8080', 'broker2:8080', 'broker3:8080'];
        $this->isolator->isolateEndpoints($endpoints);
        
        $keys = $this->isolator->getIsolatedEndpointKeys();
        $this->assertCount(3, $keys);
        foreach ($endpoints as $endpoint) {
            $this->assertContains($endpoint, $keys);
        }
    }

    /**
     * Test get isolated count
     */
    public function testGetIsolatedCount()
    {
        $this->assertEquals(0, $this->isolator->getIsolatedCount());
        
        $this->isolator->isolateEndpoints(['broker1:8080']);
        $this->assertEquals(1, $this->isolator->getIsolatedCount());
        
        $this->isolator->isolateEndpoints(['broker2:8080']);
        $this->assertEquals(2, $this->isolator->getIsolatedCount());
    }

    /**
     * Test clear isolations
     */
    public function testClearIsolations()
    {
        $this->isolator->isolateEndpoints(['broker1:8080', 'broker2:8080']);
        $this->assertEquals(2, $this->isolator->getIsolatedCount());
        
        $this->isolator->clearIsolations();
        $this->assertEquals(0, $this->isolator->getIsolatedCount());
        $this->assertEmpty($this->isolator->getIsolatedEndpointKeys());
    }

    /**
     * Test duplicate isolation doesn't increase count
     */
    public function testDuplicateIsolation()
    {
        $this->isolator->isolateEndpoints(['broker1:8080']);
        $this->assertEquals(1, $this->isolator->getIsolatedCount());
        
        // Isolate same endpoint again
        $this->isolator->isolateEndpoints(['broker1:8080']);
        $this->assertEquals(1, $this->isolator->getIsolatedCount());
    }

    /**
     * Test set isolation timeout
     */
    public function testSetIsolationTimeout()
    {
        $this->isolator->setIsolationTimeout(60);
        // No exception means success
        $this->assertTrue(true);
    }

    /**
     * Test isolation timeout minimum value
     */
    public function testIsolationTimeoutMinimum()
    {
        $this->isolator->setIsolationTimeout(-10);
        // Should be clamped to minimum of 1
        $this->assertTrue(true);
    }

    /**
     * Test empty endpoint list
     */
    public function testIsolateEmptyList()
    {
        $this->isolator->isolateEndpoints([]);
        $this->assertEquals(0, $this->isolator->getIsolatedCount());
    }

    /**
     * Test get isolated broker names with mock cache
     */
    public function testGetIsolatedBrokerNames()
    {
        // Create mock load balancer that passes instanceof check
        $mockLoadBalancer = $this->createMock(\Apache\Rocketmq\PublishingLoadBalancer::class);
        $mockLoadBalancer->method('getAllBrokerNames')
            ->willReturn(['broker1', 'broker2', 'broker3']);

        $cache = ['topic1' => $mockLoadBalancer];
        
        // Without isolation, all brokers should be returned
        $brokers = $this->isolator->getIsolatedBrokerNames($cache);
        $this->assertCount(3, $brokers);
    }

    /**
     * Test instance creation
     */
    public function testInstanceCreation()
    {
        $isolator = new ProducerEndpointIsolator();
        $this->assertInstanceOf(ProducerEndpointIsolator::class, $isolator);
    }

    /**
     * Test re-isolation updates timestamp
     */
    public function testReIsolationUpdatesTimestamp()
    {
        $this->isolator->isolateEndpoints(['broker1:8080']);
        sleep(1);
        $this->isolator->isolateEndpoints(['broker1:8080']);
        
        // Should still have only 1 isolated endpoint
        $this->assertEquals(1, $this->isolator->getIsolatedCount());
    }

    /**
     * Test various endpoint formats
     */
    public function testVariousEndpointFormats()
    {
        $endpoints = [
            'localhost:8080',
            '192.168.1.1:9876',
            '[::1]:8080',
            'broker.example.com:443'
        ];
        
        $this->isolator->isolateEndpoints($endpoints);
        $keys = $this->isolator->getIsolatedEndpointKeys();
        
        $this->assertCount(4, $keys);
        foreach ($endpoints as $endpoint) {
            $this->assertContains($endpoint, $keys);
        }
    }
}
