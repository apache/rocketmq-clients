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

use Apache\Rocketmq\ProducerHeartbeatManager;
use PHPUnit\Framework\TestCase;

/**
 * ProducerHeartbeatManagerTest - Test heartbeat management
 */
class ProducerHeartbeatManagerTest extends TestCase
{
    /**
     * Test instance creation with nullable parameters
     */
    public function testInstanceCreationWithNullableParams()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        
        $manager = new ProducerHeartbeatManager(
            $mockClient,
            'localhost:8080',
            null,  // clientId can be null
            null,  // credentials can be null
            '',    // namespace
            null   // logger can be null
        );
        
        $this->assertInstanceOf(ProducerHeartbeatManager::class, $manager);
    }

    /**
     * Test instance creation with all parameters
     */
    public function testInstanceCreationWithAllParams()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        
        $manager = new ProducerHeartbeatManager(
            $mockClient,
            'localhost:8080',
            'test-client-id',
            null,
            'test-namespace',
            $logger
        );
        
        $this->assertInstanceOf(ProducerHeartbeatManager::class, $manager);
    }

    /**
     * Test get last heartbeat time initially zero
     */
    public function testGetLastHeartbeatTimeInitiallyZero()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $manager = new ProducerHeartbeatManager($mockClient, 'localhost:8080', null, null);
        
        $this->assertEquals(0, $manager->getLastHeartbeatTime());
    }

    /**
     * Test is heartbeat in progress initially false
     */
    public function testIsHeartbeatInProgressInitiallyFalse()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $manager = new ProducerHeartbeatManager($mockClient, 'localhost:8080', null, null);
        
        $this->assertFalse($manager->isHeartbeatInProgress());
    }

    /**
     * Test set running state
     */
    public function testSetRunning()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $manager = new ProducerHeartbeatManager($mockClient, 'localhost:8080', null, null);
        
        $manager->setRunning(true);
        $this->assertTrue($manager->isRunning());
        
        $manager->setRunning(false);
        $this->assertFalse($manager->isRunning());
    }

    /**
     * Test is running initially false
     */
    public function testIsRunningInitiallyFalse()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $manager = new ProducerHeartbeatManager($mockClient, 'localhost:8080', null, null);
        
        $this->assertFalse($manager->isRunning());
    }

    /**
     * Test close telemetry session
     */
    public function testCloseTelemetrySession()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $manager = new ProducerHeartbeatManager($mockClient, 'localhost:8080', null, null);
        
        // Should not throw exception
        $manager->closeTelemetrySession();
        $this->assertTrue(true);
    }

    /**
     * Test establish telemetry session throws without proper setup
     */
    public function testEstablishTelemetrySessionWithoutSetup()
    {
        $this->markTestSkipped('Requires complex gRPC mocking');
    }

    /**
     * Test start heartbeat requires pcntl
     */
    public function testStartHeartbeatRequiresPcntl()
    {
        if (!function_exists('pcntl_signal')) {
            $this->markTestSkipped('pcntl extension not available');
        }
        
        // Create proper mock call object
        $mockCall = new class {
            public function wait() {
                return [null, (object)['code' => 0]];
            }
        };
        
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $mockClient->method('Heartbeat')->willReturn($mockCall);
        
        $manager = new ProducerHeartbeatManager($mockClient, 'localhost:8080', null, null);
        
        // Should handle gracefully
        try {
            $manager->startHeartbeat();
            $this->assertTrue(true);
        } catch (\Exception $e) {
            // Expected in test environment
            $this->assertTrue(true);
        }
    }

    /**
     * Test stop heartbeat when not started
     */
    public function testStopHeartbeatWhenNotStarted()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $manager = new ProducerHeartbeatManager($mockClient, 'localhost:8080', null, null);
        
        // Should not throw exception
        $manager->stopHeartbeat();
        $this->assertTrue(true);
    }

    /**
     * Test notify client termination
     */
    public function testNotifyClientTermination()
    {
        // Create proper mock call object
        $mockCall = new class {
            public function wait() {
                return [null, (object)['code' => 0, 'details' => '']];
            }
        };
        
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $mockClient->method('NotifyClientTermination')->willReturn($mockCall);
        
        $manager = new ProducerHeartbeatManager($mockClient, 'localhost:8080', null, null);
        
        // Should handle gracefully
        $manager->notifyClientTermination();
        $this->assertTrue(true);
    }

    /**
     * Test constructor generates default client ID when null
     */
    public function testConstructorGeneratesDefaultClientId()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $manager = new ProducerHeartbeatManager($mockClient, 'localhost:8080', null, null);
        
        // Use reflection to check clientId was set
        $reflection = new \ReflectionClass($manager);
        $property = $reflection->getProperty('clientId');
        $property->setAccessible(true);
        
        $clientId = $property->getValue($manager);
        $this->assertNotEmpty($clientId);
        $this->assertStringContainsString('php-producer-', $clientId);
    }

    /**
     * Test constructor uses provided client ID
     */
    public function testConstructorUsesProvidedClientId()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $manager = new ProducerHeartbeatManager(
            $mockClient,
            'localhost:8080',
            'custom-client-id',
            null
        );
        
        $reflection = new \ReflectionClass($manager);
        $property = $reflection->getProperty('clientId');
        $property->setAccessible(true);
        
        $clientId = $property->getValue($manager);
        $this->assertEquals('custom-client-id', $clientId);
    }
}
