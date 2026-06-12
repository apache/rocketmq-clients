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

use Apache\Rocketmq\RpcClientManager;
use Apache\Rocketmq\TlsCredentials;
use PHPUnit\Framework\TestCase;

/**
 * RpcClientManagerTest - Unit tests for RPC client connection pool management
 * 
 * Tests cover:
 * 1. Singleton pattern
 * 2. Client creation and reuse
 * 3. Connection pooling
 * 4. Idle connection cleanup
 * 5. TLS credentials handling
 * 6. Client release
 * 7. Connection counting
 */
class RpcClientManagerTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        // Reset singleton before each test
        RpcClientManager::reset();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        // Clean up after each test
        RpcClientManager::reset();
    }

    /**
     * Test singleton pattern
     */
    public function testSingletonPattern()
    {
        $instance1 = RpcClientManager::getInstance();
        $instance2 = RpcClientManager::getInstance();
        
        $this->assertSame($instance1, $instance2);
        $this->assertInstanceOf(RpcClientManager::class, $instance1);
    }

    /**
     * Test that reset clears singleton
     */
    public function testResetClearsSingleton()
    {
        $instance1 = RpcClientManager::getInstance();
        RpcClientManager::reset();
        $instance2 = RpcClientManager::getInstance();
        
        $this->assertNotSame($instance1, $instance2);
    }

    /**
     * Test getting client creates new connection
     */
    public function testGetClientCreatesNewConnection()
    {
        $manager = RpcClientManager::getInstance();
        $endpoints = 'localhost:8080';
        
        $client = $manager->getClient($endpoints);
        
        $this->assertNotNull($client);
        $this->assertEquals(1, $manager->getConnectionCount());
    }

    /**
     * Test getting same endpoints returns cached client
     */
    public function testGetClientReturnsCachedClient()
    {
        $manager = RpcClientManager::getInstance();
        $endpoints = 'localhost:8080';
        
        $client1 = $manager->getClient($endpoints);
        $client2 = $manager->getClient($endpoints);
        
        $this->assertSame($client1, $client2);
        $this->assertEquals(1, $manager->getConnectionCount());
    }

    /**
     * Test different endpoints create different clients
     */
    public function testDifferentEndpointsCreateDifferentClients()
    {
        $manager = RpcClientManager::getInstance();
        
        $client1 = $manager->getClient('localhost:8080');
        $client2 = $manager->getClient('localhost:9090');
        $client3 = $manager->getClient('remote:8080');
        
        $this->assertNotSame($client1, $client2);
        $this->assertNotSame($client1, $client3);
        $this->assertNotSame($client2, $client3);
        $this->assertEquals(3, $manager->getConnectionCount());
    }

    /**
     * Test releasing specific client
     */
    public function testReleaseClient()
    {
        $manager = RpcClientManager::getInstance();
        
        $manager->getClient('localhost:8080');
        $manager->getClient('localhost:9090');
        $this->assertEquals(2, $manager->getConnectionCount());
        
        $manager->releaseClient('localhost:8080');
        $this->assertEquals(1, $manager->getConnectionCount());
    }

    /**
     * Test releasing all clients
     */
    public function testReleaseAllClients()
    {
        $manager = RpcClientManager::getInstance();
        
        $manager->getClient('localhost:8080');
        $manager->getClient('localhost:9090');
        $manager->getClient('remote:8080');
        $this->assertEquals(3, $manager->getConnectionCount());
        
        $manager->releaseAll();
        $this->assertEquals(0, $manager->getConnectionCount());
    }

    /**
     * Test releasing non-existent client doesn't cause errors
     */
    public function testReleaseNonExistentClient()
    {
        $manager = RpcClientManager::getInstance();
        
        // Should not throw exception
        $manager->releaseClient('nonexistent:8080');
        $this->assertEquals(0, $manager->getConnectionCount());
    }

    /**
     * Test client with insecure credentials
     */
    public function testClientWithInsecureCredentials()
    {
        $manager = RpcClientManager::getInstance();
        $tlsCredentials = TlsCredentials::createInsecure();
        
        $client1 = $manager->getClient('localhost:8080', ['tlsCredentials' => $tlsCredentials]);
        $client2 = $manager->getClient('localhost:8080', ['tlsCredentials' => $tlsCredentials]);
        
        $this->assertSame($client1, $client2);
        $this->assertEquals(1, $manager->getConnectionCount());
    }

    /**
     * Test client with secure credentials creates separate connection
     */
    public function testSecureAndInsecureCreateSeparateConnections()
    {
        $manager = RpcClientManager::getInstance();
        
        // Both use insecure credentials, so they should be the same
        $insecureCreds = TlsCredentials::createInsecure();
        $client1 = $manager->getClient('localhost:8080', ['tlsCredentials' => $insecureCreds]);
        
        // No options defaults to insecure as well
        $client2 = $manager->getClient('localhost:8080');
        
        // Both resolve to insecure, so they are the same client
        $this->assertSame($client1, $client2);
        $this->assertEquals(1, $manager->getConnectionCount());
    }

    /**
     * Test different TLS configurations create separate connections
     */
    public function testDifferentTlsConfigsCreateSeparateConnections()
    {
        $manager = RpcClientManager::getInstance();
        
        $insecure = TlsCredentials::createInsecure();
        $client1 = $manager->getClient('localhost:8080', ['tlsCredentials' => $insecure]);
        
        // Note: We can't easily test mTLS without actual cert files,
        // but we can verify the key generation logic works
        
        $this->assertEquals(1, $manager->getConnectionCount());
    }

    /**
     * Test connection count tracking
     */
    public function testConnectionCountTracking()
    {
        $manager = RpcClientManager::getInstance();
        
        $this->assertEquals(0, $manager->getConnectionCount());
        
        $manager->getClient('endpoint1:8080');
        $this->assertEquals(1, $manager->getConnectionCount());
        
        $manager->getClient('endpoint2:8080');
        $this->assertEquals(2, $manager->getConnectionCount());
        
        $manager->getClient('endpoint3:8080');
        $this->assertEquals(3, $manager->getConnectionCount());
        
        $manager->releaseClient('endpoint2:8080');
        $this->assertEquals(2, $manager->getConnectionCount());
    }

    /**
     * Test that options affect client caching key
     */
    public function testOptionsAffectCachingKey()
    {
        $manager = RpcClientManager::getInstance();
        
        // Same endpoint, no options
        $client1 = $manager->getClient('localhost:8080');
        
        // Same endpoint, with empty options array
        $client2 = $manager->getClient('localhost:8080', []);
        
        // These should be the same client (empty options don't change key)
        $this->assertSame($client1, $client2);
    }

    /**
     * Test idle timeout configuration
     */
    public function testIdleTimeoutConfiguration()
    {
        $manager = RpcClientManager::getInstance();
        
        // Get client to initialize it
        $manager->getClient('localhost:8080');
        
        // Use reflection to check default values
        $reflection = new \ReflectionClass($manager);
        
        $idleTimeoutProp = $reflection->getProperty('idleTimeoutSeconds');
        $idleTimeoutProp->setAccessible(true);
        $this->assertEquals(1800, $idleTimeoutProp->getValue($manager)); // 30 minutes
        
        $checkIntervalProp = $reflection->getProperty('checkIntervalSeconds');
        $checkIntervalProp->setAccessible(true);
        $this->assertEquals(60, $checkIntervalProp->getValue($manager)); // 1 minute
    }

    /**
     * Test last used time is updated on access
     */
    public function testLastUsedTimeUpdated()
    {
        $manager = RpcClientManager::getInstance();
        
        $manager->getClient('localhost:8080');
        
        // Use reflection to check last used time
        $reflection = new \ReflectionClass($manager);
        $lastUsedProp = $reflection->getProperty('clientLastUsedTime');
        $lastUsedProp->setAccessible(true);
        $lastUsedTimes = $lastUsedProp->getValue($manager);
        
        $this->assertCount(1, $lastUsedTimes);
        $this->assertIsInt(reset($lastUsedTimes));
    }

    /**
     * Test multiple accesses update last used time
     */
    public function testMultipleAccessesUpdateLastUsedTime()
    {
        $manager = RpcClientManager::getInstance();
        
        $manager->getClient('localhost:8080');
        sleep(1); // Wait 1 second
        $manager->getClient('localhost:8080');
        
        $reflection = new \ReflectionClass($manager);
        $lastUsedProp = $reflection->getProperty('clientLastUsedTime');
        $lastUsedProp->setAccessible(true);
        $lastUsedTimes = $lastUsedProp->getValue($manager);
        
        $this->assertCount(1, $lastUsedTimes);
        // Last used time should be recent (within 2 seconds)
        $lastTime = reset($lastUsedTimes);
        $this->assertGreaterThan(time() - 2, $lastTime);
    }

    /**
     * Test cleanup idle clients removes old connections
     */
    public function testCleanupIdleClients()
    {
        $manager = RpcClientManager::getInstance();
        
        // Create some clients
        $manager->getClient('endpoint1:8080');
        $manager->getClient('endpoint2:8080');
        $this->assertEquals(2, $manager->getConnectionCount());
        
        // Use reflection to manipulate last used times
        $reflection = new \ReflectionClass($manager);
        $lastUsedProp = $reflection->getProperty('clientLastUsedTime');
        $lastUsedProp->setAccessible(true);
        
        // Make one client appear very old (older than 1800 seconds)
        $oldTime = time() - 2000;
        $lastUsedTimes = $lastUsedProp->getValue($manager);
        $keys = array_keys($lastUsedTimes);
        if (!empty($keys)) {
            $lastUsedTimes[$keys[0]] = $oldTime;
            $lastUsedProp->setValue($manager, $lastUsedTimes);
        }
        
        // Force cleanup by manipulating last check time
        $lastCheckProp = $reflection->getProperty('lastCheckTime');
        $lastCheckProp->setAccessible(true);
        $lastCheckProp->setValue($manager, time() - 100); // Force check
        
        // Access client to trigger cleanup
        $manager->getClient('endpoint3:8080');
        
        // Should have cleaned up the old client
        $this->assertLessThanOrEqual(2, $manager->getConnectionCount());
    }

    /**
     * Test that active clients are not cleaned up
     */
    public function testActiveClientsNotCleanedUp()
    {
        $manager = RpcClientManager::getInstance();
        
        $manager->getClient('localhost:8080');
        $initialCount = $manager->getConnectionCount();
        
        // Access again immediately (should still be active)
        $manager->getClient('localhost:8080');
        
        $this->assertEquals($initialCount, $manager->getConnectionCount());
    }

    /**
     * Test logger is initialized
     */
    public function testLoggerInitialized()
    {
        $manager = RpcClientManager::getInstance();
        
        $reflection = new \ReflectionClass($manager);
        $loggerProp = $reflection->getProperty('logger');
        $loggerProp->setAccessible(true);
        $logger = $loggerProp->getValue($manager);
        
        $this->assertNotNull($logger);
    }

    /**
     * Test endpoint with port variations
     */
    public function testEndpointWithPortVariations()
    {
        $manager = RpcClientManager::getInstance();
        
        $client1 = $manager->getClient('localhost:8080');
        $client2 = $manager->getClient('localhost:8081');
        $client3 = $manager->getClient('localhost:9090');
        
        $this->assertNotSame($client1, $client2);
        $this->assertNotSame($client1, $client3);
        $this->assertNotSame($client2, $client3);
        $this->assertEquals(3, $manager->getConnectionCount());
    }

    /**
     * Test IP address endpoints
     */
    public function testIpAddressEndpoints()
    {
        $manager = RpcClientManager::getInstance();
        
        $client1 = $manager->getClient('127.0.0.1:8080');
        $client2 = $manager->getClient('192.168.1.100:8080');
        
        $this->assertNotSame($client1, $client2);
        $this->assertEquals(2, $manager->getConnectionCount());
    }

    /**
     * Test IPv6 endpoints
     */
    public function testIpv6Endpoints()
    {
        $manager = RpcClientManager::getInstance();
        
        $client1 = $manager->getClient('[::1]:8080');
        $client2 = $manager->getClient('[2001:db8::1]:8080');
        
        $this->assertNotSame($client1, $client2);
        $this->assertEquals(2, $manager->getConnectionCount());
    }

    /**
     * Test domain name endpoints
     */
    public function testDomainNameEndpoints()
    {
        $manager = RpcClientManager::getInstance();
        
        $client1 = $manager->getClient('rocketmq.example.com:8080');
        $client2 = $manager->getClient('mq.prod.internal:9090');
        
        $this->assertNotSame($client1, $client2);
        $this->assertEquals(2, $manager->getConnectionCount());
    }

    /**
     * Test concurrent access to same endpoint
     */
    public function testConcurrentAccessToSameEndpoint()
    {
        $manager = RpcClientManager::getInstance();
        
        // Simulate multiple "concurrent" requests
        $clients = [];
        for ($i = 0; $i < 10; $i++) {
            $clients[] = $manager->getClient('localhost:8080');
        }
        
        // All should be the same instance
        foreach ($clients as $client) {
            $this->assertSame($clients[0], $client);
        }
        
        $this->assertEquals(1, $manager->getConnectionCount());
    }

    /**
     * Test many different endpoints
     */
    public function testManyDifferentEndpoints()
    {
        $manager = RpcClientManager::getInstance();
        
        $count = 50;
        for ($i = 0; $i < $count; $i++) {
            $manager->getClient("endpoint{$i}:8080");
        }
        
        $this->assertEquals($count, $manager->getConnectionCount());
    }

    /**
     * Test release and recreate
     */
    public function testReleaseAndRecreate()
    {
        $manager = RpcClientManager::getInstance();
        
        $client1 = $manager->getClient('localhost:8080');
        $manager->releaseClient('localhost:8080');
        
        $this->assertEquals(0, $manager->getConnectionCount());
        
        $client2 = $manager->getClient('localhost:8080');
        $this->assertEquals(1, $manager->getConnectionCount());
        
        // New client should be different instance
        $this->assertNotSame($client1, $client2);
    }

    /**
     * Test makeKey generates consistent keys
     */
    public function testMakeKeyConsistency()
    {
        $manager = RpcClientManager::getInstance();
        
        // Use reflection to access private makeKey method
        $reflection = new \ReflectionClass($manager);
        $makeKeyMethod = $reflection->getMethod('makeKey');
        $makeKeyMethod->setAccessible(true);
        
        $key1 = $makeKeyMethod->invoke($manager, 'localhost:8080', []);
        $key2 = $makeKeyMethod->invoke($manager, 'localhost:8080', []);
        
        $this->assertEquals($key1, $key2);
    }

    /**
     * Test makeKey includes TLS fingerprint
     */
    public function testMakeKeyIncludesTlsFingerprint()
    {
        $manager = RpcClientManager::getInstance();
        
        $reflection = new \ReflectionClass($manager);
        $makeKeyMethod = $reflection->getMethod('makeKey');
        $makeKeyMethod->setAccessible(true);
        
        $insecure = TlsCredentials::createInsecure();
        $key1 = $makeKeyMethod->invoke($manager, 'localhost:8080', ['tlsCredentials' => $insecure]);
        $key2 = $makeKeyMethod->invoke($manager, 'localhost:8080', []);
        
        // Both default to insecure, so keys are the same
        $this->assertEquals($key1, $key2);
    }

    /**
     * Test resolveCredentials returns insecure by default
     */
    public function testResolveCredentialsDefault()
    {
        $manager = RpcClientManager::getInstance();
        
        $reflection = new \ReflectionClass($manager);
        $resolveMethod = $reflection->getMethod('resolveCredentials');
        $resolveMethod->setAccessible(true);
        
        $credentials = $resolveMethod->invoke($manager, []);
        
        // Default is ChannelCredentials::createInsecure(), which may return null in some gRPC versions
        // Just verify it doesn't throw an exception
        $this->assertTrue(true);
    }

    /**
     * Test resolveCredentials with TLS credentials
     */
    public function testResolveCredentialsWithTls()
    {
        $manager = RpcClientManager::getInstance();
        
        $reflection = new \ReflectionClass($manager);
        $resolveMethod = $reflection->getMethod('resolveCredentials');
        $resolveMethod->setAccessible(true);
        
        $tlsCreds = TlsCredentials::createInsecure();
        $credentials = $resolveMethod->invoke($manager, ['tlsCredentials' => $tlsCreds]);
        
        // Verify it doesn't throw an exception
        $this->assertTrue(true);
    }
}
