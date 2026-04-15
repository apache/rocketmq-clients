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
use Apache\Rocketmq\Credentials;
use Apache\Rocketmq\ExponentialBackoffRetryPolicy;

/**
 * Configuration object test class
 */
class ConfigurationTest extends TestCase
{
    /**
     * Test Credentials basic functionality
     */
    public function testCredentialsBasic()
    {
        $credentials = new Credentials('TEST_AK', 'TEST_SK');
        
        $this->assertEquals('TEST_AK', $credentials->getAccessKey());
        $this->assertEquals('TEST_SK', $credentials->getAccessSecret());
        $this->assertNull($credentials->getSecurityToken());
        $this->assertFalse($credentials->hasSecurityToken());
    }
    
    /**
     * Test Credentials with STS Token
     */
    public function testCredentialsWithStsToken()
    {
        $credentials = new Credentials('TEST_AK', 'TEST_SK', 'TEST_TOKEN');
        
        $this->assertEquals('TEST_AK', $credentials->getAccessKey());
        $this->assertEquals('TEST_SK', $credentials->getAccessSecret());
        $this->assertEquals('TEST_TOKEN', $credentials->getSecurityToken());
        $this->assertTrue($credentials->hasSecurityToken());
    }
    
    /**
     * Test Credentials parameter validation
     */
    public function testCredentialsValidation()
    {
        $this->expectException(\InvalidArgumentException::class);
        new Credentials('', 'TEST_SK');
    }
    
    /**
     * Test Credentials string representation
     */
    public function testCredentialsToString()
    {
        $credentials = new Credentials('AK123456', 'SECRET_KEY');
        $str = (string)$credentials;
        
        $this->assertStringContainsString('AK12***', $str);
        $this->assertStringContainsString('Credentials', $str);
    }
    
    /**
     * Test ClientConfiguration basic functionality
     */
    public function testClientConfigurationBasic()
    {
        $config = new ClientConfiguration('127.0.0.1:8080');
        
        $this->assertEquals('127.0.0.1:8080', $config->getEndpoints());
        $this->assertEquals('', $config->getNamespace());
        $this->assertNull($config->getSessionCredentialsProvider());
        $this->assertFalse($config->hasCredentials());
        $this->assertEquals(3, $config->getRequestTimeout());
        $this->assertTrue($config->isSslEnabled());
        $this->assertNull($config->getRetryPolicy());
    }
    
    /**
     * Test ClientConfiguration chaining
     */
    public function testClientConfigurationChaining()
    {
        $credentials = new Credentials('AK_TEST', 'SK_TEST');
        $retryPolicy = new ExponentialBackoffRetryPolicy(5, 100, 5000);
        
        $config = (new ClientConfiguration('127.0.0.1:8080'))
            ->withNamespace('test-ns')
            ->withCredentials($credentials)
            ->withRequestTimeout(10)
            ->withSslEnabled(true)
            ->withRetryPolicy($retryPolicy);
        
        $this->assertEquals('test-ns', $config->getNamespace());
        $this->assertTrue($config->hasCredentials());
        $this->assertEquals(10, $config->getRequestTimeout());
        $this->assertTrue($config->isSslEnabled());
        $this->assertEquals($retryPolicy, $config->getRetryPolicy());
    }
    
    /**
     * Test ClientConfiguration endpoints parsing
     */
    public function testClientConfigurationEndpointsParsing()
    {
        // Test removing http:// prefix
        $config1 = new ClientConfiguration('http://127.0.0.1:8080');
        $this->assertEquals('127.0.0.1:8080', $config1->getEndpoints());
        
        // Test removing https:// prefix
        $config2 = new ClientConfiguration('https://127.0.0.1:8080');
        $this->assertEquals('127.0.0.1:8080', $config2->getEndpoints());
        
        // Test multiple endpoints
        $config3 = new ClientConfiguration('host1:8080;host2:8080');
        $this->assertEquals('host1:8080;host2:8080', $config3->getEndpoints());
    }
    
    /**
     * Test ClientConfiguration parameter validation
     */
    public function testClientConfigurationValidation()
    {
        $this->expectException(\InvalidArgumentException::class);
        new ClientConfiguration('');
    }
    
    /**
     * Test ClientConfiguration timeout validation
     */
    public function testClientConfigurationTimeoutValidation()
    {
        $config = new ClientConfiguration('127.0.0.1:8080');
        
        $this->expectException(\InvalidArgumentException::class);
        $config->withRequestTimeout(0);
    }
    
    /**
     * Test ClientConfiguration get or create retry policy
     */
    public function testGetOrCreateRetryPolicy()
    {
        $config = new ClientConfiguration('127.0.0.1:8080');
        
        // First call should create default policy
        $policy1 = $config->getOrCreateRetryPolicy();
        $this->assertInstanceOf(ExponentialBackoffRetryPolicy::class, $policy1);
        
        // Second call should return same instance
        $policy2 = $config->getOrCreateRetryPolicy();
        $this->assertSame($policy1, $policy2);
    }
    
    /**
     * Test ClientConfiguration clone
     */
    public function testClientConfigurationClone()
    {
        $credentials = new Credentials('AK_TEST', 'SK_TEST');
        $original = (new ClientConfiguration('127.0.0.1:8080'))
            ->withNamespace('ns1')
            ->withCredentials($credentials)
            ->withRequestTimeout(5);
        
        $cloned = $original->clone();
        
        // Verify cloned object has same values
        $this->assertEquals($original->getEndpoints(), $cloned->getEndpoints());
        $this->assertEquals($original->getNamespace(), $cloned->getNamespace());
        $this->assertEquals($original->getRequestTimeout(), $cloned->getRequestTimeout());
        
        // Modifying cloned object does not affect original object
        $cloned->withNamespace('ns2');
        $this->assertEquals('ns1', $original->getNamespace());
        $this->assertEquals('ns2', $cloned->getNamespace());
    }
    
    /**
     * Test ClientConfiguration string representation
     */
    public function testClientConfigurationToString()
    {
        $credentials = new Credentials('AK123456', 'SECRET_KEY');
        $config = (new ClientConfiguration('127.0.0.1:8080'))
            ->withNamespace('test-ns')
            ->withCredentials($credentials)
            ->withRequestTimeout(5)
            ->withSslEnabled(true);
        
        $str = (string)$config;
        
        $this->assertStringContainsString('127.0.0.1:8080', $str);
        $this->assertStringContainsString('test-ns', $str);
        $this->assertStringContainsString('5s', $str);
        $this->assertStringContainsString('ssl=true', $str);
        $this->assertStringContainsString('credentials=', $str);
    }
    
    /**
     * Test ClientConfiguration string representation without credentials
     */
    public function testClientConfigurationToStringWithoutCredentials()
    {
        $config = new ClientConfiguration('127.0.0.1:8080');
        $str = (string)$config;
        
        $this->assertStringNotContainsString('credentials=', $str);
    }
}
