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
use Apache\Rocketmq\ExponentialBackoffRetryPolicy;

/**
 * Retry mechanism test class
 */
class RetryPolicyTest extends TestCase
{
    /**
     * Test basic configuration
     */
    public function testBasicConfiguration()
    {
        $policy = new ExponentialBackoffRetryPolicy();
        
        $this->assertEquals(3, $policy->getMaxAttempts());
        $this->assertEquals(100, $policy->getInitialBackoff());
        $this->assertEquals(5000, $policy->getMaxBackoff());
        $this->assertEquals(2.0, $policy->getBackoffMultiplier());
    }
    
    /**
     * Test custom configuration
     */
    public function testCustomConfiguration()
    {
        $policy = new ExponentialBackoffRetryPolicy(5, 200, 10000, 3.0);
        
        $this->assertEquals(5, $policy->getMaxAttempts());
        $this->assertEquals(200, $policy->getInitialBackoff());
        $this->assertEquals(10000, $policy->getMaxBackoff());
        $this->assertEquals(3.0, $policy->getBackoffMultiplier());
    }
    
    /**
     * Test immediate retry policy
     */
    public function testImmediateRetryPolicy()
    {
        $policy = ExponentialBackoffRetryPolicy::immediatelyRetryPolicy(3);
        
        for ($i = 1; $i <= 5; $i++) {
            $delay = $policy->getNextAttemptDelay($i);
            $this->assertEquals(0, $delay);
        }
    }
    
    /**
     * Test fixed delay policy
     */
    public function testFixedDelayPolicy()
    {
        $policy = ExponentialBackoffRetryPolicy::fixedDelayRetryPolicy(3, 1000);
        
        for ($i = 1; $i <= 5; $i++) {
            $delay = $policy->getNextAttemptDelay($i);
            $this->assertEquals(1000, $delay);
        }
    }
    
    /**
     * Test exponential backoff calculation
     */
    public function testExponentialBackoffCalculation()
    {
        $policy = new ExponentialBackoffRetryPolicy(10, 100, 5000, 2.0);
        
        // First retry: 100ms
        $this->assertEquals(100, $policy->getNextAttemptDelay(1));
        
        // Second retry: 200ms
        $this->assertEquals(200, $policy->getNextAttemptDelay(2));
        
        // Third retry: 400ms
        $this->assertEquals(400, $policy->getNextAttemptDelay(3));
        
        // Should cap at maxBackoff (5000ms)
        $delay = $policy->getNextAttemptDelay(10);
        $this->assertLessThanOrEqual(5000, $delay);
    }
    
    /**
     * Test exception retry judgment - retryable exceptions
     */
    public function testRetryableExceptions()
    {
        $policy = new ExponentialBackoffRetryPolicy();
        
        // Connection timeout (code 4)
        $exception1 = new \Exception("Connection timeout", 4);
        $this->assertTrue($policy->shouldRetry(1, $exception1));
        
        // Service unavailable (code 14)
        $exception2 = new \Exception("Service unavailable", 14);
        $this->assertTrue($policy->shouldRetry(1, $exception2));
        
        // Too many requests (code 8)
        $exception3 = new \Exception("Too many requests", 8);
        $this->assertTrue($policy->shouldRetry(1, $exception3));
    }
    
    /**
     * Test exception retry judgment - non-retryable exceptions
     */
    public function testNonRetryableExceptions()
    {
        $policy = new ExponentialBackoffRetryPolicy();
        
        // Invalid parameter (code 3)
        $exception1 = new \Exception("Invalid parameter", 3);
        $this->assertFalse($policy->shouldRetry(1, $exception1));
        
        // Permission denied (code 7)
        $exception2 = new \Exception("Permission denied", 7);
        $this->assertFalse($policy->shouldRetry(1, $exception2));
    }
    
    /**
     * Test retry count limit
     */
    public function testRetryCountLimit()
    {
        $policy = new ExponentialBackoffRetryPolicy(3, 100, 5000, 2.0);
        $exception = new \Exception("Network error", 14);
        
        // Should allow retries up to maxAttempts-1 (attempts are 0-indexed)
        $this->assertTrue($policy->shouldRetry(0, $exception));
        $this->assertTrue($policy->shouldRetry(1, $exception));
        $this->assertTrue($policy->shouldRetry(2, $exception));
        
        // Should reject at and after maxAttempts
        $this->assertFalse($policy->shouldRetry(3, $exception));
        $this->assertFalse($policy->shouldRetry(4, $exception));
    }
    
    /**
     * Test parameter validation - invalid maxAttempts
     */
    public function testInvalidMaxAttempts()
    {
        $this->expectException(\InvalidArgumentException::class);
        new ExponentialBackoffRetryPolicy(0);
    }
    
    /**
     * Test parameter validation - invalid initialBackoff
     */
    public function testInvalidInitialBackoff()
    {
        $this->expectException(\InvalidArgumentException::class);
        new ExponentialBackoffRetryPolicy(3, -1);
    }
    
    /**
     * Test parameter validation - invalid backoffMultiplier
     */
    public function testInvalidBackoffMultiplier()
    {
        $this->expectException(\InvalidArgumentException::class);
        new ExponentialBackoffRetryPolicy(3, 100, 5000, 0.5);
    }
    
    /**
     * Test string representation
     */
    public function testToString()
    {
        $policy = new ExponentialBackoffRetryPolicy(5, 200, 10000, 2.0);
        $str = (string)$policy;
        
        $this->assertStringContainsString('maxAttempts=5', $str);
        $this->assertStringContainsString('initialBackoff=200', $str);
        $this->assertStringContainsString('maxBackoff=10000', $str);
    }
}
