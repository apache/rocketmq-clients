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
namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\ExponentialBackoffRetryPolicy;
use Apache\Rocketmq\CustomizedBackoffRetryPolicy;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../../ExponentialBackoffRetryPolicy.php';
require_once __DIR__ . '/../../CustomizedBackoffRetryPolicy.php';

class RetryErrorHandlingIntegrationTest extends IntegrationTestCase
{
    public function testExponentialBackoffRetryPolicy()
    {
        $policy = new ExponentialBackoffRetryPolicy(3, 1000, 30000, 2.0);

        $delay1 = $policy->getNextDelayMs(1);
        $delay2 = $policy->getNextDelayMs(2);
        $delay3 = $policy->getNextDelayMs(3);

        // With maxAttempts=3, attempt 3 returns 0 (cap hit)
        // attempt 1: 1000 * 2^0 = 1000
        // attempt 2: 1000 * 2^1 = 2000
        $this->assertGreaterThan(0, $delay1);
        $this->assertGreaterThan(0, $delay2);
        // With exponent growth, delay should increase
        $this->assertGreaterThan($delay1, $delay2);
        // attempt 3 >= maxAttempts, returns 0
        $this->assertEquals(0, $delay3);
    }

    public function testExponentialBackoffMaxCap()
    {
        // maxDelayMs=5000 will cap the exponential growth
        $policy = new ExponentialBackoffRetryPolicy(10, 1000, 5000, 2.0);

        // attempt 5: 1000 * 2^4 = 16000, capped at 5000
        $delay = $policy->getNextDelayMs(5);
        $this->assertLessThanOrEqual(5000, $delay);
        $this->assertGreaterThan(0, $delay);
    }

    public function testCustomizedBackoffRetryPolicy()
    {
        $delays = [100, 500, 2000, 10000];
        $policy = new CustomizedBackoffRetryPolicy(5, $delays);

        // getNextDelayMs returns delays from the configured sequence
        $this->assertEquals(100, $policy->getNextDelayMs(1));
        $this->assertEquals(500, $policy->getNextDelayMs(2));
        $this->assertEquals(2000, $policy->getNextDelayMs(3));
        $this->assertEquals(10000, $policy->getNextDelayMs(4));
    }

    public function testCustomizedBackoffExceedsArray()
    {
        $delays = [100, 500];
        // maxAttempts must be > 5 so attempt 5 doesn't return 0
        $policy = new CustomizedBackoffRetryPolicy(6, $delays);

        // Attempt 5 exceeds the delays array length, so last value is returned
        $delay = $policy->getNextDelayMs(5);
        $this->assertEquals(500, $delay);

        // Attempt 6 >= maxAttempts, returns 0
        $delay2 = $policy->getNextDelayMs(6);
        $this->assertEquals(0, $delay2);
    }
}
