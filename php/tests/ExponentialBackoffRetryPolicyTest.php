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

use PHPUnit\Framework\TestCase;
require_once __DIR__ . '/../autoload.php';
require_once __DIR__ . '/../ExponentialBackoffRetryPolicy.php';

use Apache\Rocketmq\ExponentialBackoffRetryPolicy;

/**
 * Tests for ExponentialBackoffRetryPolicy.
 */
class ExponentialBackoffRetryPolicyTest extends TestCase
{
    public function testDefaultPolicy()
    {
        $policy = new ExponentialBackoffRetryPolicy();
        $this->assertEquals(3, $policy->getMaxAttempts(), "Default max attempts should be 3");

        $delay1 = $policy->getNextDelayMs(1);
        $this->assertEquals(1000, $delay1, "Attempt 1 delay should be base (1000ms)");

        $delay2 = $policy->getNextDelayMs(2);
        $this->assertEquals(2000, $delay2, "Attempt 2 delay should be 2000ms (1000 * 2)");
    }

    public function testExponentialGrowth()
    {
        $policy = new ExponentialBackoffRetryPolicy(5, 100, 10000, 2.0);

        $this->assertEquals(100, $policy->getNextDelayMs(1), "Attempt 1 should be 100ms");
        $this->assertEquals(200, $policy->getNextDelayMs(2), "Attempt 2 should be 200ms");
        $this->assertEquals(400, $policy->getNextDelayMs(3), "Attempt 3 should be 400ms");
        $this->assertEquals(800, $policy->getNextDelayMs(4), "Attempt 4 should be 800ms");
        $this->assertEquals(0, $policy->getNextDelayMs(5), "Attempt 5 should be 0 (at max)");
    }

    public function testMaxDelayCap()
    {
        $policy = new ExponentialBackoffRetryPolicy(10, 1000, 5000, 3.0);

        $delay1 = $policy->getNextDelayMs(1);
        $this->assertEquals(1000, $delay1, "Attempt 1 should be 1000ms");

        $delay2 = $policy->getNextDelayMs(2);
        $this->assertEquals(3000, $delay2, "Attempt 2 should be 3000ms");

        $delay3 = $policy->getNextDelayMs(3);
        $this->assertEquals(5000, $delay3, "Attempt 3 should be capped at 5000ms");
    }

    public function testWithJitter()
    {
        $policy = new ExponentialBackoffRetryPolicy(5, 1000, 10000, 2.0);

        $jitter = $policy->getNextDelayWithJitterMs(1);
        // Jitter range is 50%-100% of base, allow small tolerance for edge cases
        $minExpected = (int)(1000 * 0.49);
        $maxExpected = 1000;
        $this->assertTrue(
            $jitter >= $minExpected && $jitter <= $maxExpected,
            "Jitter should be between 50%% and 100%% of base ({$minExpected}-{$maxExpected}). Got: {$jitter}"
        );
    }

    public function testRejectsNegativeMaxAttempts()
    {
        $this->expectException(\InvalidArgumentException::class);
        new ExponentialBackoffRetryPolicy(0);
    }
}
