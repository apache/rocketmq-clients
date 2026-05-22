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

require_once __DIR__ . '/TestRunner.php';
require_once __DIR__ . '/../ExponentialBackoffRetryPolicy.php';
require_once __DIR__ . '/../CustomizedBackoffRetryPolicy.php';
require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ExponentialBackoffRetryPolicy;
use Apache\Rocketmq\CustomizedBackoffRetryPolicy;

class ExponentialBackoffRetryPolicyTest
{
    public function testDefaultPolicy()
    {
        $policy = new ExponentialBackoffRetryPolicy();
        TestRunner::assertEquals(3, $policy->getMaxAttempts(), "Default max attempts should be 3");

        $delay1 = $policy->getNextDelayMs(1);
        TestRunner::assertEquals(1000, $delay1, "Attempt 1 delay should be base (1000ms)");

        $delay2 = $policy->getNextDelayMs(2);
        TestRunner::assertEquals(2000, $delay2, "Attempt 2 delay should be 2000ms (1000 * 2)");
    }

    public function testExponentialGrowth()
    {
        $policy = new ExponentialBackoffRetryPolicy(5, 100, 10000, 2.0);

        TestRunner::assertEquals(100, $policy->getNextDelayMs(1), "Attempt 1 should be 100ms");
        TestRunner::assertEquals(200, $policy->getNextDelayMs(2), "Attempt 2 should be 200ms");
        TestRunner::assertEquals(400, $policy->getNextDelayMs(3), "Attempt 3 should be 400ms");
        TestRunner::assertEquals(800, $policy->getNextDelayMs(4), "Attempt 4 should be 800ms");
        TestRunner::assertEquals(0, $policy->getNextDelayMs(5), "Attempt 5 should be 0 (at max)");
    }

    public function testMaxDelayCap()
    {
        $policy = new ExponentialBackoffRetryPolicy(10, 1000, 5000, 3.0);

        $delay1 = $policy->getNextDelayMs(1);
        TestRunner::assertEquals(1000, $delay1, "Attempt 1 should be 1000ms");

        $delay2 = $policy->getNextDelayMs(2);
        TestRunner::assertEquals(3000, $delay2, "Attempt 2 should be 3000ms");

        $delay3 = $policy->getNextDelayMs(3);
        TestRunner::assertEquals(5000, $delay3, "Attempt 3 should be capped at 5000ms");
    }

    public function testWithJitter()
    {
        $policy = new ExponentialBackoffRetryPolicy(5, 1000, 10000, 2.0);

        $jitter = $policy->getNextDelayWithJitterMs(1);
        // Jitter range is 50%-100% of base, allow small tolerance for edge cases
        $minExpected = (int)(1000 * 0.49);
        $maxExpected = 1000;
        TestRunner::assertTrue(
            $jitter >= $minExpected && $jitter <= $maxExpected,
            "Jitter should be between 50%% and 100%% of base ({$minExpected}-{$maxExpected}). Got: {$jitter}"
        );
    }

    public function testRejectsNegativeMaxAttempts()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new ExponentialBackoffRetryPolicy(0);
        }, "Zero max attempts should be rejected");
    }
}

class CustomizedBackoffRetryPolicyTest
{
    public function testCustomDelays()
    {
        $policy = new CustomizedBackoffRetryPolicy(4, [1000, 5000, 10000]);

        TestRunner::assertEquals(4, $policy->getMaxAttempts(), "Max attempts should be 4");
        TestRunner::assertEquals(1000, $policy->getNextDelayMs(1), "Attempt 1 should be 1000ms");
        TestRunner::assertEquals(5000, $policy->getNextDelayMs(2), "Attempt 2 should be 5000ms");
        TestRunner::assertEquals(10000, $policy->getNextDelayMs(3), "Attempt 3 should be 10000ms");
        TestRunner::assertEquals(0, $policy->getNextDelayMs(4), "Attempt 4 should be 0 (at max)");
    }

    public function testCyclesThroughDelays()
    {
        $policy = new CustomizedBackoffRetryPolicy(10, [100, 200]);

        TestRunner::assertEquals(100, $policy->getNextDelayMs(1), "Attempt 1 should be 100ms");
        TestRunner::assertEquals(200, $policy->getNextDelayMs(2), "Attempt 2 should be 200ms");
        TestRunner::assertEquals(100, $policy->getNextDelayMs(3), "Attempt 3 should cycle back to 100ms");
        TestRunner::assertEquals(200, $policy->getNextDelayMs(4), "Attempt 4 should be 200ms");
    }

    public function testEmptyDelays()
    {
        $policy = new CustomizedBackoffRetryPolicy(3, []);

        TestRunner::assertEquals(0, $policy->getNextDelayMs(1), "Empty delays should return 0");
    }

    public function testRejectsNegativeMaxAttempts()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new CustomizedBackoffRetryPolicy(0);
        }, "Zero max attempts should be rejected");
    }

    /**
     * Mirrors Java: testFromProtobuf
     */
    public function testFromProtobuf()
    {
        $duration0 = new \Google\Protobuf\Duration();
        $duration0->setSeconds(1);
        $duration1 = new \Google\Protobuf\Duration();
        $duration1->setSeconds(2);
        $duration2 = new \Google\Protobuf\Duration();
        $duration2->setSeconds(3);

        $customizedBackoff = new \Apache\Rocketmq\V2\CustomizedBackoff();
        $customizedBackoff->setNext([$duration0, $duration1, $duration2]);

        $retryPolicyPb = new \Apache\Rocketmq\V2\RetryPolicy();
        $retryPolicyPb->setMaxAttempts(3);
        $retryPolicyPb->setCustomizedBackoff($customizedBackoff);

        $policy = CustomizedBackoffRetryPolicy::fromProtobuf($retryPolicyPb);

        TestRunner::assertEquals(3, $policy->getMaxAttempts(), "Max attempts should be 3");
        $durations = $policy->getDurations();
        TestRunner::assertEquals(3, count($durations), "Should have 3 durations");
        TestRunner::assertEquals(1000, $durations[0], "First duration should be 1000ms");
        TestRunner::assertEquals(2000, $durations[1], "Second duration should be 2000ms");
        TestRunner::assertEquals(3000, $durations[2], "Third duration should be 3000ms");
    }

    /**
     * Mirrors Java: testFromProtobufWithoutCustomizedBackoff
     */
    public function testFromProtobufWithoutCustomizedBackoff()
    {
        $exponentialBackoff = new \Apache\Rocketmq\V2\ExponentialBackoff();
        $retryPolicyPb = new \Apache\Rocketmq\V2\RetryPolicy();
        $retryPolicyPb->setMaxAttempts(3);
        $retryPolicyPb->setExponentialBackoff($exponentialBackoff);

        TestRunner::assertThrows(\InvalidArgumentException::class, function() use ($retryPolicyPb) {
            CustomizedBackoffRetryPolicy::fromProtobuf($retryPolicyPb);
        }, "Exponential backoff should throw when converted to Customized");
    }

    /**
     * Mirrors Java: testToProtobuf
     */
    public function testToProtobuf()
    {
        $policy = new CustomizedBackoffRetryPolicy(3, [1000, 2000]);
        $protobuf = $policy->toProtobuf();

        TestRunner::assertEquals(3, $protobuf->getMaxAttempts(), "Max attempts should be 3");
        TestRunner::assertTrue(
            $protobuf->hasCustomizedBackoff(),
            "Should have customized backoff"
        );

        $backoff = $protobuf->getCustomizedBackoff();
        $nextList = iterator_to_array($backoff->getNext());
        TestRunner::assertEquals(2, count($nextList), "Should have 2 durations");

        $d0Secs = $nextList[0]->getSeconds();
        $d1Secs = $nextList[1]->getSeconds();
        TestRunner::assertEquals(1, $d0Secs, "First duration should be 1s");
        TestRunner::assertEquals(2, $d1Secs, "Second duration should be 2s");
    }

    /**
     * Mirrors Java: testInheritBackoff
     */
    public function testInheritBackoff()
    {
        // Server-side durations: 1s, 2s, 3s
        $duration0 = new \Google\Protobuf\Duration();
        $duration0->setSeconds(1);
        $duration1 = new \Google\Protobuf\Duration();
        $duration1->setSeconds(2);
        $duration2 = new \Google\Protobuf\Duration();
        $duration2->setSeconds(3);

        $serverBackoff = new \Apache\Rocketmq\V2\CustomizedBackoff();
        $serverBackoff->setNext([$duration0, $duration1, $duration2]);

        $serverPolicyPb = new \Apache\Rocketmq\V2\RetryPolicy();
        $serverPolicyPb->setMaxAttempts(5);
        $serverPolicyPb->setCustomizedBackoff($serverBackoff);

        // Client-side: different delays
        $clientPolicy = new CustomizedBackoffRetryPolicy(3, [3000, 2000, 1000]);
        $inherited = $clientPolicy->inheritBackoff($serverPolicyPb);

        TestRunner::assertEquals(
            3,
            $inherited->getMaxAttempts(),
            "Should keep own maxAttempts (3)"
        );
        $inheritedDurations = $inherited->getDurations();
        TestRunner::assertEquals(3, count($inheritedDurations), "Should have 3 inherited durations");
        TestRunner::assertEquals(1000, $inheritedDurations[0], "First should be server's 1000ms");
        TestRunner::assertEquals(2000, $inheritedDurations[1], "Second should be server's 2000ms");
        TestRunner::assertEquals(3000, $inheritedDurations[2], "Third should be server's 3000ms");
    }

    /**
     * Mirrors Java: testInheritBackoffWithoutCustomizedBackoff
     */
    public function testInheritBackoffWithoutCustomizedBackoff()
    {
        $exponentialBackoff = new \Apache\Rocketmq\V2\ExponentialBackoff();
        $serverPolicyPb = new \Apache\Rocketmq\V2\RetryPolicy();
        $serverPolicyPb->setExponentialBackoff($exponentialBackoff);

        $clientPolicy = new CustomizedBackoffRetryPolicy(3, [3000, 2000, 1000]);

        TestRunner::assertThrows(\InvalidArgumentException::class, function() use ($clientPolicy, $serverPolicyPb) {
            $clientPolicy->inheritBackoff($serverPolicyPb);
        }, "Inheriting from exponential backoff should throw");
    }
}

TestRunner::run(new ExponentialBackoffRetryPolicyTest());
TestRunner::run(new CustomizedBackoffRetryPolicyTest());
