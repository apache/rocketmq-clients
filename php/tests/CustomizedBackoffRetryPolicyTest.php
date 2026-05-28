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
require_once __DIR__ . '/../CustomizedBackoffRetryPolicy.php';
require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\CustomizedBackoffRetryPolicy;

/**
 * Tests for CustomizedBackoffRetryPolicy.
 */
class CustomizedBackoffRetryPolicyTest extends TestCase
{
    public function testCustomDelays()
    {
        $policy = new CustomizedBackoffRetryPolicy(4, [1000, 5000, 10000]);

        $this->assertEquals(4, $policy->getMaxAttempts(), "Max attempts should be 4");
        $this->assertEquals(1000, $policy->getNextDelayMs(1), "Attempt 1 should be 1000ms");
        $this->assertEquals(5000, $policy->getNextDelayMs(2), "Attempt 2 should be 5000ms");
        $this->assertEquals(10000, $policy->getNextDelayMs(3), "Attempt 3 should be 10000ms");
        $this->assertEquals(0, $policy->getNextDelayMs(4), "Attempt 4 should be 0 (at max)");
    }

    public function testCyclesThroughDelays()
    {
        $policy = new CustomizedBackoffRetryPolicy(10, [100, 200]);

        $this->assertEquals(100, $policy->getNextDelayMs(1), "Attempt 1 should be 100ms");
        $this->assertEquals(200, $policy->getNextDelayMs(2), "Attempt 2 should be 200ms");
        $this->assertEquals(200, $policy->getNextDelayMs(3), "Attempt 3 should clamp to last delay (200ms)");
        $this->assertEquals(200, $policy->getNextDelayMs(4), "Attempt 4 should clamp to last delay (200ms)");
    }

    public function testEmptyDelays()
    {
        $policy = new CustomizedBackoffRetryPolicy(3, []);

        $this->assertEquals(0, $policy->getNextDelayMs(1), "Empty delays should return 0");
    }

    public function testRejectsNegativeMaxAttempts()
    {
        $this->expectException(\InvalidArgumentException::class);
        new CustomizedBackoffRetryPolicy(0);
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

        $this->assertEquals(3, $policy->getMaxAttempts(), "Max attempts should be 3");
        $durations = $policy->getDurations();
        $this->assertEquals(3, count($durations), "Should have 3 durations");
        $this->assertEquals(1000, $durations[0], "First duration should be 1000ms");
        $this->assertEquals(2000, $durations[1], "Second duration should be 2000ms");
        $this->assertEquals(3000, $durations[2], "Third duration should be 3000ms");
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

        $this->expectException(\InvalidArgumentException::class);
        CustomizedBackoffRetryPolicy::fromProtobuf($retryPolicyPb);
    }

    /**
     * Mirrors Java: testToProtobuf
     */
    public function testToProtobuf()
    {
        $policy = new CustomizedBackoffRetryPolicy(3, [1000, 2000]);
        $protobuf = $policy->toProtobuf();

        $this->assertEquals(3, $protobuf->getMaxAttempts(), "Max attempts should be 3");
        $this->assertTrue(
            $protobuf->hasCustomizedBackoff(),
            "Should have customized backoff"
        );

        $backoff = $protobuf->getCustomizedBackoff();
        $nextList = iterator_to_array($backoff->getNext());
        $this->assertEquals(2, count($nextList), "Should have 2 durations");

        $d0Secs = $nextList[0]->getSeconds();
        $d1Secs = $nextList[1]->getSeconds();
        $this->assertEquals(1, $d0Secs, "First duration should be 1s");
        $this->assertEquals(2, $d1Secs, "Second duration should be 2s");
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

        $this->assertEquals(
            3,
            $inherited->getMaxAttempts(),
            "Should keep own maxAttempts (3)"
        );
        $inheritedDurations = $inherited->getDurations();
        $this->assertEquals(3, count($inheritedDurations), "Should have 3 inherited durations");
        $this->assertEquals(1000, $inheritedDurations[0], "First should be server's 1000ms");
        $this->assertEquals(2000, $inheritedDurations[1], "Second should be server's 2000ms");
        $this->assertEquals(3000, $inheritedDurations[2], "Third should be server's 3000ms");
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

        $this->expectException(\InvalidArgumentException::class);
        $clientPolicy->inheritBackoff($serverPolicyPb);
    }
}
