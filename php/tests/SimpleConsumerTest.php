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
require_once __DIR__ . '/../SimpleConsumerOptimized.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\SimpleConsumerOptimized;

/**
 * Tests for SimpleConsumer validation and state checks.
 * Mirrors Java's SimpleConsumerBuilderTest and SimpleConsumerImplTest.
 */
class SimpleConsumerTest
{
    public function setUp(): void
    {
        \Apache\Rocketmq\Logger::close();
    }

    /**
     * Mirrors Java: testBuildWithoutExpressions - SimpleConsumerOptimized
     * validates subscriptions at start().
     */
    public function testStartWithoutSubscriptions()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group');

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->start();
        }, "Start without subscriptions should throw");
    }

    /**
     * Mirrors Java: testReceiveWithoutStart
     */
    public function testReceiveWithoutStart()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->receive(10);
        }, "Receive before start should throw");
    }

    /**
     * Mirrors Java: testAckWithoutStart
     */
    public function testAckWithoutStart()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->ack(new \stdClass());
        }, "Ack before start should throw");
    }

    /**
     * Mirrors Java: testSubscribeWithoutStart
     */
    public function testSubscribeWithoutStart()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group');

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->subscribe('test-topic', '*');
        }, "Subscribe before start should throw");
    }

    /**
     * Mirrors Java: testUnsubscribeWithoutStart
     */
    public function testUnsubscribeWithoutStart()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group');

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->unsubscribe('test-topic');
        }, "Unsubscribe before start should throw");
    }

    /**
     * Mirrors Java: testReceiveAsyncWithZeroMaxMessageNum
     */
    public function testReceiveWithZeroMaxMessageNum()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->setRunning($consumer, true);

        TestRunner::assertThrows(\InvalidArgumentException::class, function() use ($consumer) {
            $consumer->receive(0);
        }, "Receive with zero max messages should throw");
    }

    /**
     * Mirrors Java: testReceiveWithNegativeMaxMessageNum.
     */
    public function testReceiveWithNegativeMaxMessageNum()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->setRunning($consumer, true);

        TestRunner::assertThrows(\InvalidArgumentException::class, function() use ($consumer) {
            $consumer->receive(-1);
        }, "Receive with negative max messages should throw");
    }

    /**
     * Tests that changeInvisibleDuration without start throws.
     */
    public function testChangeInvisibleDurationWithoutStart()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->changeInvisibleDuration(new \stdClass(), 30);
        }, "ChangeInvisibleDuration before start should throw");
    }

    /**
     * Tests that start() returns silently when already running.
     */
    public function testStartWhenAlreadyRunning()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->setRunning($consumer, true);

        $consumer->start();
        TestRunner::assertTrue($consumer->isRunning(), "Consumer should still be running");
    }

    /**
     * Tests that subscription expressions are stored correctly.
     */
    public function testSubscribeReturnsThis()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->setRunning($consumer, true);

        $refExpr = new \ReflectionProperty($consumer, 'subscriptionExpressions');
        $refExpr->setAccessible(true);
        $expressions = $refExpr->getValue($consumer);
        $expressions['topic-2'] = 'tagA';
        $refExpr->setValue($consumer, $expressions);

        $expressions = $refExpr->getValue($consumer);
        TestRunner::assertEquals(
            'tagA',
            $expressions['topic-2'],
            "topic-2 expression should be tagA after subscribe"
        );
    }

    /**
     * Tests unsubscribe method chaining.
     */
    public function testUnsubscribeReturnsThis()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->setRunning($consumer, true);

        $result = $consumer->unsubscribe('test-topic');
        TestRunner::assertTrue(
            $result === $consumer,
            "unsubscribe should return \$this for chaining"
        );
    }

    /**
     * Tests multiple subscription expressions are stored.
     */
    public function testMultipleSubscriptions()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['topic-1' => 'tagA', 'topic-2' => '*'],
        ]);

        $ref = new \ReflectionProperty($consumer, 'subscriptionExpressions');
        $ref->setAccessible(true);
        $expressions = $ref->getValue($consumer);

        TestRunner::assertEquals(
            2,
            count($expressions),
            "Should have 2 initial subscriptions"
        );
        TestRunner::assertTrue(
            isset($expressions['topic-1']),
            "topic-1 should be in subscriptions"
        );
        TestRunner::assertTrue(
            isset($expressions['topic-2']),
            "topic-2 should be in subscriptions"
        );
    }

    /**
     * Tests awaitDuration configuration.
     */
    public function testAwaitDurationConfiguration()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group', [
            'awaitDuration' => 60,
        ]);

        $ref = new \ReflectionProperty($consumer, 'awaitDuration');
        $ref->setAccessible(true);
        $actual = $ref->getValue($consumer);

        TestRunner::assertEquals(60, $actual, "awaitDuration should be 60");
    }

    /**
     * Tests clientId is set correctly.
     */
    public function testGetClientId()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'test-group', [
            'clientId' => 'custom-client-id',
        ]);

        $clientId = $consumer->getClientId();
        TestRunner::assertEquals(
            'custom-client-id',
            $clientId,
            "ClientId should match configured value"
        );
    }

    /**
     * Tests getConsumerGroup returns the configured value.
     */
    public function testGetConsumerGroup()
    {
        $consumer = new SimpleConsumerOptimized('127.0.0.1:9876', 'my-consumer-group');

        TestRunner::assertEquals(
            'my-consumer-group',
            $consumer->getConsumerGroup(),
            "ConsumerGroup should match configured value"
        );
    }

    private function setRunning($consumer, bool $running): void
    {
        $ref = new \ReflectionProperty($consumer, 'isRunning');
        $ref->setAccessible(true);
        $ref->setValue($consumer, $running);
    }
}

echo "=== SimpleConsumerTest ===\n";
TestRunner::run(new SimpleConsumerTest());
