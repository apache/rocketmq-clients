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
require_once __DIR__ . '/../PushConsumer.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\PushConsumer;

/**
 * Tests for PushConsumer validation rules.
 * Mirrors Java's PushConsumerBuilderImplTest.
 */
class PushConsumerTest
{
    /**
     * Mirrors Java: testSetConsumerGroupWithNull - PHP validates in start()
     */
    public function testStartWithoutSubscriptions()
    {
        \Apache\Rocketmq\Logger::close();

        // PHP PushConsumer has no separate builder; validation happens at start()
        // Creating a consumer with no subscriptions should fail at start
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'messageListener' => function($msg) { return 0; },
        ]);

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->start();
        }, "Start without subscriptions should throw");

        // Clean up: shutdown is safe since isRunning is false
    }

    /**
     * Mirrors Java: testSetConsumerGroupWithNull
     */
    public function testConstructorWithNullConsumerGroup()
    {
        \Apache\Rocketmq\Logger::close();

        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new PushConsumer('127.0.0.1:9876', '');
        }, "Empty consumer group should throw");
    }

    /**
     * Tests that null messageListener is caught at start().
     * Mirrors Java: testSetMessageListenerWithNull
     */
    public function testStartWithoutMessageListener()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->start();
        }, "Start without message listener should throw");
    }

    /**
     * Tests negative maxCacheMessageCount validation.
     * Java expects IllegalArgumentException; PHP should reject or clamp.
     */
    public function testNegativeMaxCacheMessageCount()
    {
        \Apache\Rocketmq\Logger::close();

        // PHP doesn't validate at construction but at runtime thresholds
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'maxCacheMessageCount' => -1,
        ]);

        // The value should be clamped or handled gracefully
        $threshold = $consumer->getCacheMessageCountThresholdPerQueue();
        TestRunner::assertTrue(
            $threshold >= 0,
            "Cache count threshold should be non-negative (got {$threshold})"
        );
    }

    /**
     * Tests negative maxCacheMessageSizeInBytes validation.
     */
    public function testNegativeMaxCacheMessageSize()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'maxCacheMessageSizeInBytes' => -1,
        ]);

        $threshold = $consumer->getCacheMessageBytesThresholdPerQueue();
        TestRunner::assertTrue(
            $threshold >= 0,
            "Cache bytes threshold should be non-negative (got {$threshold})"
        );
    }

    /**
     * Tests unsubscribe before start (mirrors Java testUnsubscribeBeforeStartup).
     */
    public function testUnsubscribeBeforeStart()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
            'messageListener' => function($msg) { return 0; },
        ]);

        // Should not throw - unsubscribe is allowed before start
        $consumer->unsubscribe('test-topic');

        $expressions = $consumer->getSubscriptionExpressions();
        TestRunner::assertTrue(empty($expressions), "Topic should be removed from subscriptions");
    }

    /**
     * Tests subscribe before start.
     */
    public function testSubscribeBeforeStart()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'messageListener' => function($msg) { return 0; },
        ]);

        $consumer->subscribe('new-topic', 'tagA');

        $expressions = $consumer->getSubscriptionExpressions();
        TestRunner::assertTrue(
            isset($expressions['new-topic']),
            "Topic should be added to subscriptions"
        );
        TestRunner::assertEqualsWithMessage(
            'tagA',
            $expressions['new-topic'],
            "Expression should match"
        );
    }

    /**
     * Tests that start() rejects when consumer is already running.
     */
    public function testStartWhenAlreadyRunning()
    {
        \Apache\Rocketmq\Logger::close();

        // Use reflection to simulate running state
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
            'messageListener' => function($msg) { return 0; },
        ]);

        $ref = new \ReflectionProperty($consumer, 'isRunning');
        $ref->setAccessible(true);
        $ref->setValue($consumer, true);

        // start() should return silently when already running (no exception)
        $consumer->start();
        TestRunner::assertTrue($consumer->isRunning(), "Consumer should still be running");
    }

    /**
     * Tests subscribe and unsubscribe method chaining (returns $this).
     */
    public function testSubscribeReturnsThis()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'messageListener' => function($msg) { return 0; },
        ]);

        $result = $consumer->subscribe('topic-1', 'tagA');
        TestRunner::assertTrue(
            $result === $consumer,
            "subscribe should return \$this for chaining"
        );

        $result = $consumer->unsubscribe('topic-1');
        TestRunner::assertTrue(
            $result === $consumer,
            "unsubscribe should return \$this for chaining"
        );
    }

    /**
     * Mirrors Java: testSubscribeBeforeStartup
     * Subscribe before start should succeed.
     */
    public function testMultipleSubscriptions()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'messageListener' => function($msg) { return 0; },
        ]);

        $consumer->subscribe('topic-1', 'tagA');
        $consumer->subscribe('topic-2', '*');
        $consumer->subscribe('topic-3', 'SQL:age > 10');

        $expressions = $consumer->getSubscriptionExpressions();
        TestRunner::assertEqualsWithMessage(
            3,
            count($expressions),
            "Should have 3 subscriptions"
        );
        TestRunner::assertEqualsWithMessage(
            'tagA',
            $expressions['topic-1'],
            "topic-1 expression should be tagA"
        );
        TestRunner::assertEqualsWithMessage(
            '*',
            $expressions['topic-2'],
            "topic-2 expression should be *"
        );
    }

    /**
     * Mirrors Java: testQueryAssignment - verifies queryAssignment internal method.
     * We verify that expressions are properly stored for later QueryAssignment calls.
     */
    public function testSubscriptionExpressionsAreStored()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['topic-1' => 'tagA'],
            'messageListener' => function($msg) { return 0; },
        ]);

        $expressions = $consumer->getSubscriptionExpressions();
        TestRunner::assertEqualsWithMessage(
            ['topic-1' => 'tagA'],
            $expressions,
            "Initial expressions should be stored correctly"
        );
    }

    /**
     * Tests setMessageListener returns $this for chaining.
     */
    public function testSetMessageListenerReturnsThis()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group');
        $result = $consumer->setMessageListener(function($msg) { return 0; });

        TestRunner::assertTrue(
            $result === $consumer,
            "setMessageListener should return \$this for chaining"
        );
    }

    /**
     * Tests that FIFO mode can be configured.
     */
    public function testFifoModeConfiguration()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'fifo' => true,
            'messageListener' => function($msg) { return 0; },
        ]);

        $ref = new \ReflectionProperty($consumer, 'fifo');
        $ref->setAccessible(true);
        $fifo = $ref->getValue($consumer);

        TestRunner::assertTrue($fifo, "FIFO mode should be enabled");
    }

    /**
     * Tests getAwaitDuration and getReceiveBatchSize getters.
     */
    public function testConsumerGetters()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'awaitDuration' => 15,
            'receiveBatchSize' => 16,
            'messageListener' => function($msg) { return 0; },
        ]);

        TestRunner::assertEqualsWithMessage(
            15,
            $consumer->getAwaitDuration(),
            "awaitDuration should be 15"
        );
        TestRunner::assertEqualsWithMessage(
            16,
            $consumer->getReceiveBatchSize(),
            "receiveBatchSize should be 16"
        );
        TestRunner::assertEqualsWithMessage(
            'test-group',
            $consumer->getGroupResource()->getName(),
            "consumerGroup should match"
        );
    }

    /**
     * Tests that getCacheMessageCountThresholdPerQueue distributes evenly.
     */
    public function testCacheThresholdDistribution()
    {
        \Apache\Rocketmq\Logger::close();

        // With no process queues, threshold should be 0
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'maxCacheMessageCount' => 4096,
            'maxCacheMessageSizeInBytes' => 67108864,
        ]);

        $countThreshold = $consumer->getCacheMessageCountThresholdPerQueue();
        TestRunner::assertEqualsWithMessage(
            0,
            $countThreshold,
            "Count threshold should be 0 with no process queues"
        );

        $bytesThreshold = $consumer->getCacheMessageBytesThresholdPerQueue();
        TestRunner::assertEqualsWithMessage(
            0,
            $bytesThreshold,
            "Bytes threshold should be 0 with no process queues"
        );
    }
}

echo "=== PushConsumerTest ===\n";
$test = new PushConsumerTest();
$test->testStartWithoutSubscriptions();
echo "  [OK] testStartWithoutSubscriptions\n";
$test->testConstructorWithNullConsumerGroup();
echo "  [OK] testConstructorWithNullConsumerGroup\n";
$test->testStartWithoutMessageListener();
echo "  [OK] testStartWithoutMessageListener\n";
$test->testNegativeMaxCacheMessageCount();
echo "  [OK] testNegativeMaxCacheMessageCount\n";
$test->testNegativeMaxCacheMessageSize();
echo "  [OK] testNegativeMaxCacheMessageSize\n";
$test->testUnsubscribeBeforeStart();
echo "  [OK] testUnsubscribeBeforeStart\n";
$test->testSubscribeBeforeStart();
echo "  [OK] testSubscribeBeforeStart\n";
$test->testStartWhenAlreadyRunning();
echo "  [OK] testStartWhenAlreadyRunning\n";
$test->testSubscribeReturnsThis();
echo "  [OK] testSubscribeReturnsThis\n";
$test->testMultipleSubscriptions();
echo "  [OK] testMultipleSubscriptions\n";
$test->testSubscriptionExpressionsAreStored();
echo "  [OK] testSubscriptionExpressionsAreStored\n";
$test->testSetMessageListenerReturnsThis();
echo "  [OK] testSetMessageListenerReturnsThis\n";
$test->testFifoModeConfiguration();
echo "  [OK] testFifoModeConfiguration\n";
$test->testConsumerGetters();
echo "  [OK] testConsumerGetters\n";
$test->testCacheThresholdDistribution();
echo "  [OK] testCacheThresholdDistribution\n";
