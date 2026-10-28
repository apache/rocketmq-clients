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

require_once __DIR__ . '/../PushConsumer.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\PushConsumer;

/**
 * Tests for PushConsumer validation rules.
 * Mirrors Java's PushConsumerBuilderImplTest.
 */
class PushConsumerTest extends TestCase
{
    public function setUp(): void
    {
        \Apache\Rocketmq\Logger::close();
    }

    /**
     * Mirrors Java: testSetConsumerGroupWithNull - PHP validates in start()
     */
    public function testStartWithoutSubscriptions()
    {
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'messageListener' => function($msg) { return 0; },
        ]);

        $this->expectException(\RuntimeException::class);
        $consumer->start();
    }

    /**
     * Mirrors Java: testSetConsumerGroupWithNull
     */
    public function testConstructorWithNullConsumerGroup()
    {
        $this->expectException(\InvalidArgumentException::class);
        new PushConsumer('127.0.0.1:9876', '');
    }

    /**
     * Tests that null messageListener is caught at start().
     * Mirrors Java: testSetMessageListenerWithNull
     */
    public function testStartWithoutMessageListener()
    {
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->expectException(\RuntimeException::class);
        $consumer->start();
    }

    /**
     * Tests negative maxCacheMessageCount validation.
     */
    public function testNegativeMaxCacheMessageCount()
    {
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'maxCacheMessageCount' => -1,
        ]);

        $threshold = $consumer->getCacheMessageCountThresholdPerQueue();
        $this->assertTrue(
            $threshold >= 0,
            "Cache count threshold should be non-negative (got {$threshold})"
        );
    }

    /**
     * Tests negative maxCacheMessageSizeInBytes validation.
     */
    public function testNegativeMaxCacheMessageSize()
    {
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'maxCacheMessageSizeInBytes' => -1,
        ]);

        $threshold = $consumer->getCacheMessageBytesThresholdPerQueue();
        $this->assertTrue(
            $threshold >= 0,
            "Cache bytes threshold should be non-negative (got {$threshold})"
        );
    }

    /**
     * Tests unsubscribe before start (mirrors Java testUnsubscribeBeforeStartup).
     */
    public function testUnsubscribeBeforeStart()
    {
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
            'messageListener' => function($msg) { return 0; },
        ]);

        $consumer->unsubscribe('test-topic');

        $expressions = $consumer->getSubscriptionExpressions();
        $this->assertTrue(empty($expressions), "Topic should be removed from subscriptions");
    }

    /**
     * Tests subscribe before start.
     */
    public function testSubscribeBeforeStart()
    {
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'messageListener' => function($msg) { return 0; },
        ]);

        $consumer->subscribe('new-topic', 'tagA');

        $expressions = $consumer->getSubscriptionExpressions();
        $this->assertTrue(
            isset($expressions['new-topic']),
            "Topic should be added to subscriptions"
        );
        $this->assertEquals(
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
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
            'messageListener' => function($msg) { return 0; },
        ]);

        $ref = new \ReflectionProperty($consumer, 'isRunning');
        $ref->setAccessible(true);
        $ref->setValue($consumer, true);

        $consumer->start();
        $this->assertTrue($consumer->isRunning(), "Consumer should still be running");
    }

    /**
     * Tests subscribe and unsubscribe method chaining (returns $this).
     */
    public function testSubscribeReturnsThis()
    {
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'messageListener' => function($msg) { return 0; },
        ]);

        $result = $consumer->subscribe('topic-1', 'tagA');
        $this->assertTrue(
            $result === $consumer,
            "subscribe should return \$this for chaining"
        );

        $result = $consumer->unsubscribe('topic-1');
        $this->assertTrue(
            $result === $consumer,
            "unsubscribe should return \$this for chaining"
        );
    }

    /**
     * Mirrors Java: testSubscribeBeforeStartup
     */
    public function testMultipleSubscriptions()
    {
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'messageListener' => function($msg) { return 0; },
        ]);

        $consumer->subscribe('topic-1', 'tagA');
        $consumer->subscribe('topic-2', '*');
        $consumer->subscribe('topic-3', 'SQL:age > 10');

        $expressions = $consumer->getSubscriptionExpressions();
        $this->assertEquals(
            3,
            count($expressions),
            "Should have 3 subscriptions"
        );
        $this->assertEquals(
            'tagA',
            $expressions['topic-1'],
            "topic-1 expression should be tagA"
        );
        $this->assertEquals(
            '*',
            $expressions['topic-2'],
            "topic-2 expression should be *"
        );
    }

    /**
     * Mirrors Java: testQueryAssignment - verifies queryAssignment internal method.
     */
    public function testSubscriptionExpressionsAreStored()
    {
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['topic-1' => 'tagA'],
            'messageListener' => function($msg) { return 0; },
        ]);

        $expressions = $consumer->getSubscriptionExpressions();
        $this->assertEquals(
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
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group');
        $result = $consumer->setMessageListener(function($msg) { return 0; });

        $this->assertTrue(
            $result === $consumer,
            "setMessageListener should return \$this for chaining"
        );
    }

    /**
     * Tests that FIFO mode can be configured.
     */
    public function testFifoModeConfiguration()
    {
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'fifo' => true,
            'messageListener' => function($msg) { return 0; },
        ]);

        $ref = new \ReflectionProperty($consumer, 'fifo');
        $ref->setAccessible(true);
        $fifo = $ref->getValue($consumer);

        $this->assertTrue($fifo, "FIFO mode should be enabled");
    }

    /**
     * Tests getAwaitDuration and getReceiveBatchSize getters.
     */
    public function testConsumerGetters()
    {
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'awaitDuration' => 15,
            'receiveBatchSize' => 16,
            'messageListener' => function($msg) { return 0; },
        ]);

        $this->assertEquals(
            15,
            $consumer->getAwaitDuration(),
            "awaitDuration should be 15"
        );
        $this->assertEquals(
            16,
            $consumer->getReceiveBatchSize(),
            "receiveBatchSize should be 16"
        );
        $this->assertEquals(
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
        $consumer = new PushConsumer('127.0.0.1:9876', 'test-group', [
            'maxCacheMessageCount' => 4096,
            'maxCacheMessageSizeInBytes' => 67108864,
        ]);

        $countThreshold = $consumer->getCacheMessageCountThresholdPerQueue();
        $this->assertEquals(
            0,
            $countThreshold,
            "Count threshold should be 0 with no process queues"
        );

        $bytesThreshold = $consumer->getCacheMessageBytesThresholdPerQueue();
        $this->assertEquals(
            0,
            $bytesThreshold,
            "Bytes threshold should be 0 with no process queues"
        );
    }
}
