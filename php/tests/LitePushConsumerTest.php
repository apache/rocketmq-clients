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
require_once __DIR__ . '/../LitePushConsumer.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\LitePushConsumer;

/**
 * Tests for LitePushConsumer validation rules.
 * Mirrors Java's LitePushConsumerBuilderImplTest.
 */
class LitePushConsumerTest
{
    /**
     * Mirrors Java: testSetConsumerGroupWithNull
     */
    public function testConstructorWithNullConsumerGroup()
    {
        \Apache\Rocketmq\Logger::close();

        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new LitePushConsumer('127.0.0.1:9876', '', 'parent-topic');
        }, "Empty consumer group should throw");
    }

    /**
     * Mirrors Java: testBindTopicWithNull
     */
    public function testConstructorWithNullParentTopic()
    {
        \Apache\Rocketmq\Logger::close();

        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new LitePushConsumer('127.0.0.1:9876', 'test-group', '');
        }, "Empty parent topic should throw");
    }

    /**
     * Mirrors Java: testSetMessageListenerWithNull
     * PHP: start() without messageListener should throw.
     */
    public function testStartWithoutMessageListener()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', []);

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->start();
        }, "Start without message listener should throw");
    }

    /**
     * Mirrors Java: testSetNegativeMaxCacheMessageCount
     */
    public function testNegativeMaxCacheMessageCount()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'maxCacheMessageCount' => -1,
            'messageListener' => function($msg) { return 0; },
        ]);

        $threshold = $consumer->getCacheMessageCountThresholdPerQueue();
        TestRunner::assertTrue(
            $threshold >= 0,
            "Cache count threshold should be non-negative (got {$threshold})"
        );
    }

    /**
     * Mirrors Java: testSetNegativeMaxCacheMessageSizeInBytes
     */
    public function testNegativeMaxCacheMessageSizeInBytes()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'maxCacheMessageSizeInBytes' => -1,
            'messageListener' => function($msg) { return 0; },
        ]);

        $threshold = $consumer->getCacheMessageBytesThresholdPerQueue();
        TestRunner::assertTrue(
            $threshold >= 0,
            "Cache bytes threshold should be non-negative (got {$threshold})"
        );
    }

    /**
     * Mirrors Java: testBuildWithoutClientConfiguration
     * PHP: LitePushConsumer without lite topics should throw at start.
     */
    public function testStartWithoutLiteTopics()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'messageListener' => function($msg) { return 0; },
        ]);

        // No lite topics subscribed - should throw
        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->start();
        }, "Start without lite topics should throw");
    }

    /**
     * Tests that subscribeLite works before start.
     */
    public function testSubscribeLiteBeforeStart()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'messageListener' => function($msg) { return 0; },
        ]);

        $consumer->subscribeLite('lite-topic-1', function($msg) { return 0; });
        $consumer->subscribeLite('lite-topic-2');

        $topics = $consumer->getLiteTopics();
        TestRunner::assertEqualsWithMessage(
            2,
            count($topics),
            "Should have 2 lite topics"
        );
        TestRunner::assertTrue(
            in_array('lite-topic-1', $topics),
            "lite-topic-1 should be in topics"
        );
        TestRunner::assertTrue(
            in_array('lite-topic-2', $topics),
            "lite-topic-2 should be in topics"
        );
    }

    /**
     * Tests unsubscribeLite before start.
     */
    public function testUnsubscribeLiteBeforeStart()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'messageListener' => function($msg) { return 0; },
        ]);

        $consumer->subscribeLite('lite-topic', function($msg) { return 0; });
        $consumer->unsubscribeLite('lite-topic');

        $topics = $consumer->getLiteTopics();
        TestRunner::assertTrue(
            empty($topics),
            "Lite topics should be empty after unsubscribe"
        );
    }

    /**
     * Tests that lite topic name length is validated.
     */
    public function testLiteTopicNameTooLong()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'maxLiteTopicSize' => 10,
            'messageListener' => function($msg) { return 0; },
        ]);

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->subscribeLite('this-is-a-very-long-lite-topic-name', function($msg) { return 0; });
        }, "Long lite topic name should throw");
    }

    /**
     * Tests isLiteConsumer returns true.
     */
    public function testIsLiteConsumer()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'messageListener' => function($msg) { return 0; },
        ]);

        TestRunner::assertTrue(
            $consumer->isLiteConsumer(),
            "LitePushConsumer should return true for isLiteConsumer"
        );
    }
}

echo "=== LitePushConsumerTest ===\n";
$test = new LitePushConsumerTest();
$test->testConstructorWithNullConsumerGroup();
echo "  [OK] testConstructorWithNullConsumerGroup\n";
$test->testConstructorWithNullParentTopic();
echo "  [OK] testConstructorWithNullParentTopic\n";
$test->testStartWithoutMessageListener();
echo "  [OK] testStartWithoutMessageListener\n";
$test->testNegativeMaxCacheMessageCount();
echo "  [OK] testNegativeMaxCacheMessageCount\n";
$test->testNegativeMaxCacheMessageSizeInBytes();
echo "  [OK] testNegativeMaxCacheMessageSizeInBytes\n";
$test->testStartWithoutLiteTopics();
echo "  [OK] testStartWithoutLiteTopics\n";
$test->testSubscribeLiteBeforeStart();
echo "  [OK] testSubscribeLiteBeforeStart\n";
$test->testUnsubscribeLiteBeforeStart();
echo "  [OK] testUnsubscribeLiteBeforeStart\n";
$test->testLiteTopicNameTooLong();
echo "  [OK] testLiteTopicNameTooLong\n";
$test->testIsLiteConsumer();
echo "  [OK] testIsLiteConsumer\n";
