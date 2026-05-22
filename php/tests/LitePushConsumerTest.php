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
    public function setUp(): void
    {
        \Apache\Rocketmq\Logger::close();
    }

    /**
     * Mirrors Java: testSetConsumerGroupWithNull
     */
    public function testConstructorWithNullConsumerGroup()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new LitePushConsumer('127.0.0.1:9876', '', 'parent-topic');
        }, "Empty consumer group should throw");
    }

    /**
     * Mirrors Java: testBindTopicWithNull
     */
    public function testConstructorWithNullParentTopic()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new LitePushConsumer('127.0.0.1:9876', 'test-group', '');
        }, "Empty parent topic should throw");
    }

    /**
     * Mirrors Java: testSetMessageListenerWithNull
     */
    public function testStartWithoutMessageListener()
    {
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
     */
    public function testStartWithoutLiteTopics()
    {
        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'messageListener' => function($msg) { return 0; },
        ]);

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->start();
        }, "Start without lite topics should throw");
    }

    /**
     * Tests that subscribeLite works before start.
     */
    public function testSubscribeLiteBeforeStart()
    {
        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'messageListener' => function($msg) { return 0; },
        ]);

        $consumer->subscribeLite('lite-topic-1', function($msg) { return 0; });
        $consumer->subscribeLite('lite-topic-2');

        $topics = $consumer->getLiteTopics();
        TestRunner::assertEquals(
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
TestRunner::run(new LitePushConsumerTest());
