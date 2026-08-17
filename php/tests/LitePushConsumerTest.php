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

require_once __DIR__ . '/../LitePushConsumer.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\LitePushConsumer;

/**
 * Tests for LitePushConsumer validation rules.
 * Mirrors Java's LitePushConsumerBuilderImplTest.
 */
class LitePushConsumerTest extends TestCase
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
        $this->expectException(\InvalidArgumentException::class);
        new LitePushConsumer('127.0.0.1:9876', '', 'parent-topic');
    }

    /**
     * Mirrors Java: testBindTopicWithNull
     */
    public function testConstructorWithNullParentTopic()
    {
        $this->expectException(\InvalidArgumentException::class);
        new LitePushConsumer('127.0.0.1:9876', 'test-group', '');
    }

    /**
     * Mirrors Java: testSetMessageListenerWithNull
     */
    public function testStartWithoutMessageListener()
    {
        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', []);

        $this->expectException(\RuntimeException::class);
        $consumer->start();
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
        $this->assertTrue(
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
        $this->assertTrue(
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

        $this->expectException(\RuntimeException::class);
        $consumer->start();
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
        $this->assertEquals(
            2,
            count($topics),
            "Should have 2 lite topics"
        );
        $this->assertTrue(
            in_array('lite-topic-1', $topics),
            "lite-topic-1 should be in topics"
        );
        $this->assertTrue(
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
        $this->assertTrue(
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

        $this->expectException(\RuntimeException::class);
        $consumer->subscribeLite('this-is-a-very-long-lite-topic-name', function($msg) { return 0; });
    }

    /**
     * Tests isLiteConsumer returns true.
     */
    public function testIsLiteConsumer()
    {
        $consumer = new LitePushConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'messageListener' => function($msg) { return 0; },
        ]);

        $this->assertTrue(
            $consumer->isLiteConsumer(),
            "LitePushConsumer should return true for isLiteConsumer"
        );
    }
}
