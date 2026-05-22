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
require_once __DIR__ . '/../LiteSimpleConsumer.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\LiteSimpleConsumer;

/**
 * Tests for LiteSimpleConsumer validation rules.
 * Mirrors Java's LiteSimpleConsumerBuilderImplTest.
 */
class LiteSimpleConsumerTest
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
            new LiteSimpleConsumer('127.0.0.1:9876', '', 'parent-topic');
        }, "Empty consumer group should throw");
    }

    /**
     * Mirrors Java: testBindTopicWithNull
     */
    public function testConstructorWithEmptyParentTopic()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', '');
        }, "Empty parent topic should throw");
    }

    /**
     * Mirrors Java: testBindTopicWithBlank
     */
    public function testConstructorWithWhitespaceParentTopic()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', '  ');
        }, "Whitespace-only parent topic should throw");
    }

    /**
     * Mirrors Java: testBindTopicWithBlankString
     */
    public function testSubscribeLiteWithEmptyTopic()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        TestRunner::assertThrows(\InvalidArgumentException::class, function() use ($consumer) {
            $consumer->subscribeLite('');
        }, "Empty lite topic should throw");
    }

    /**
     * Mirrors Java: testBindTopicWithWhitespaces
     */
    public function testSubscribeLiteWithWhitespaceTopic()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        TestRunner::assertThrows(\InvalidArgumentException::class, function() use ($consumer) {
            $consumer->subscribeLite('   ');
        }, "Whitespace-only lite topic should throw");
    }

    /**
     * Mirrors Java: testSetNegativeMaxCacheMessageCount
     */
    public function testConstructorWithZeroAwaitDuration()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'awaitDuration' => 0,
        ]);

        $ref = new \ReflectionProperty($consumer, 'awaitDuration');
        $ref->setAccessible(true);
        $actual = $ref->getValue($consumer);
        TestRunner::assertEquals(0, $actual, "awaitDuration should be 0");
    }

    /**
     * Mirrors Java: testBuildWithoutConsumerGroup
     */
    public function testStartWithoutLiteTopics()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->start();
        }, "Start without lite topics should throw");
    }

    /**
     * Tests that subscribeLite works before start.
     */
    public function testSubscribeLiteBeforeStart()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        $consumer->subscribeLite('lite-topic-1');
        $consumer->subscribeLite('lite-topic-2', 'tagA');

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
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        $consumer->subscribeLite('lite-topic');
        $consumer->unsubscribeLite('lite-topic');

        $topics = $consumer->getLiteTopics();
        TestRunner::assertTrue(
            empty($topics),
            "Lite topics should be empty after unsubscribe"
        );
    }

    /**
     * Tests receive() before start throws.
     */
    public function testReceiveWithoutStart()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->receive(10);
        }, "Receive before start should throw");
    }

    /**
     * Tests ack() before start throws.
     */
    public function testAckWithoutStart()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->ack(new \stdClass());
        }, "Ack before start should throw");
    }

    /**
     * Tests that start() returns silently when already running.
     */
    public function testStartWhenAlreadyRunning()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');
        $consumer->subscribeLite('lite-topic');

        $ref = new \ReflectionProperty($consumer, 'isRunning');
        $ref->setAccessible(true);
        $ref->setValue($consumer, true);

        $consumer->start();
        TestRunner::assertTrue($consumer->isRunning(), "Consumer should still be running");
    }

    /**
     * Tests that lite topic name length is validated.
     */
    public function testLiteTopicNameTooLong()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'maxLiteTopicSize' => 10,
        ]);

        TestRunner::assertThrows(\RuntimeException::class, function() use ($consumer) {
            $consumer->subscribeLite('this-is-a-very-long-lite-topic-name');
        }, "Long lite topic name should throw");
    }

    /**
     * Mirrors Java: testMethodChaining
     */
    public function testMethodChaining()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        $result = $consumer->subscribeLite('topic1');
        TestRunner::assertTrue(
            $result === $consumer,
            "subscribeLite should return \$this for chaining"
        );

        $result = $consumer->unsubscribeLite('topic1');
        TestRunner::assertTrue(
            $result === $consumer,
            "unsubscribeLite should return \$this for chaining"
        );
    }

    /**
     * Tests getClientId returns expected format.
     */
    public function testGetClientId()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');
        $clientId = $consumer->getClientId();

        TestRunner::assertTrue(
            strlen($clientId) > 0,
            "ClientId should be non-empty"
        );
        TestRunner::assertTrue(
            strpos($clientId, 'php-lite-simple-consumer-') === 0,
            "ClientId should start with expected prefix"
        );
    }
}

echo "=== LiteSimpleConsumerTest ===\n";
TestRunner::run(new LiteSimpleConsumerTest());
