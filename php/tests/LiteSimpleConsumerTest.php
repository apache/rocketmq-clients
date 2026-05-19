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
    /**
     * Mirrors Java: testSetConsumerGroupWithNull
     */
    public function testConstructorWithNullConsumerGroup()
    {
        \Apache\Rocketmq\Logger::close();

        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new LiteSimpleConsumer('127.0.0.1:9876', '', 'parent-topic');
        }, "Empty consumer group should throw");
    }

    /**
     * Mirrors Java: testBindTopicWithNull
     */
    public function testConstructorWithEmptyParentTopic()
    {
        \Apache\Rocketmq\Logger::close();

        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', '');
        }, "Empty parent topic should throw");
    }

    /**
     * Mirrors Java: testBindTopicWithBlank
     */
    public function testConstructorWithWhitespaceParentTopic()
    {
        \Apache\Rocketmq\Logger::close();

        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', '  ');
        }, "Whitespace-only parent topic should throw");
    }

    /**
     * Mirrors Java: testBindTopicWithBlankString
     */
    public function testSubscribeLiteWithEmptyTopic()
    {
        \Apache\Rocketmq\Logger::close();

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
        \Apache\Rocketmq\Logger::close();

        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        TestRunner::assertThrows(\InvalidArgumentException::class, function() use ($consumer) {
            $consumer->subscribeLite('   ');
        }, "Whitespace-only lite topic should throw");
    }

    /**
     * Mirrors Java: testSetNegativeMaxCacheMessageCount
     * PHP LiteSimpleConsumer doesn't have maxCacheMessageCount but validates
     * awaitDuration similarly.
     */
    public function testConstructorWithZeroAwaitDuration()
    {
        \Apache\Rocketmq\Logger::close();

        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'awaitDuration' => 0,
        ]);

        // awaitDuration of 0 should be accepted
        $ref = new \ReflectionProperty($consumer, 'awaitDuration');
        $ref->setAccessible(true);
        $actual = $ref->getValue($consumer);
        TestRunner::assertEqualsWithMessage(0, $actual, "awaitDuration should be 0");
    }

    /**
     * Mirrors Java: testBuildWithoutConsumerGroup
     * PHP: LiteSimpleConsumer without lite topics should throw at start.
     */
    public function testStartWithoutLiteTopics()
    {
        \Apache\Rocketmq\Logger::close();

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
        \Apache\Rocketmq\Logger::close();

        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        $consumer->subscribeLite('lite-topic-1');
        $consumer->subscribeLite('lite-topic-2', 'tagA');

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
        \Apache\Rocketmq\Logger::close();

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
        \Apache\Rocketmq\Logger::close();

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
        \Apache\Rocketmq\Logger::close();

        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');
        $consumer->subscribeLite('lite-topic');

        $ref = new \ReflectionProperty($consumer, 'isRunning');
        $ref->setAccessible(true);
        $ref->setValue($consumer, true);

        // start() should return silently when already running
        $consumer->start();
        TestRunner::assertTrue($consumer->isRunning(), "Consumer should still be running");
    }

    /**
     * Tests that lite topic name length is validated.
     */
    public function testLiteTopicNameTooLong()
    {
        \Apache\Rocketmq\Logger::close();

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
        \Apache\Rocketmq\Logger::close();

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
        \Apache\Rocketmq\Logger::close();

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
$test = new LiteSimpleConsumerTest();
$test->testConstructorWithNullConsumerGroup();
echo "  [OK] testConstructorWithNullConsumerGroup\n";
$test->testConstructorWithEmptyParentTopic();
echo "  [OK] testConstructorWithEmptyParentTopic\n";
$test->testConstructorWithWhitespaceParentTopic();
echo "  [OK] testConstructorWithWhitespaceParentTopic\n";
$test->testSubscribeLiteWithEmptyTopic();
echo "  [OK] testSubscribeLiteWithEmptyTopic\n";
$test->testSubscribeLiteWithWhitespaceTopic();
echo "  [OK] testSubscribeLiteWithWhitespaceTopic\n";
$test->testConstructorWithZeroAwaitDuration();
echo "  [OK] testConstructorWithZeroAwaitDuration\n";
$test->testStartWithoutLiteTopics();
echo "  [OK] testStartWithoutLiteTopics\n";
$test->testSubscribeLiteBeforeStart();
echo "  [OK] testSubscribeLiteBeforeStart\n";
$test->testUnsubscribeLiteBeforeStart();
echo "  [OK] testUnsubscribeLiteBeforeStart\n";
$test->testReceiveWithoutStart();
echo "  [OK] testReceiveWithoutStart\n";
$test->testAckWithoutStart();
echo "  [OK] testAckWithoutStart\n";
$test->testStartWhenAlreadyRunning();
echo "  [OK] testStartWhenAlreadyRunning\n";
$test->testLiteTopicNameTooLong();
echo "  [OK] testLiteTopicNameTooLong\n";
$test->testMethodChaining();
echo "  [OK] testMethodChaining\n";
$test->testGetClientId();
echo "  [OK] testGetClientId\n";
