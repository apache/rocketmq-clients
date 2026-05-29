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

require_once __DIR__ . '/../SimpleConsumer.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\SimpleConsumer;

/**
 * Tests for SimpleConsumer validation and state checks.
 * Mirrors Java's SimpleConsumerBuilderTest and SimpleConsumerImplTest.
 */
class SimpleConsumerTest extends TestCase
{
    public function setUp(): void
    {
        \Apache\Rocketmq\Logger::close();
    }

    /**
     * Mirrors Java: testBuildWithoutExpressions - SimpleConsumer
     * validates subscriptions at start().
     */
    public function testStartWithoutSubscriptions()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group');

        $this->expectException(\RuntimeException::class);
        $consumer->start();
    }

    /**
     * Mirrors Java: testReceiveWithoutStart
     */
    public function testReceiveWithoutStart()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->expectException(\RuntimeException::class);
        $consumer->receive(10);
    }

    /**
     * Mirrors Java: testAckWithoutStart
     */
    public function testAckWithoutStart()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->expectException(\RuntimeException::class);
        $consumer->ack(new \stdClass());
    }

    /**
     * Mirrors Java: testSubscribeWithoutStart
     */
    public function testSubscribeWithoutStart()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group');

        $this->expectException(\RuntimeException::class);
        $consumer->subscribe('test-topic', '*');
    }

    /**
     * Mirrors Java: testUnsubscribeWithoutStart
     */
    public function testUnsubscribeWithoutStart()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group');

        $this->expectException(\RuntimeException::class);
        $consumer->unsubscribe('test-topic');
    }

    /**
     * Mirrors Java: testReceiveAsyncWithZeroMaxMessageNum
     */
    public function testReceiveWithZeroMaxMessageNum()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->setRunning($consumer, true);

        $this->expectException(\InvalidArgumentException::class);
        $consumer->receive(0);
    }

    /**
     * Mirrors Java: testReceiveWithNegativeMaxMessageNum.
     */
    public function testReceiveWithNegativeMaxMessageNum()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->setRunning($consumer, true);

        $this->expectException(\InvalidArgumentException::class);
        $consumer->receive(-1);
    }

    /**
     * Tests that changeInvisibleDuration without start throws.
     */
    public function testChangeInvisibleDurationWithoutStart()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->expectException(\RuntimeException::class);
        $consumer->changeInvisibleDuration(new \stdClass(), 30);
    }

    /**
     * Tests that subscriptions returns $this for method chaining.
     */
    public function testSubscribeReturnsThis()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group');
        $this->setRunning($consumer, true);

        $result = $consumer->subscribe('test-topic', '*');
        $this->assertSame($consumer, $result, "subscribe should return \$this for chaining");
    }

    /**
     * Tests unsubscribe method chaining.
     */
    public function testUnsubscribeReturnsThis()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group');

        $this->setRunning($consumer, true);

        $result = $consumer->unsubscribe('test-topic');
        $this->assertTrue(
            $result === $consumer,
            "unsubscribe should return \$this for chaining"
        );
    }

    /**
     * Tests multiple subscriptions can coexist and be enumerated.
     */
    public function testMultipleSubscriptions()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group');
        $this->setRunning($consumer, true);

        $consumer->subscribe('topic-1', 'tagA');
        $consumer->subscribe('topic-2', '*');
        $expressions = $consumer->getSubscriptionExpressions();
        $this->assertEquals(2, count($expressions), 'Should have 2 subscriptions after two subscribe calls');

        $this->assertTrue(
            isset($expressions['topic-1']),
            'topic-1 should be subscriptions'
        );

        $this->assretTrue(
            isset($expressions['topic-2']),
            'topic-2 should be subscriptions'
        );
        $this->assertEquals('tagA', $expressions['topic-1'], "topic-1 should have tag 'tagA'");
        $this->assertEquals('*', $expressions['topic-2'], "topic-2 should have tag '*'");
    }

    /**
     * Tests awaitDuration configuration.
     */
    public function testAwaitDurationConfiguration()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group', [
            'awaitDuration' => 60,
        ]);

        $ref = new \ReflectionProperty($consumer, 'awaitDuration');
        $ref->setAccessible(true);
        $actual = $ref->getValue($consumer);

        $this->assertEquals(60, $actual, "awaitDuration should be 60");
    }

    /**
     * Tests clientId is set correctly.
     */
    public function testGetClientId()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group', [
            'clientId' => 'custom-client-id',
        ]);

        $clientId = $consumer->getClientId();
        $this->assertEquals(
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
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'my-consumer-group');

        $this->assertEquals(
            'my-consumer-group',
            $consumer->getConsumerGroup(),
            "ConsumerGroup should match configured value"
        );
    }

    /**
     * Tests subscribe stores the subscription and returns $this for chaining.
     */
    public function testSubscribeStoresAndReturnsThis()
    {
        $consumer = new SimpleConsumer('127.0.0.1:9876', 'test-group');
        $this->setRunning($consumer, true);
        $result = $consumer->subscribe('topic-a', '*');
        $this->assertSame($consumer, $result, "subscribe should return \$this for chaining");
        $result2 = $consumer->subscribe('topic-b', 'tagX');
        $this->assertSame($consumer, $result2, "subscribe should return \$this for chaining");

        $expressions = $consumer->getSubscriptionExpressions();
        $this->assertCount(2, $expressions, "should have 2 subscriptions after two subscribe calls");
        $this->assertEquals("*", $expressions['topic-a'], "topic-a should have tag '*'");
        $this->assertEquals(
            'tagX', $expressions['topic-b'], "topic-b should have tag 'tagX'"
        );
    }

    private function setRunning($consumer, bool $running): void
    {
        $ref = new \ReflectionProperty($consumer, 'isStarted');
        $ref->setAccessible(true);
        $ref->setValue($consumer, $running);
    }
}
