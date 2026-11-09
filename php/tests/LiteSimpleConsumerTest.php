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

require_once __DIR__ . '/../LiteSimpleConsumer.php';
require_once __DIR__ . '/../LiteSimpleConsumerBuilder.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\LiteSimpleConsumer;
use Apache\Rocketmq\LiteSimpleConsumerBuilder;
use Apache\Rocketmq\V2\ClientType;

/**
 * Tests for LiteSimpleConsumer validation rules and lite subscription management.
 * Mirrors Java's LiteSimpleConsumerBuilderImplTest / LiteSimpleConsumerImplTest.
 */
class LiteSimpleConsumerTest extends TestCase
{
    public function setUp(): void
    {
        \Apache\Rocketmq\Logger::close();
    }

    /**
     * Mirrors Java: bind topic must be non-empty.
     */
    public function testConstructorWithNullParentTopic()
    {
        $this->expectException(\InvalidArgumentException::class);
        new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', '');
    }

    /**
     * Mirrors Java: subscribeLite works before start.
     */
    public function testSubscribeLiteBeforeStart()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        $result = $consumer->subscribeLite('lite-topic-1');
        $this->assertSame($consumer, $result);
        $consumer->subscribeLite('lite-topic-2');

        $topics = $consumer->getLiteTopics();
        $this->assertEquals(2, count($topics), "Should have 2 lite topics");
        $this->assertTrue(in_array('lite-topic-1', $topics), "lite-topic-1 should be in topics");
        $this->assertTrue(in_array('lite-topic-2', $topics), "lite-topic-2 should be in topics");
    }

    /**
     * Mirrors Java: unsubscribeLite before start.
     */
    public function testUnsubscribeLiteBeforeStart()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        $consumer->subscribeLite('lite-topic');
        $consumer->unsubscribeLite('lite-topic');

        $topics = $consumer->getLiteTopics();
        $this->assertTrue(empty($topics), "Lite topics should be empty after unsubscribe");
    }

    /**
     * Lite topic name length is validated.
     */
    public function testLiteTopicNameTooLong()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'maxLiteTopicSize' => 10,
        ]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('exceeds max length');
        $consumer->subscribeLite('this-is-a-very-long-lite-topic-name');
    }

    /**
     * Lite subscription quota is enforced.
     */
    public function testLiteTopicQuotaExceeded()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic', [
            'liteSubscriptionQuota' => 1,
        ]);
        $consumer->subscribeLite('topic-1');
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('quota exceeded');
        $consumer->subscribeLite('topic-2');
    }

    /**
     * subscribeLite cannot be called after start().
     */
    public function testSubscribeLiteAfterStartThrows()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');
        $consumer->subscribeLite('lite-topic-1');
        // Force started state via reflection to emulate an already-started consumer.
        $ref = new \ReflectionProperty(LiteSimpleConsumer::class, 'isStarted');
        $ref->setAccessible(true);
        $ref->setValue($consumer, true);

        $this->expectException(\RuntimeException::class);
        $consumer->subscribeLite('lite-topic-2');
    }

    /**
     * start() requires at least one lite topic.
     */
    public function testStartWithoutLiteTopics()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('no lite topics');
        $consumer->start();
    }

    /**
     * getClientType() reports the lite simple consumer type.
     */
    public function testGetClientType()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');

        $ref = new \ReflectionMethod(LiteSimpleConsumer::class, 'getClientType');
        $ref->setAccessible(true);
        $clientType = $ref->invoke($consumer);

        $this->assertEquals(ClientType::LITE_SIMPLE_CONSUMER, $clientType);
    }

    /**
     * handleUnsubscribeLite only removes known lite topics.
     */
    public function testHandleUnsubscribeLite()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');
        $consumer->subscribeLite('lite-topic-1');
        $consumer->subscribeLite('lite-topic-2');

        $consumer->handleUnsubscribeLite('lite-topic-1');
        $this->assertNotContains('lite-topic-1', $consumer->getLiteTopics());
        $this->assertContains('lite-topic-2', $consumer->getLiteTopics());
    }

    /**
     * handleUnsubscribeLite ignores unknown lite topics.
     */
    public function testHandleUnsubscribeLiteIgnoresUnknown()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');
        $consumer->subscribeLite('lite-topic-1');

        $consumer->handleUnsubscribeLite('nonexistent');
        $this->assertContains('lite-topic-1', $consumer->getLiteTopics());
    }

    /**
     * Builder.buildWithoutStart() wires lite topics and returns a configured consumer.
     */
    public function testBuilderBuildWithoutStart()
    {
        $consumer = (new LiteSimpleConsumerBuilder())
            ->setEndpoints('127.0.0.1:8081')
            ->setConsumerGroup('lite-group')
            ->bindTopic('ParentTopic')
            ->subscriptionLite('lite-topic-a')
            ->subscriptionLite('lite-topic-b')
            ->buildWithoutStart();

        $this->assertInstanceOf(LiteSimpleConsumer::class, $consumer);
        $this->assertEquals(['lite-topic-a', 'lite-topic-b'], $consumer->getLiteTopics());
    }

    /**
     * Builder requires endpoints, group, parent topic and at least one lite topic.
     */
    public function testBuilderValidatesRequiredFields()
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('must be set');
        (new LiteSimpleConsumerBuilder())
            ->bindTopic('ParentTopic')
            ->subscriptionLite('lite-topic-a')
            ->buildWithoutStart();
    }

    /**
     * isRunning() is false before start().
     */
    public function testIsRunningInitiallyFalse()
    {
        $consumer = new LiteSimpleConsumer('127.0.0.1:9876', 'test-group', 'parent-topic');
        $this->assertFalse($consumer->isRunning());
    }
}
