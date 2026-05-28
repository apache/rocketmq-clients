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

require_once __DIR__ . '/../Producer.php';
require_once __DIR__ . '/../RpcClientManager.php';
require_once __DIR__ . '/../Logger.php';
require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\Producer;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;

/**
 * Tests for Producer validation rules.
 * Mirrors Java's ProducerBuilderImplTest.
 */
class ProducerValidationTest extends TestCase
{
    public function setUp(): void
    {
        \Apache\Rocketmq\Logger::close();
    }

    /**
     * Mirrors Java: testSetClientConfigurationWithNull
     * PHP: empty endpoints - start() will fail to connect but may not throw immediately.
     * We verify the producer can be created.
     */
    public function testConstructorWithNullEndpoints()
    {
        // Producer doesn't validate endpoints at construction in PHP.
        // The gRPC channel is created lazily. We just verify construction works.
        $producer = new Producer('', []);
        $this->assertNotNull($producer, "Producer object should be created");
        // Cleanup: shutdown is safe since isRunning is false
    }

    /**
     * Mirrors Java: testSetTopicWithNull
     * PHP: Message with null topic should fail validation.
     */
    public function testSendMessageWithNullTopic()
    {
        // Use reflection to simulate running state since we can't start gRPC
        $producer = new Producer('127.0.0.1:9876');
        $ref = new \ReflectionProperty($producer, 'isRunning');
        $ref->setAccessible(true);
        $ref->setValue($producer, true);

        $message = new Message();
        $this->expectException(\InvalidArgumentException::class);
        $producer->send($message);
    }

    /**
     * Mirrors Java: testSetIllegalTopic
     * PHP: Message with whitespace-only topic should fail validation.
     */
    public function testSendMessageWithIllegalTopic()
    {
        $producer = new Producer('127.0.0.1:9876');
        $ref = new \ReflectionProperty($producer, 'isRunning');
        $ref->setAccessible(true);
        $ref->setValue($producer, true);

        $message = new Message();
        $tabTopic = new Resource();
        $tabTopic->setName("\t");
        $message->setTopic($tabTopic);
        $this->expectException(\InvalidArgumentException::class);
        $producer->send($message);
    }

    /**
     * Mirrors Java: testSetTopic
     * PHP: valid topic should pass validation (no exception from validation).
     */
    public function testValidTopic()
    {
        $producer = new Producer('127.0.0.1:9876');
        $ref = new \ReflectionProperty($producer, 'isRunning');
        $ref->setAccessible(true);
        $ref->setValue($producer, true);

        // Valid topic with null body should throw for body, not topic
        $validTopic = new Resource();
        $validTopic->setName('abc');
        $message = new Message();
        $message->setTopic($validTopic);
        // No body set - should throw for body validation
        $this->expectException(\InvalidArgumentException::class);
        $producer->send($message);
    }

    /**
     * Mirrors Java: testSetNegativeMaxAttempts
     * PHP: maxAttempts is validated via ExponentialBackoffRetryPolicy.
     */
    public function testNegativeMaxAttempts()
    {
        $this->expectException(\InvalidArgumentException::class);
        new Producer('127.0.0.1:9876', [
                'maxAttempts' => -1,
            ]);
    }

    /**
     * Mirrors Java: testSetMaxAttempts
     * PHP: maxAttempts should be set correctly.
     */
    public function testSetMaxAttempts()
    {
        $producer = new Producer('127.0.0.1:9876', [
            'maxAttempts' => 3,
        ]);

        $ref = new \ReflectionProperty($producer, 'maxAttempts');
        $ref->setAccessible(true);
        $actual = $ref->getValue($producer);
        $this->assertEquals(3, $actual, "maxAttempts should be 3");
    }

    /**
     * Mirrors Java: testSetTransactionCheckerWithNull
     * PHP: transactionChecker is stored in Transaction object, not producer.
     * We verify the producer has the beginTransaction method.
     */
    public function testBeginTransactionWhenNotRunning()
    {
        // Producer not running should throw
        $producer = new Producer('127.0.0.1:9876');

        $this->expectException(\RuntimeException::class);
        $producer->beginTransaction();
    }

    /**
     * Mirrors Java: testSetTransactionChecker
     * PHP: beginTransaction should work when producer is running.
     */
    public function testBeginTransactionWhenRunning()
    {
        $producer = new Producer('127.0.0.1:9876');
        $ref = new \ReflectionProperty($producer, 'isRunning');
        $ref->setAccessible(true);
        $ref->setValue($producer, true);

        // Must set a TransactionChecker before beginTransaction
        $checker = new FakeTransactionChecker();
        $producer->setTransactionChecker($checker);

        $tx = $producer->beginTransaction();
        $this->assertNotNull($tx, "beginTransaction should return a transaction");
    }

    /**
     * Mirrors Java: testBuildWithoutClientConfiguration
     * PHP: empty endpoints at construction doesn't throw (gRPC defers errors).
     * We verify the object is created - connection fails lazily.
     */
    public function testConstructorValidatesEndpoints()
    {
        // Empty endpoints: construction succeeds, connection fails lazily
        $producer = new Producer('');
        $this->assertNotNull($producer, "Producer object should be created even with empty endpoints");
    }

    /**
     * PHP specific: send when producer is not running.
     */
    public function testSendWhenNotRunning()
    {
        $producer = new Producer('127.0.0.1:9876');

        $message = new Message();
        $topic = new Resource();
        $topic->setName('test-topic');
        $message->setTopic($topic);
        $message->setBody('hello');
        $this->expectException(\RuntimeException::class);
        $producer->send($message);
    }

    /**
     * Mirrors Java: testRecall - recallMessage when not running should throw.
     */
    public function testRecallWhenNotRunning()
    {
        $producer = new Producer('127.0.0.1:9876');

        $this->expectException(\RuntimeException::class);
        $producer->recallMessage('test-topic', 'handle-123');
    }

    /**
     * Mirrors Java: testSendBeforeStartup
     * Producer not running, send should throw.
     */
    public function testSendBeforeStartup()
    {
        $producer = new Producer('127.0.0.1:9876', [
            'topics' => ['test-topic'],
        ]);

        $message = new Message();
        $topic = new Resource();
        $topic->setName('test-topic');
        $message->setTopic($topic);
        $message->setBody('body');

        $this->expectException(\RuntimeException::class);
        $producer->send($message);
    }

    /**
     * Tests that beginTransaction requires running producer.
     */
    public function testBeginTransactionRequiresRunning()
    {
        $producer = new Producer('127.0.0.1:9876');

        $this->expectException(\RuntimeException::class);
        $producer->beginTransaction();
    }

    /**
     * Tests recallMessageAsync also requires running producer.
     */
    public function testRecallMessageAsyncWhenNotRunning()
    {
        $producer = new Producer('127.0.0.1:9876');

        $this->expectException(\RuntimeException::class);
        foreach ($producer->recallMessageAsync('test-topic', 'handle') as $result) {
            // generator execution will trigger the throw
        }
    }

    /**
     * Tests message size validation (exceeds 4MB limit).
     * Mirrors Java: testSendWithOversizedMessage.
     */
    public function testSendWithOversizedMessage()
    {
        $producer = new Producer('127.0.0.1:9876');
        $ref = new \ReflectionProperty($producer, 'isRunning');
        $ref->setAccessible(true);
        $ref->setValue($producer, true);

        $message = new Message();
        $topic = new Resource();
        $topic->setName('test-topic');
        $message->setTopic($topic);
        // 4MB + 1 byte
        $message->setBody(str_repeat('x', 4 * 1024 * 1024 + 1));

        $this->expectException(\InvalidArgumentException::class);
        $producer->send($message);
    }

    /**
     * Tests sendFifoMessage when not running.
     */
    public function testSendFifoMessageWhenNotRunning()
    {
        $producer = new Producer('127.0.0.1:9876');

        $this->expectException(\RuntimeException::class);
        $producer->sendFifoMessage('test-topic', 'body', 'group-1');
    }

    /**
     * Tests sendPriorityMessage when not running.
     */
    public function testSendPriorityMessageWhenNotRunning()
    {
        $producer = new Producer('127.0.0.1:9876');

        $this->expectException(\RuntimeException::class);
        $producer->sendPriorityMessage('test-topic', 'body', 1);
    }

    /**
     * Tests sendDelayedMessage when not running.
     */
    public function testSendDelayedMessageWhenNotRunning()
    {
        $producer = new Producer('127.0.0.1:9876');

        $this->expectException(\RuntimeException::class);
        $producer->sendDelayedMessage('test-topic', 'body', time() + 3600);
    }

    /**
     * Tests shutdown when not running (should be a no-op).
     */
    public function testShutdownWhenNotRunning()
    {
        $producer = new Producer('127.0.0.1:9876');

        // Should not throw
        $producer->shutdown();
        $this->assertFalse($producer->isRunning(), "Producer should not be running after shutdown");
    }

    /**
     * Tests getClientId returns a non-empty value.
     */
    public function testGetClientId()
    {
        $producer = new Producer('127.0.0.1:9876', [
            'clientId' => 'my-test-producer',
        ]);

        $this->assertEquals(
            'my-test-producer',
            $producer->getClientId(),
            "ClientId should match configured value"
        );
    }
}

/**
 * Fake TransactionChecker for test use.
 */
class FakeTransactionChecker implements \Apache\Rocketmq\TransactionChecker
{
    public function check(\Apache\Rocketmq\MessageView $messageView): int
    {
        return \Apache\Rocketmq\V2\TransactionResolution::COMMIT;
    }
}

