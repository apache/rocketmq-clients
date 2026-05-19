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
require_once __DIR__ . '/../ProcessQueue.php';
require_once __DIR__ . '/../ConsumeResult.php';
require_once __DIR__ . '/../Logger.php';
require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Permission;

/**
 * Fake consumer for ProcessQueue testing.
 */
class FakeConsumerForPQ
{
    public $countThreshold = 1024;
    public $bytesThreshold = 1048576;
    public $awaitDuration = 30;
    public $receiveBatchSize = 32;
    public $clientId = 'test-pq-client-id';

    public function getGroupResource()
    {
        $resource = new Resource();
        $resource->setName('test-group');
        return $resource;
    }

    public function getClientId() { return $this->clientId; }
    public function getAwaitDuration() { return $this->awaitDuration; }
    public function getReceiveBatchSize() { return $this->receiveBatchSize; }

    public function getCacheMessageCountThresholdPerQueue() { return $this->countThreshold; }
    public function getCacheMessageBytesThresholdPerQueue() { return $this->bytesThreshold; }

    public function ackMessage($messageView) {}
    public function nackMessage($messageView) {}
}

/**
 * Fake consumer that tracks ack/nack calls for eraseMessage tests.
 */
class FakeConsumerForAckPQ
{
    public $countThreshold = 1024;
    public $bytesThreshold = 1048576;
    public $awaitDuration = 30;
    public $receiveBatchSize = 32;
    public $clientId = 'test-pq-ack-client-id';
    public $ackCalled = false;
    public $nackCalled = false;

    public function getGroupResource()
    {
        $resource = new Resource();
        $resource->setName('test-group');
        return $resource;
    }

    public function getClientId() { return $this->clientId; }
    public function getAwaitDuration() { return $this->awaitDuration; }
    public function getReceiveBatchSize() { return $this->receiveBatchSize; }

    public function getCacheMessageCountThresholdPerQueue() { return $this->countThreshold; }
    public function getCacheMessageBytesThresholdPerQueue() { return $this->bytesThreshold; }

    public function ackMessage($messageView) { $this->ackCalled = true; }
    public function nackMessage($messageView) { $this->nackCalled = true; }
}

class ProcessQueueTest
{
    private function createFakeMessageQueue()
    {
        $address = new Address();
        $address->setHost('127.0.0.1');
        $address->setPort(8080);

        $endpoints = new Endpoints();
        $endpoints->setScheme(AddressScheme::IPv4);
        $endpoints->setAddresses([$address]);

        $broker = new Broker();
        $broker->setName('test-broker');
        $broker->setEndpoints($endpoints);

        $topic = new Resource();
        $topic->setName('test-topic');

        $queue = new MessageQueue();
        $queue->setTopic($topic);
        $queue->setBroker($broker);
        $queue->setId(0);
        $queue->setPermission(Permission::READ_WRITE);

        return $queue;
    }

    public function testConstructorAndInitialState()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForPQ();
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');

        TestRunner::assertFalse($pq->isDropped(), "ProcessQueue should not be dropped initially");
        TestRunner::assertEqualsWithMessage(0, $pq->cachedMessagesCount(), "Cached messages count should be 0");
        TestRunner::assertEqualsWithMessage(0, $pq->cachedMessageBytes(), "Cached message bytes should be 0");
    }

    public function testDropAndIsDropped()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForPQ();
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');

        TestRunner::assertFalse($pq->isDropped(), "Should not be dropped before drop()");
        $pq->drop();
        TestRunner::assertTrue($pq->isDropped(), "Should be dropped after drop()");
    }

    public function testGetMessageQueue()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForPQ();
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');

        $resultMq = $pq->getMessageQueue();
        TestRunner::assertTrueWithMessage(
            $resultMq->getTopic()->getName() === 'test-topic',
            "getMessageQueue should return the original MessageQueue"
        );
    }

    public function testFetchMessageImmediately()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForPQ();
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');
        $pq->fetchMessageImmediately();

        TestRunner::assertTrueWithMessage(true, "fetchMessageImmediately should not throw");
    }

    public function testExpiredNotInitially()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForPQ();
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');

        TestRunner::assertFalse($pq->expired(), "ProcessQueue should not be expired immediately after creation");
    }

    public function testCacheNotFullInitially()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForPQ();
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');

        TestRunner::assertFalse($pq->isCacheFull(), "Cache should not be full with no messages");
    }

    /**
     * Mirrors Java: testCachedMessagesCount and testCachedMessageBytes.
     * Tests that cacheMessages and eviction track count/bytes correctly.
     */
    public function testCachedMessagesCountAndBytes()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForPQ();
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');

        // Create a fake cached message
        $msg = new \stdClass();
        $msg->body = "hello world";
        $pq->testCacheMessages([$msg]);

        TestRunner::assertEqualsWithMessage(1, $pq->cachedMessagesCount(), "Cached count should be 1");
        TestRunner::assertEqualsWithMessage(11, $pq->cachedMessageBytes(), "Cached bytes should be 11");
    }

    /**
     * Mirrors Java: testEraseMessageWithConsumeSuccess.
     * eraseMessage with SUCCESS should call ackMessage and evict.
     */
    public function testEraseMessageWithConsumeSuccess()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForAckPQ();
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');

        $msg = new \stdClass();
        $msg->body = "test-message";
        $pq->testCacheMessages([$msg]);

        TestRunner::assertEqualsWithMessage(1, $pq->cachedMessagesCount(), "Should have 1 cached message");

        $pq->eraseMessage($msg, \Apache\Rocketmq\ConsumeResult::SUCCESS);

        TestRunner::assertEqualsWithMessage(0, $pq->cachedMessagesCount(), "Message should be evicted after erase");
        TestRunner::assertTrue($fakeConsumer->ackCalled, "ackMessage should be called for SUCCESS");
        TestRunner::assertFalse($fakeConsumer->nackCalled, "nackMessage should NOT be called for SUCCESS");
    }

    /**
     * Mirrors Java: testEraseMessageWithConsumeFailure.
     * eraseMessage with FAILURE should call nackMessage and evict.
     */
    public function testEraseMessageWithConsumeFailure()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForAckPQ();
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');

        $msg = new \stdClass();
        $msg->body = "test-message";
        $pq->testCacheMessages([$msg]);

        $pq->eraseMessage($msg, \Apache\Rocketmq\ConsumeResult::FAILURE);

        TestRunner::assertEqualsWithMessage(0, $pq->cachedMessagesCount(), "Message should be evicted after erase");
        TestRunner::assertFalse($fakeConsumer->ackCalled, "ackMessage should NOT be called for FAILURE");
        TestRunner::assertTrue($fakeConsumer->nackCalled, "nackMessage should be called for FAILURE");
    }

    /**
     * Tests that cache full blocks further caching.
     */
    public function testCacheFullThreshold()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForPQ();
        $fakeConsumer->countThreshold = 3;
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');

        // Fill up to threshold
        for ($i = 0; $i < 3; $i++) {
            $msg = new \stdClass();
            $msg->body = "msg-{$i}";
            $pq->testCacheMessages([$msg]);
        }

        TestRunner::assertTrue($pq->isCacheFull(), "Cache should be full at threshold");

        // Verify messages still count
        TestRunner::assertEqualsWithMessage(3, $pq->cachedMessagesCount(), "Should have 3 cached messages");
    }

    /**
     * Tests that dropped ProcessQueue returns 0 from fetchMessages.
     */
    public function testDroppedQueueDoesNotFetch()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForPQ();
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');
        $pq->drop();

        // After drop, fetchMessages should return 0 immediately without calling broker
        $count = $pq->fetchMessages();
        TestRunner::assertEqualsWithMessage(0, $count, "Dropped queue should fetch 0 messages");
    }

    /**
     * Tests evictMessage reduces byte count correctly.
     */
    public function testEvictMessageReducesBytes()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForPQ();
        $mq = $this->createFakeMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($fakeConsumer, $mq, '*');

        $msg1 = new \stdClass();
        $msg1->body = "hello";
        $msg2 = new \stdClass();
        $msg2->body = "world!";
        $pq->testCacheMessages([$msg1, $msg2]);

        $initialBytes = $pq->cachedMessageBytes();
        TestRunner::assertEqualsWithMessage(11, $initialBytes, "Initial bytes should be 11 (5+6)");

        $pq->evictMessage($msg1);

        $afterBytes = $pq->cachedMessageBytes();
        TestRunner::assertEqualsWithMessage(6, $afterBytes, "Bytes should be 6 after evicting 5-byte message");
        TestRunner::assertEqualsWithMessage(1, $pq->cachedMessagesCount(), "Count should be 1 after eviction");
    }
}

echo "=== ProcessQueueTest ===\n";
$test = new ProcessQueueTest();
$test->testConstructorAndInitialState();
echo "  [OK] testConstructorAndInitialState\n";
$test->testDropAndIsDropped();
echo "  [OK] testDropAndIsDropped\n";
$test->testGetMessageQueue();
echo "  [OK] testGetMessageQueue\n";
$test->testFetchMessageImmediately();
echo "  [OK] testFetchMessageImmediately\n";
$test->testExpiredNotInitially();
echo "  [OK] testExpiredNotInitially\n";
$test->testCacheNotFullInitially();
echo "  [OK] testCacheNotFullInitially\n";
$test->testCachedMessagesCountAndBytes();
echo "  [OK] testCachedMessagesCountAndBytes\n";
$test->testEraseMessageWithConsumeSuccess();
echo "  [OK] testEraseMessageWithConsumeSuccess\n";
$test->testEraseMessageWithConsumeFailure();
echo "  [OK] testEraseMessageWithConsumeFailure\n";
$test->testCacheFullThreshold();
echo "  [OK] testCacheFullThreshold\n";
$test->testDroppedQueueDoesNotFetch();
echo "  [OK] testDroppedQueueDoesNotFetch\n";
$test->testEvictMessageReducesBytes();
echo "  [OK] testEvictMessageReducesBytes\n";
