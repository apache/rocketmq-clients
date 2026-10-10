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

require_once __DIR__ . '/../ProcessQueue.php';
require_once __DIR__ . '/../ConsumeResult.php';
require_once __DIR__ . '/../Logger.php';
require_once __DIR__ . '/../MessageView.php';
require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/helpers/FakeConsumer.php';

use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\SystemProperties;

class ProcessQueueConsumerTest extends TestCase
{
    private function createMessageQueue(): MessageQueue
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

    private function createMessage(string $body, string $receiptHandle = null): Message
    {
        $sysProps = new SystemProperties();
        if ($receiptHandle !== null) {
            $sysProps->setReceiptHandle($receiptHandle);
        }

        $msg = new Message();
        $msg->setBody($body);
        $msg->setSystemProperties($sysProps);
        return $msg;
    }

    public function setUp(): void
    {
        \Apache\Rocketmq\Logger::close();
    }

    // -----------------------------------------------------------------------
    // ConsumerInterface interaction tests
    // -----------------------------------------------------------------------

    public function testConstructorWithConsumerInterface()
    {
        $consumer = new \FakeConsumer('consumer-001');
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, 'tagA');

        $this->assertFalse($pq->isDropped());
        $this->assertEquals(0, $pq->cachedMessagesCount());
        $this->assertEquals(0, $pq->cachedMessageBytes());
        $this->assertSame($mq, $pq->getMessageQueue());
    }

    public function testGetMessageQueueFromConsumer()
    {
        $consumer = new \FakeConsumer('consumer-002');
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, '*');
        $returned = $pq->getMessageQueue();

        $this->assertEquals('test-topic', $returned->getTopic()->getName());
        $this->assertEquals('test-broker', $returned->getBroker()->getName());
    }

    public function testDropStopsFetchingForConsumer()
    {
        $consumer = new \FakeConsumer('consumer-003');
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, '*');
        $this->assertFalse($pq->isDropped());

        $pq->drop();
        $this->assertTrue($pq->isDropped());

        $count = $pq->fetchMessages();
        $this->assertEquals(0, $count);
    }

    // -----------------------------------------------------------------------
    // Ack / Nack interaction via eraseMessage
    // -----------------------------------------------------------------------

    public function testEraseMessageSuccessCallsAckOnConsumer()
    {
        $consumer = new \FakeConsumer('consumer-ack-001');
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, '*');

        $msg = $this->createMessage('hello', 'rh-ack-001');
        $pq->testCacheMessages([$msg]);

        $messageViews = $pq->getCachedMessages();
        $this->assertCount(1, $messageViews);

        $pq->eraseMessage($messageViews[0], \Apache\Rocketmq\ConsumeResult::SUCCESS);

        $this->assertCount(1, $consumer->ackCalls, 'ackMessage should be called once');
        $this->assertCount(0, $consumer->nackCalls, 'nackMessage should not be called');
        $this->assertEquals(0, $pq->cachedMessagesCount(), 'Message should be evicted');
    }

    public function testEraseMessageFailureCallsNackOnConsumer()
    {
        $consumer = new \FakeConsumer('consumer-nack-001');
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, '*');

        $msg = $this->createMessage('world', 'rh-nack-001');
        $pq->testCacheMessages([$msg]);

        $messageViews = $pq->getCachedMessages();
        $this->assertCount(1, $messageViews);

        $pq->eraseMessage($messageViews[0], \Apache\Rocketmq\ConsumeResult::FAILURE);

        $this->assertCount(0, $consumer->ackCalls, 'ackMessage should not be called');
        $this->assertCount(1, $consumer->nackCalls, 'nackMessage should be called once');
        $this->assertEquals(0, $pq->cachedMessagesCount());
    }

    public function testDiscardMessageCallsNackAndEvicts()
    {
        $consumer = new \FakeConsumer('consumer-discard-001');
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, '*');

        $msg = $this->createMessage('discard-me', 'rh-discard-001');
        $pq->testCacheMessages([$msg]);

        $messageViews = $pq->getCachedMessages();
        $this->assertCount(1, $messageViews);

        $pq->discardMessage($messageViews[0]);

        $this->assertCount(1, $consumer->nackCalls, 'discardMessage should call nackMessage');
        $this->assertEquals(0, $pq->cachedMessagesCount(), 'Message should be evicted');
    }

    // -----------------------------------------------------------------------
    // Cache threshold tests with consumer configuration
    // -----------------------------------------------------------------------

    public function testCacheFullRespectsConsumerCountThreshold()
    {
        $consumer = new \FakeConsumer('consumer-threshold-001');
        $consumer->countThreshold = 2;
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, '*');

        $this->assertFalse($pq->isCacheFull());

        $pq->testCacheMessages([$this->createMessage('a', 'rh-a')]);
        $this->assertFalse($pq->isCacheFull());

        $pq->testCacheMessages([$this->createMessage('b', 'rh-b')]);
        $this->assertTrue($pq->isCacheFull());
        $this->assertEquals(2, $pq->cachedMessagesCount());
    }

    public function testCacheFullRespectsConsumerBytesThreshold()
    {
        $consumer = new \FakeConsumer('consumer-bytes-001');
        $consumer->countThreshold = 1024; // high
        $consumer->bytesThreshold = 10;    // low byte limit
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, '*');

        // "12345678901" = 11 bytes > threshold of 10
        $pq->testCacheMessages([$this->createMessage('12345678901', 'rh-bytes')]);
        $this->assertTrue($pq->isCacheFull());
    }

    // -----------------------------------------------------------------------
    // Eviction with consumer configuration
    // -----------------------------------------------------------------------

    public function testEvictMessageWithConsumerBytesTracking()
    {
        $consumer = new \FakeConsumer('consumer-evict-001');
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, '*');

        $msg1 = $this->createMessage('AAAA', 'rh-evict-1');
        $msg2 = $this->createMessage('BB', 'rh-evict-2');
        $pq->testCacheMessages([$msg1, $msg2]);

        $this->assertEquals(6, $pq->cachedMessageBytes());
        $this->assertEquals(2, $pq->cachedMessagesCount());

        $cached = $pq->getCachedMessages();
        $pq->evictMessage($cached[0]);

        $this->assertEquals(2, $pq->cachedMessageBytes());
        $this->assertEquals(1, $pq->cachedMessagesCount());
    }

    // -----------------------------------------------------------------------
    // Multiple erase operations
    // -----------------------------------------------------------------------

    public function testMultipleEraseOperationsTrackConsumerCalls()
    {
        $consumer = new \FakeConsumer('consumer-multi-001');
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, '*');

        $messages = [
            $this->createMessage('msg-1', 'rh-multi-1'),
            $this->createMessage('msg-2', 'rh-multi-2'),
            $this->createMessage('msg-3', 'rh-multi-3'),
        ];
        $pq->testCacheMessages($messages);

        $cached = $pq->getCachedMessages();
        $this->assertCount(3, $cached);

        $pq->eraseMessage($cached[0], \Apache\Rocketmq\ConsumeResult::SUCCESS);
        $pq->eraseMessage($cached[1], \Apache\Rocketmq\ConsumeResult::FAILURE);
        $pq->eraseMessage($cached[2], \Apache\Rocketmq\ConsumeResult::SUCCESS);

        $this->assertCount(2, $consumer->ackCalls, 'Two SUCCESS = two ack calls');
        $this->assertCount(1, $consumer->nackCalls, 'One FAILURE = one nack call');
        $this->assertEquals(0, $pq->cachedMessagesCount());
    }

    // -----------------------------------------------------------------------
    // Expired tests with consumer awaitDuration
    // -----------------------------------------------------------------------

    public function testExpiredWithConsumerAwaitDuration()
    {
        $consumer = new \FakeConsumer('consumer-expire-001');
        $consumer->awaitDuration = 1; // 1 second
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, '*');
        $this->assertFalse($pq->expired());
    }

    // -----------------------------------------------------------------------
    // fetchMessageImmediately with full cache
    // -----------------------------------------------------------------------

    public function testFetchMessageImmediatelyWhenCacheFull()
    {
        $consumer = new \FakeConsumer('consumer-fetch-001');
        $consumer->countThreshold = 1;
        $mq = $this->createMessageQueue();

        $pq = new \Apache\Rocketmq\ProcessQueue($consumer, $mq, '*');

        $pq->testCacheMessages([$this->createMessage('full', 'rh-full')]);
        $this->assertTrue($pq->isCacheFull());

        // fetchMessageImmediately just sets a flag, should not throw
        $pq->fetchMessageImmediately();
        $this->assertTrue(true);
    }
}
