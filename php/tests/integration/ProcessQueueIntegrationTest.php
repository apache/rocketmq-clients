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
namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\ConsumeService;
use Apache\Rocketmq\ConsumerInterface;
use Apache\Rocketmq\ExponentialBackoffRetryPolicy;
use Apache\Rocketmq\MessageView;
use Apache\Rocketmq\ProcessQueue;
use Apache\Rocketmq\RpcClientManager;
use Apache\Rocketmq\SessionCredentials;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\FilterType;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../ProcessQueue.php';
require_once __DIR__ . '/../../ConsumerInterface.php';
require_once __DIR__ . '/../../RpcClientManager.php';

class ProcessQueueIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    private function createTestConsumer(): ConsumerInterface
    {
        $mock = GrpcMockHelper::createMockClient();
        RpcClientManager::getInstance()->registerMock($this->endpoints, $mock);

        return new class implements ConsumerInterface {
            public function getClientId(): string { return 'test-client-id'; }
            public function getTopicResource(string $topic): Resource
            {
                $r = new Resource();
                $r->setName($topic);
                return $r;
            }
            public function getNamespace(): string { return ''; }
            public function getGroupResourceWithNamespace(): Resource
            {
                $r = new Resource();
                $r->setName('test-group');
                return $r;
            }
            public function getSessionCredentials(): ?SessionCredentials { return null; }
            public function buildMetadata(?int $timeoutMs = null): array { return ['x-rocketmq-client-id' => 'test-client-id']; }
            public function ackMessage(MessageView $messageView): bool { return true; }
            public function nackMessage(MessageView $messageView, int $deliveryAttempt = 1, ?int $invisibleDuration = null): bool { return true; }
            public function getAwaitDuration(): int { return 30; }
            public function getReceiveBatchSize(): int { return 32; }
            public function getCacheMessageCountThresholdPerQueue(): int { return 1000; }
            public function getCacheMessageBytesThresholdPerQueue(): int { return 1048576; }
            public function executeInterceptors(string $hookPoint, array $context): void {}
            public function getRetryPolicy(): ?ExponentialBackoffRetryPolicy { return null; }
            public function getConsumeService(): ?ConsumeService { return null; }
        };
    }

    private function createMessageQueue(): MessageQueue
    {
        $mq = new MessageQueue();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $mq->setTopic($topicResource);
        $mq->setPermission(Permission::READ_WRITE);
        $broker = new Broker();
        $broker->setName('broker-1');
        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost('127.0.0.1');
        $address->setPort(8080);
        $brokerEndpoints->setAddresses([$address]);
        $broker->setEndpoints($brokerEndpoints);
        $mq->setBroker($broker);
        return $mq;
    }

    public function testConstructorCreatesProcessQueue()
    {
        $consumer = $this->createTestConsumer();

        $mq = $this->createMessageQueue();

        $filterExpression = new FilterExpression();
        $filterExpression->setExpression('*');
        $filterExpression->setType(FilterType::TAG);

        $pq = new ProcessQueue($consumer, $mq, '*');

        $this->assertNotNull($pq);
    }

    public function testConstructorWithDefaultFilterExpression()
    {
        $consumer = $this->createTestConsumer();

        $mq = $this->createMessageQueue();

        // Default filterExpression should be '*'
        $pq = new ProcessQueue($consumer, $mq);

        $this->assertNotNull($pq);
    }

    public function testFetchMessageImmediately()
    {
        $consumer = $this->createTestConsumer();

        $mq = $this->createMessageQueue();

        $pq = new ProcessQueue($consumer, $mq, '*');

        // fetchMessageImmediately sets a flag, should not throw
        $pq->fetchMessageImmediately();
        $this->assertTrue(true);
    }

    public function testGetMessageQueue()
    {
        $consumer = $this->createTestConsumer();

        $mq = $this->createMessageQueue();

        $pq = new ProcessQueue($consumer, $mq, '*');
        $returnedMq = $pq->getMessageQueue();

        $this->assertSame($mq, $returnedMq);
        $this->assertEquals('test-topic', $returnedMq->getTopic()->getName());
    }

    public function testCacheMessages()
    {
        $consumer = $this->createTestConsumer();

        $mq = $this->createMessageQueue();
        $pq = new ProcessQueue($consumer, $mq, '*');

        $msg = new Message();
        $msgTopic = new Resource();
        $msgTopic->setName('test-topic');
        $msg->setTopic($msgTopic);
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-001');
        $sysProps->setReceiptHandle('receipt-001');
        $msg->setSystemProperties($sysProps);
        $msg->setBody('Hello World');

        $pq->testCacheMessages([$msg]);

        $cached = $pq->getCachedMessages();
        $this->assertCount(1, $cached);
    }

    public function testDropAndIsDropped()
    {
        $consumer = $this->createTestConsumer();

        $mq = $this->createMessageQueue();
        $pq = new ProcessQueue($consumer, $mq, '*');

        $this->assertFalse($pq->isDropped());

        $pq->drop();
        $this->assertTrue($pq->isDropped());
    }
}
