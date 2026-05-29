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

use Apache\Rocketmq\ProcessQueue;
use Apache\Rocketmq\SimpleConsumer;
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
require_once __DIR__ . '/../../SimpleConsumer.php';

class ProcessQueueIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

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
        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $mq = $this->createMessageQueue();

        $filterExpression = new FilterExpression();
        $filterExpression->setExpression('*');
        $filterExpression->setType(FilterType::TAG);

        $pq = new ProcessQueue($consumer, $mq, '*');

        $this->assertNotNull($pq);
    }

    public function testConstructorWithDefaultFilterExpression()
    {
        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $mq = $this->createMessageQueue();

        // Default filterExpression should be '*'
        $pq = new ProcessQueue($consumer, $mq);

        $this->assertNotNull($pq);
    }

    public function testFetchMessageImmediately()
    {
        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $mq = $this->createMessageQueue();

        $pq = new ProcessQueue($consumer, $mq, '*');

        // fetchMessageImmediately sets a flag, should not throw
        $pq->fetchMessageImmediately();
        $this->assertTrue(true);
    }

    public function testGetMessageQueue()
    {
        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $mq = $this->createMessageQueue();

        $pq = new ProcessQueue($consumer, $mq, '*');
        $returnedMq = $pq->getMessageQueue();

        $this->assertSame($mq, $returnedMq);
        $this->assertEquals('test-topic', $returnedMq->getTopic()->getName());
    }

    public function testCacheMessages()
    {
        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

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
        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $mq = $this->createMessageQueue();
        $pq = new ProcessQueue($consumer, $mq, '*');

        $this->assertFalse($pq->isDropped());

        $pq->drop();
        $this->assertTrue($pq->isDropped());
    }
}
