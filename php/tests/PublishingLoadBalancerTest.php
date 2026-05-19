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
require_once __DIR__ . '/../ProducerOptimized.php';

use Apache\Rocketmq\PublishingLoadBalancer;
use Apache\Rocketmq\Test\TestRunner;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints as V2Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\MessageType as V2MessageType;

class FakeRouteData
{
    private $messageQueues = [];

    public function __construct($messageQueues)
    {
        $this->messageQueues = $messageQueues;
    }

    public function getMessageQueues()
    {
        return $this->messageQueues;
    }
}

class PublishingLoadBalancerTest
{
    private function fakePbMessageQueue0()
    {
        $address = new Address();
        $address->setHost('127.0.0.1');
        $address->setPort(8080);

        $endpoints = new V2Endpoints();
        $endpoints->setScheme(AddressScheme::IPv4);
        $endpoints->setAddresses([$address]);

        $broker = new Broker();
        $broker->setName('foo-bar-broker-0');
        $broker->setEndpoints($endpoints);

        $topic = new Resource();
        $topic->setName('foo-bar-topic-0');

        $queue = new MessageQueue();
        $queue->setTopic($topic);
        $queue->setBroker($broker);
        $queue->setPermission(Permission::READ_WRITE);
        $queue->setAcceptMessageTypes([V2MessageType::NORMAL]);

        return $queue;
    }

    public function testTakeMessageQueueByMessageGroup()
    {
        $messageQueue = $this->fakePbMessageQueue0();
        $routeData = new FakeRouteData([$messageQueue]);
        $loadBalancer = new PublishingLoadBalancer($routeData);

        $result = $loadBalancer->takeMessageQueueByMessageGroup('test');
        TestRunner::assertNotNull($result, "Should return a message queue");
    }

    public function testTakeTwoMessageQueuesWithSingleQueue()
    {
        $messageQueue = $this->fakePbMessageQueue0();
        $routeData = new FakeRouteData([$messageQueue]);
        $loadBalancer = new PublishingLoadBalancer($routeData);

        $result = $loadBalancer->takeMessageQueue([], 2);
        TestRunner::assertEqualsWithMessage(1, count($result), "Should return only 1 queue when only 1 exists");
    }

    public function testTakeMessageQueuesWithAllEndpointsIsolated()
    {
        $messageQueue = $this->fakePbMessageQueue0();
        $routeData = new FakeRouteData([$messageQueue]);
        $loadBalancer = new PublishingLoadBalancer($routeData);

        $brokerName = $messageQueue->getBroker()->getName();

        // When all endpoints are isolated, should still return queues (round two fallback)
        $result = $loadBalancer->takeMessageQueue([$brokerName], 1);
        TestRunner::assertNotNull($result, "Should return queues even when all endpoints are isolated");
        TestRunner::assertEqualsWithMessage(1, count($result), "Should return 1 queue");
    }

    public function testTakeMessageQueueRoundRobin()
    {
        $queues = [];
        for ($i = 0; $i < 3; $i++) {
            $address = new Address();
            $address->setHost('127.0.0.1');
            $address->setPort(8080 + $i);

            $endpoints = new V2Endpoints();
            $endpoints->setScheme(AddressScheme::IPv4);
            $endpoints->setAddresses([$address]);

            $broker = new Broker();
            $broker->setName("broker-{$i}");
            $broker->setEndpoints($endpoints);

            $topic = new Resource();
            $topic->setName('test-topic');

            $queue = new MessageQueue();
            $queue->setTopic($topic);
            $queue->setBroker($broker);
            $queue->setPermission(Permission::READ_WRITE);

            $queues[] = $queue;
        }

        $routeData = new FakeRouteData($queues);
        $loadBalancer = new PublishingLoadBalancer($routeData);

        // Take 1 queue at a time, should get different brokers over time
        $brokers = [];
        for ($i = 0; $i < 6; $i++) {
            $result = $loadBalancer->takeMessageQueue([], 1);
            if (!empty($result)) {
                $brokers[] = $result[0]->getBroker()->getName();
            }
        }

        $uniqueBrokers = array_unique($brokers);
        TestRunner::assertTrue(count($uniqueBrokers) >= 2, "Should distribute across multiple brokers");
    }
}

echo "=== PublishingLoadBalancerTest ===\n";
$test = new PublishingLoadBalancerTest();
$test->testTakeMessageQueueByMessageGroup();
echo "  [OK] testTakeMessageQueueByMessageGroup\n";
$test->testTakeTwoMessageQueuesWithSingleQueue();
echo "  [OK] testTakeTwoMessageQueuesWithSingleQueue\n";
$test->testTakeMessageQueuesWithAllEndpointsIsolated();
echo "  [OK] testTakeMessageQueuesWithAllEndpointsIsolated\n";
$test->testTakeMessageQueueRoundRobin();
echo "  [OK] testTakeMessageQueueRoundRobin\n";
