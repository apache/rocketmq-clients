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
require_once __DIR__ . '/../PublishingLoadBalancer.php';

use Apache\Rocketmq\PublishingLoadBalancer;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints as V2Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\MessageType as V2MessageType;

class FakeRouteData {
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

class PublishingLoadBalancerTest extends TestCase
{
    /**
     * Build $count writable master-broker queues named broker-0..broker-{count-1}.
     */
    private function buildQueues(int $count): array
    {
        $queues = [];
        for ($i = 0; $i < $count; $i++) {
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
        return $queues;
    }

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
        $this->assertNotNull($result, "Should return a message queue");
    }

    /**
     * Cross-client FIFO queue selection: expected indices equal Java's
     * LongMath.mod(Hashing.sipHash24().hashBytes(group.getBytes(UTF_8)).asLong(), queueCount)
     * (and the Node.js client's siphash24-based selection), so the same message
     * group must map to the same queue in every language client.
     */
    public function testTakeMessageQueueByMessageGroupMatchesJavaAndNodeClients()
    {
        $expectedByQueueCount = [
            3 => [
                'message-group-0' => 1,
                'message-group-1' => 1,
                'fifo-group' => 2,
                'order-12345' => 2,
                'RocketMQ' => 0,
            ],
            5 => [
                'message-group-0' => 0,
                'message-group-1' => 1,
                'fifo-group' => 4,
                'order-12345' => 3,
                'RocketMQ' => 0,
            ],
        ];

        foreach ($expectedByQueueCount as $queueCount => $expectations) {
            $loadBalancer = new PublishingLoadBalancer(new FakeRouteData($this->buildQueues($queueCount)));
            foreach ($expectations as $group => $expectedIndex) {
                $selected = $loadBalancer->takeMessageQueueByMessageGroup($group);
                $this->assertSame(
                    "broker-{$expectedIndex}",
                    $selected->getBroker()->getName(),
                    "Group '{$group}' with {$queueCount} queues should select queue {$expectedIndex}"
                );
                // Deterministic: repeated selection must return the same queue
                $again = $loadBalancer->takeMessageQueueByMessageGroup($group);
                $this->assertSame($selected, $again);
            }
        }
    }

    public function testTakeTwoMessageQueuesWithSingleQueue()
    {
        $messageQueue = $this->fakePbMessageQueue0();
        $routeData = new FakeRouteData([$messageQueue]);
        $loadBalancer = new PublishingLoadBalancer($routeData);

        $result = $loadBalancer->takeMessageQueue([], 2);
        $this->assertEquals(1, count($result), "Should return only 1 queue when only 1 exists");
    }

    public function testTakeMessageQueuesWithAllEndpointsIsolated()
    {
        $messageQueue = $this->fakePbMessageQueue0();
        $routeData = new FakeRouteData([$messageQueue]);
        $loadBalancer = new PublishingLoadBalancer($routeData);

        $brokerName = $messageQueue->getBroker()->getName();

        // When all endpoints are isolated, should still return queues (round two fallback)
        $result = $loadBalancer->takeMessageQueue([$brokerName], 1);
        $this->assertNotNull($result, "Should return queues even when all endpoints are isolated");
        $this->assertEquals(1, count($result), "Should return 1 queue");
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
        $this->assertTrue(count($uniqueBrokers) >= 2, "Should distribute across multiple brokers");
    }
}

