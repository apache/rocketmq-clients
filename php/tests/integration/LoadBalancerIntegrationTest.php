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

use Apache\Rocketmq\SubscriptionLoadBalancer;
use Apache\Rocketmq\PublishingLoadBalancer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\V2\QueryRouteResponse;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\Status as V2Status;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../../SubscriptionLoadBalancer.php';
require_once __DIR__ . '/../../PublishingLoadBalancer.php';

class LoadBalancerIntegrationTest extends IntegrationTestCase
{
    /**
     * Create a QueryRouteResponse with the given number of readable/writable queues.
     * Each queue is on a master broker with READ_WRITE permission.
     */
    private function createRouteResponseWithQueues(int $count): QueryRouteResponse
    {
        $routeResponse = new QueryRouteResponse();
        $status = new V2Status();
        $status->setCode(20000);
        $routeResponse->setStatus($status);

        $queues = [];
        for ($i = 0; $i < $count; $i++) {
            $broker = new Broker();
            $broker->setName("broker-{$i}");
            // ID defaults to 0 (MASTER_BROKER_ID), which is what load balancers require
            $brokerEndpoints = new Endpoints();
            $brokerEndpoints->setScheme(AddressScheme::IPv4);
            $address = new Address();
            $address->setHost('127.0.0.1');
            $address->setPort(8080 + $i);
            $brokerEndpoints->setAddresses([$address]);
            $broker->setEndpoints($brokerEndpoints);

            $mq = new MessageQueue();
            $topicResource = new Resource();
            $topicResource->setName('test-topic');
            $mq->setTopic($topicResource);
            $mq->setBroker($broker);
            $mq->setPermission(Permission::READ_WRITE);
            $queues[] = $mq;
        }
        $routeResponse->setMessageQueues($queues);
        return $routeResponse;
    }

    public function testSubscriptionLoadBalancerRoundRobin()
    {
        $routeResponse = $this->createRouteResponseWithQueues(3);
        $lb = new SubscriptionLoadBalancer($routeResponse);

        $mq1 = $lb->takeMessageQueue();
        $mq2 = $lb->takeMessageQueue();
        $mq3 = $lb->takeMessageQueue();
        $mq4 = $lb->takeMessageQueue(); // wraps around

        $this->assertNotNull($mq1);
        $this->assertNotNull($mq2);
        $this->assertNotNull($mq3);
        $this->assertNotNull($mq4);

        // Round-robin: 4th call should return same broker as 1st
        $this->assertEquals(
            $mq1->getBroker()->getName(),
            $mq4->getBroker()->getName()
        );
    }

    public function testSubscriptionLoadBalancerSingleQueue()
    {
        $routeResponse = $this->createRouteResponseWithQueues(1);
        $lb = new SubscriptionLoadBalancer($routeResponse);

        $mq1 = $lb->takeMessageQueue();
        $this->assertNotNull($mq1);

        // Same queue every time
        $mq2 = $lb->takeMessageQueue();
        $this->assertEquals($mq1->getBroker()->getName(), $mq2->getBroker()->getName());
    }

    public function testPublishingLoadBalancerWithExcludedBrokers()
    {
        $routeResponse = $this->createRouteResponseWithQueues(2);
        $lb = new PublishingLoadBalancer($routeResponse);

        // PublishingLoadBalancer::takeMessageQueue() accepts excluded broker names and count,
        // returning an array of MessageQueue objects.
        $result = $lb->takeMessageQueue([], 1);
        $this->assertCount(1, $result);
        $this->assertNotNull($result[0]);

        // Exclude the broker name from the first result so the second call
        // must return the other broker.
        $excludedBrokerName = $result[0]->getBroker()->getName();
        $result2 = $lb->takeMessageQueue([$excludedBrokerName], 1);
        $this->assertCount(1, $result2);
        $this->assertNotEquals(
            $excludedBrokerName,
            $result2[0]->getBroker()->getName()
        );
    }

    public function testPublishingLoadBalancerEmptyQueues()
    {
        $routeResponse = new QueryRouteResponse();
        $status = new V2Status();
        $status->setCode(20000);
        $routeResponse->setStatus($status);
        $routeResponse->setMessageQueues([]);

        // PublishingLoadBalancer throws when no writable queues are available
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/No writable message queue/');
        new PublishingLoadBalancer($routeResponse);
    }
}
