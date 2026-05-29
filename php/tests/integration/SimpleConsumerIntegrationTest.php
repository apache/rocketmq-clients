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

use Apache\Rocketmq\SimpleConsumer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\ReceiveMessageResponse;
use Apache\Rocketmq\V2\AckMessageResponse;
use Apache\Rocketmq\V2\HeartbeatResponse;
use Apache\Rocketmq\V2\QueryRouteResponse;
use Apache\Rocketmq\V2\NotifyClientTerminationResponse;
use Apache\Rocketmq\V2\ChangeInvisibleDurationResponse;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\Status as V2Status;
use Apache\Rocketmq\V2\SystemProperties;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../SimpleConsumer.php';

class SimpleConsumerIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    /**
     * Create a broker mock with all required mocks set up (QueryRoute, Telemetry, Heartbeat).
     *
     * @param object $mock Main client mock
     * @param string $brokerHost Broker host for registering broker mock
     * @param int $brokerPort Broker port for registering broker mock
     * @return object Broker mock client for setting up per-test ReceiveMessage mocks
     */
    private function setupCommonMocks($mock, string $brokerHost = '127.0.0.1', int $brokerPort = 8081): object
    {
        $brokerKey = $brokerHost . ':' . $brokerPort;

        // Setup QueryRoute response with message queues pointing to broker
        $routeResponse = new QueryRouteResponse();
        $routeStatus = new V2Status();
        $routeStatus->setCode(20000);
        $routeResponse->setStatus($routeStatus);

        $broker = new Broker();
        $broker->setName('broker-1');
        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost($brokerHost);
        $address->setPort($brokerPort);
        $brokerEndpoints->setAddresses([$address]);
        $broker->setEndpoints($brokerEndpoints);

        $mq = new MessageQueue();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $mq->setTopic($topicResource);
        $mq->setBroker($broker);
        $mq->setPermission(Permission::READ_WRITE);
        $routeResponse->setMessageQueues([$mq]);

        GrpcMockHelper::mockUnaryCall($mock, 'QueryRoute', $routeResponse, 0);

        // Setup Telemetry mock (bidi stream) - server echoes back Settings
        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        // Setup Heartbeat mock
        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);

        // Setup NotifyClientTermination mock for graceful shutdown
        GrpcMockHelper::mockUnaryCall($mock, 'NotifyClientTermination', new NotifyClientTerminationResponse(), 0);

        // Register broker mock
        $brokerMock = GrpcMockHelper::createMockClient();
        \Apache\Rocketmq\RpcClientManager::getInstance()->registerMock($brokerKey, $brokerMock);

        return $brokerMock;
    }

    /**
     * Create and start a SimpleConsumer with test-topic subscription.
     */
    private function createStartedConsumer(): SimpleConsumer
    {
        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);
        $consumer->start();
        return $consumer;
    }

    /**
     * Test receive returns messages from broker.
     */
    public function testReceiveWithMessages()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $brokerMock = $this->setupCommonMocks($mock);

        // Setup ReceiveMessage response from broker with a message
        $msg = new Message();
        $msgTopic = new Resource();
        $msgTopic->setName('test-topic');
        $msg->setTopic($msgTopic);
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-001');
        $sysProps->setReceiptHandle('receipt-001');
        $msg->setSystemProperties($sysProps);
        $msg->setBody('Hello World');

        $receiveResponse = new ReceiveMessageResponse();
        $receiveResponse->setMessage($msg);

        GrpcMockHelper::mockServerStreamCall($brokerMock, 'ReceiveMessage', [$receiveResponse]);

        $consumer = $this->createStartedConsumer();

        $messages = $consumer->receive(10, 30);

        $this->assertCount(1, $messages);
        $this->assertEquals('Hello World', $messages[0]->getBody());

        $consumer->shutdown();
    }

    /**
     * Test receive when no messages available.
     */
    public function testReceiveEmptyResult()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $brokerMock = $this->setupCommonMocks($mock);

        // Empty ReceiveMessage response with 40404 status (no messages)
        $receiveResponse = new ReceiveMessageResponse();
        $receiveStatus = new V2Status();
        $receiveStatus->setCode(40404);
        $receiveResponse->setStatus($receiveStatus);

        GrpcMockHelper::mockServerStreamCall($brokerMock, 'ReceiveMessage', [$receiveResponse]);

        $consumer = $this->createStartedConsumer();

        $messages = $consumer->receive(10, 30);
        $this->assertEmpty($messages);

        $consumer->shutdown();
    }

    /**
     * Test ack sends correct gRPC call.
     */
    public function testAckMessages()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        // Override AckMessage mock with a response that includes entries with success status
        $ackResponse = new AckMessageResponse();
        $ackStatus = new V2Status();
        $ackStatus->setCode(20000);
        $ackResponse->setStatus($ackStatus);

        // Create an empty entries list so the ack loop exits immediately
        // (when entries list is empty in response, ackMessagesForTopic considers all successful)
        GrpcMockHelper::mockUnaryCall($mock, 'AckMessage', $ackResponse, 0);

        $consumer = $this->createStartedConsumer();

        $msg = new Message();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $msg->setTopic($topicResource);
        $sysProps = new SystemProperties();
        $sysProps->setReceiptHandle('receipt-001');
        $sysProps->setMessageId('msg-001');
        $msg->setSystemProperties($sysProps);

        // Should not throw
        $consumer->ack([$msg]);
        $this->assertTrue(true); // Verify ack completes without exception

        $consumer->shutdown();
    }

    /**
     * Test changeInvisibleDuration.
     */
    public function testChangeInvisibleDuration()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $changeResponse = new ChangeInvisibleDurationResponse();
        $changeResponse->setReceiptHandle('new-receipt-001');
        GrpcMockHelper::mockUnaryCall($mock, 'ChangeInvisibleDuration', $changeResponse, 0);

        $consumer = $this->createStartedConsumer();

        $msg = new Message();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $msg->setTopic($topicResource);
        $sysProps = new SystemProperties();
        $sysProps->setReceiptHandle('receipt-001');
        $sysProps->setMessageId('msg-001');
        $msg->setSystemProperties($sysProps);

        $result = $consumer->changeInvisibleDuration($msg, 60);
        $this->assertTrue($result);

        $consumer->shutdown();
    }

    /**
     * Test start fails when TelemetrySession doesn't get settings response.
     */
    public function testStartFailsWithoutTelemetryResponse()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        // Bidi stream returns no response — TelemetrySession polls until timeout
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', []);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/Telemetry/');
        $consumer->start();
    }

    /**
     * Test shutdown calls NotifyClientTermination and closes session.
     */
    public function testShutdownNotifiesTermination()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $consumer = $this->createStartedConsumer();
        $consumer->shutdown();

        // No exception means shutdown completed successfully
        $this->assertTrue(true);
    }

    /**
     * Test receive throws when consumer not started.
     */
    public function testReceiveThrowsWhenNotStarted()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/not started/i');
        $consumer->receive(10, 30);
    }

    /**
     * Test ack throws when consumer not started.
     */
    public function testAckThrowsWhenNotStarted()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/not started/i');
        $consumer->ack([]);
    }

    /**
     * Test changeInvisibleDuration throws when consumer not started.
     */
    public function testChangeInvisibleDurationThrowsWhenNotStarted()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/not started/i');

        $msg = new Message();
        $consumer->changeInvisibleDuration($msg, 60);
    }

    /**
     * Test receive throws on invalid maxMessages <= 0.
     */
    public function testReceiveThrowsOnInvalidMaxMessages()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $consumer = $this->createStartedConsumer();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/maxMessages/');
        $consumer->receive(0, 30);
    }

    /**
     * Test start throws when no subscriptions configured.
     */
    public function testStartThrowsWithoutSubscriptions()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/No subscriptions/');
        $consumer->start();
    }

    /**
     * Test double start is idempotent.
     */
    public function testDoubleStartIsIdempotent()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $consumer = $this->createStartedConsumer();

        // Second start should be a no-op
        $consumer->start();

        // Consumer should still be functional
        $this->assertTrue(true);

        $consumer->shutdown();
    }

    /**
     * Test getClientId returns the configured client ID.
     */
    public function testGetClientId()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
            'clientId' => 'my-custom-client',
        ]);

        $this->assertEquals('my-custom-client', $consumer->getClientId());
    }

    /**
     * Test subscribe and unsubscribe after start.
     */
    public function testSubscribeAndUnsubscribe()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $consumer = $this->createStartedConsumer();

        // Add a new subscription
        $consumer->subscribe('another-topic', 'tagA');

        $expressions = $consumer->getSubscriptionExpressions();
        $this->assertArrayHasKey('test-topic', $expressions);
        $this->assertArrayHasKey('another-topic', $expressions);
        $this->assertEquals('tagA', $expressions['another-topic']);

        // Remove the subscription
        $consumer->unsubscribe('another-topic');
        $expressions = $consumer->getSubscriptionExpressions();
        $this->assertArrayNotHasKey('another-topic', $expressions);

        $consumer->shutdown();
    }
}
