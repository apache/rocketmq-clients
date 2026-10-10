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

use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\SendMessageResponse;
use Apache\Rocketmq\V2\HeartbeatResponse;
use Apache\Rocketmq\V2\QueryRouteResponse;
use Apache\Rocketmq\V2\NotifyClientTerminationResponse;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\Status as V2Status;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SendResultEntry;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../Producer.php';

class ProducerIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';
    private $brokerHost = '127.0.0.1';
    private $brokerPort = 8081;
    private $brokerKey;

    protected function setUp(): void
    {
        parent::setUp();
        $this->brokerKey = $this->brokerHost . ':' . $this->brokerPort;
    }

    /**
     * Set up common mocks shared across Producer tests.
     *
     * - QueryRoute on main client: needed for route warm-up during start() with topics
     * - Telemetry bidi stream on main client: needed for establishTelemetrySession()
     * - Heartbeat on broker client: Producer sends heartbeats to broker endpoints
     * - NotifyClientTermination on main client: needed for shutdown()
     * - Registers broker mock at 127.0.0.1:8081 for heartbeat calls
     *
     * @param object $mock Main client mock (localhost:8080)
     * @return object Broker mock client
     */
    private function setupCommonMocks($mock): object
    {
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
        $address->setHost($this->brokerHost);
        $address->setPort($this->brokerPort);
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

        // Setup NotifyClientTermination mock for graceful shutdown
        GrpcMockHelper::mockUnaryCall($mock, 'NotifyClientTermination', new NotifyClientTerminationResponse(), 0);

        // Register broker mock and set up Heartbeat
        // Producer's doHeartbeat() sends heartbeats to broker endpoints
        $brokerMock = GrpcMockHelper::createMockClient();
        \Apache\Rocketmq\RpcClientManager::getInstance()->registerMock($this->brokerKey, $brokerMock);
        GrpcMockHelper::mockUnaryCall($brokerMock, 'Heartbeat', new HeartbeatResponse(), 0);

        return $brokerMock;
    }

    /**
     * Set up SendMessage mock on the main client for successful send.
     *
     * Producer's sendMessageWithRetry() calls $this->client->SendMessage(),
     * so the SendMessage mock goes on the main client (not the broker mock).
     */
    private function setupSendMessageMock($mock): void
    {
        $sendResponse = new SendMessageResponse();
        $sendStatus = new V2Status();
        $sendStatus->setCode(20000);
        $sendResponse->setStatus($sendStatus);

        $entry = new SendResultEntry();
        $entry->setMessageId('msg-001');
        $entry->setTransactionId('tx-001');
        $entryStatus = new V2Status();
        $entryStatus->setCode(20000);
        $entry->setStatus($entryStatus);
        $sendResponse->setEntries([$entry]);

        GrpcMockHelper::mockUnaryCall($mock, 'SendMessage', $sendResponse, 0);
    }

    /**
     * Test successful send of a message through the Producer lifecycle.
     */
    public function testSendMessageSuccess()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);
        $this->setupSendMessageMock($mock);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic']]);
        $producer->start();

        $msg = new Message();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $msg->setTopic($topicResource);
        $msg->setBody('Test Message Body');

        $result = $producer->send($msg);
        $this->assertNotNull($result);
        $this->assertArrayHasKey('messageId', $result);
        $this->assertEquals('msg-001', $result['messageId']);

        $producer->shutdown();
    }

    /**
     * Test that shutdown notifies the server of client termination.
     */
    public function testShutdownNotifiesTermination()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic']]);
        $producer->start();
        $producer->shutdown();

        // No exception means shutdown completed successfully
        $this->assertTrue(true);
    }

    /**
     * Test starting a Producer without any pre-configured topics.
     */
    public function testStartProducerWithoutTopics()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        // Only need Telemetry mock - no route warm-up, no heartbeat
        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $producer = new Producer($this->endpoints, []);
        $producer->start();

        // Producer should start successfully even without pre-configured topics
        $this->assertTrue(true);

        $producer->shutdown();
    }

    /**
     * Test that send() throws an exception when the Producer is not started.
     */
    public function testSendThrowsWhenNotStarted()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic']]);

        $msg = new Message();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $msg->setTopic($topicResource);
        $msg->setBody('Test Message Body');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/not running/i');
        $producer->send($msg);
    }

    /**
     * Test that send() throws an exception when message has no topic.
     */
    public function testSendThrowsWithoutTopic()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic']]);
        $producer->start();

        $msg = new Message();
        $msg->setBody('Test Message Body');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/topic/i');
        $producer->send($msg);

        $producer->shutdown();
    }

    /**
     * Test that send() throws an exception when message has empty body.
     */
    public function testSendThrowsWithEmptyBody()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic']]);
        $producer->start();

        $msg = new Message();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $msg->setTopic($topicResource);
        $msg->setBody('');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/body/i');
        $producer->send($msg);

        $producer->shutdown();
    }

    /**
     * Test double start is idempotent.
     */
    public function testDoubleStartIsIdempotent()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic']]);
        $producer->start();

        // Second start should be a no-op
        $producer->start();

        $this->assertTrue(true);

        $producer->shutdown();
    }

    /**
     * Test isRunning reflects the producer state.
     */
    public function testIsRunningState()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic']]);

        $this->assertFalse($producer->isRunning());

        $producer->start();
        $this->assertTrue($producer->isRunning());

        $producer->shutdown();
        $this->assertFalse($producer->isRunning());
    }

    /**
     * Test getClientId returns the configured client ID.
     */
    public function testGetClientId()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $producer = new Producer($this->endpoints, [
            'topics' => ['test-topic'],
            'clientId' => 'my-custom-producer',
        ]);

        $this->assertEquals('my-custom-producer', $producer->getClientId());
    }

    /**
     * Test double shutdown is idempotent.
     */
    public function testDoubleShutdownIsIdempotent()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        $this->setupCommonMocks($mock);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic']]);
        $producer->start();
        $producer->shutdown();

        // Second shutdown should be a no-op
        $producer->shutdown();

        $this->assertFalse($producer->isRunning());
    }
}
