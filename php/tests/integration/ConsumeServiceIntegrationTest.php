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
use Apache\Rocketmq\StandardConsumeService;
use Apache\Rocketmq\FifoConsumeService;
use Apache\Rocketmq\ProcessQueue;
use Apache\Rocketmq\ConsumeResult;
use Apache\Rocketmq\ConsumeResultSuspend;
use Apache\Rocketmq\ConsumerInterface;
use Apache\Rocketmq\ExponentialBackoffRetryPolicy;
use Apache\Rocketmq\MessageView;
use Apache\Rocketmq\SessionCredentials;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\Logger;
use Apache\Rocketmq\V2\AckMessageResponse;
use Apache\Rocketmq\V2\AckMessageResultEntry;
use Apache\Rocketmq\V2\ChangeInvisibleDurationResponse;
use Apache\Rocketmq\V2\ForwardMessageToDeadLetterQueueResponse;
use Apache\Rocketmq\V2\Status;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\FilterType;
use Apache\Rocketmq\V2\ReceiveMessageResponse;
use Apache\Rocketmq\RpcClientManager;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../ConsumeService.php';
require_once __DIR__ . '/../../ProcessQueue.php';
require_once __DIR__ . '/../../ConsumerInterface.php';
require_once __DIR__ . '/../../Logger.php';

class ConsumeServiceIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    /**
     * Helper to create a consumer object that ConsumeService can use.
     *
     * Implements ConsumerInterface with all methods required by ConsumeService
     * for ack, nack, forwardToDeadLetterQueue, and consume operations.
     *
     * @param string $endpoints Server endpoint for mock registration
     * @return ConsumerInterface Consumer with all methods ConsumeService requires
     */
    private function createTestConsumer(string $endpoints): ConsumerInterface
    {
        $mock = GrpcMockHelper::createMockClient();
        RpcClientManager::getInstance()->registerMock($endpoints, $mock);

        return new class($endpoints) implements ConsumerInterface {
            private string $endpoints;
            private string $group = 'test-group';
            private string $namespace = 'test-ns';

            public function __construct(string $endpoints)
            {
                $this->endpoints = $endpoints;
            }

            public function getClientId(): string
            {
                return 'test-client-id';
            }

            public function getTopicResource(string $topic): Resource
            {
                $resource = new Resource();
                $resource->setName($topic);
                return $resource;
            }

            public function getNamespace(): string
            {
                return $this->namespace;
            }

            public function getGroupResourceWithNamespace(): Resource
            {
                $resource = new Resource();
                $ns = $this->namespace;
                $name = $ns ? $ns . '%' . $this->group : $this->group;
                $resource->setName($name);
                return $resource;
            }

            public function getSessionCredentials(): ?SessionCredentials
            {
                return null;
            }

            public function buildMetadata(?int $timeoutMs = null): array
            {
                return ['x-rocketmq-client-id' => 'test-client-id'];
            }

            public function ackMessage(MessageView $messageView): bool
            {
                return true;
            }

            public function nackMessage(MessageView $messageView, int $deliveryAttempt = 1, ?int $invisibleDuration = null): bool
            {
                return true;
            }

            public function getAwaitDuration(): int
            {
                return 30;
            }

            public function getReceiveBatchSize(): int
            {
                return 32;
            }

            public function getCacheMessageCountThresholdPerQueue(): int
            {
                return 1000;
            }

            public function getCacheMessageBytesThresholdPerQueue(): int
            {
                return 1048576;
            }

            public function executeInterceptors(string $hookPoint, array $context): void
            {
                // no-op for tests
            }

            public function getRetryPolicy(): ?ExponentialBackoffRetryPolicy
            {
                return null;
            }

            public function getConsumeService(): ?ConsumeService
            {
                return null;
            }
        };
    }

    /**
     * Helper to create a consumer object (legacy alias).
     *
     * @return ConsumerInterface Consumer with all methods ConsumeService requires
     */
    private function createMockConsumer(): ConsumerInterface
    {
        return $this->createTestConsumer($this->endpoints);
    }

    // Test 1: StandardConsumeService construction
    public function testStandardConsumeServiceConstruction()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };

        $service = new StandardConsumeService($logger, $listener, $consumer);
        $this->assertNotNull($service);
    }

    // Test 2: FifoConsumeService construction with/without accelerator
    public function testFifoConsumeServiceConstruction()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };

        $service = new FifoConsumeService($logger, $listener, $consumer, true);
        $this->assertNotNull($service);
    }

    // Test 3: consumeMessage returns SUCCESS
    public function testConsumeMessageSuccess()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $consumer);

        $msg = new Message();
        $msg->setBody('test');
        $topic = new Resource();
        $topic->setName('test-topic');
        $msg->setTopic($topic);
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-001');
        $msg->setSystemProperties($sysProps);

        $messageView = new \Apache\Rocketmq\MessageView($msg);
        $result = $service->consumeMessage($messageView);
        $this->assertEquals(ConsumeResult::SUCCESS, $result);
    }

    // Test 4: consumeMessage returns FAILURE
    public function testConsumeMessageFailure()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::FAILURE; };
        $service = new StandardConsumeService($logger, $listener, $consumer);

        $msg = new Message();
        $msg->setBody('test');
        $topic = new Resource();
        $topic->setName('test-topic');
        $msg->setTopic($topic);
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-002');
        $msg->setSystemProperties($sysProps);

        $messageView = new \Apache\Rocketmq\MessageView($msg);
        $result = $service->consumeMessage($messageView);
        $this->assertEquals(ConsumeResult::FAILURE, $result);
    }

    // Test 5: consumeMessage handles ConsumeResultSuspend
    public function testConsumeMessageSuspend()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $suspendResult = ConsumeResultSuspend::of(5000);
        $listener = function ($msg) use ($suspendResult) { return $suspendResult; };
        $service = new StandardConsumeService($logger, $listener, $consumer);

        $msg = new Message();
        $msg->setBody('test');
        $topic = new Resource();
        $topic->setName('test-topic');
        $msg->setTopic($topic);
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-003');
        $msg->setSystemProperties($sysProps);

        $messageView = new \Apache\Rocketmq\MessageView($msg);
        $result = $service->consumeMessage($messageView);
        $this->assertInstanceOf(ConsumeResultSuspend::class, $result);
        $this->assertEquals(5000, $result->getSuspendTimeMs());
    }

    // Test 6: consumeMessage catches exceptions and returns FAILURE
    public function testConsumeMessageCatchesException()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { throw new \RuntimeException('test error'); };
        $service = new StandardConsumeService($logger, $listener, $consumer);

        $msg = new Message();
        $msg->setBody('test');
        $topic = new Resource();
        $topic->setName('test-topic');
        $msg->setTopic($topic);
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-004');
        $msg->setSystemProperties($sysProps);

        $messageView = new \Apache\Rocketmq\MessageView($msg);
        $result = $service->consumeMessage($messageView);
        $this->assertEquals(ConsumeResult::FAILURE, $result);
    }

    // Test 7: ackMessage with no receipt handle returns false
    public function testAckMessageNoReceiptHandle()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $consumer);

        $msg = new Message();
        $msg->setBody('test');
        $topic = new Resource();
        $topic->setName('test-topic');
        $msg->setTopic($topic);
        // No system properties -> no receipt handle

        $messageView = new \Apache\Rocketmq\MessageView($msg);
        $result = $service->ackMessage($messageView);
        $this->assertFalse($result);
    }

    // Test 8: ackMessage success with mock
    public function testAckMessageSuccess()
    {
        $endpoints = '127.0.0.1:8080';
        $consumerObj = $this->createTestConsumer($endpoints);

        $ackResponse = new AckMessageResponse();
        $ackStatus = new Status();
        $ackStatus->setCode(20000);
        $ackResponse->setStatus($ackStatus);
        GrpcMockHelper::mockUnaryCall(
            RpcClientManager::getInstance()->getClient($endpoints),
            'AckMessage',
            $ackResponse,
            0
        );

        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $consumerObj);

        $msg = new Message();
        $msg->setBody('test');
        $topic = new Resource();
        $topic->setName('test-topic');
        $msg->setTopic($topic);
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-ack-001');
        $sysProps->setReceiptHandle('receipt-handle-001');
        $msg->setSystemProperties($sysProps);

        // Set endpoints on messageView for getBrokerClient routing
        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost('127.0.0.1');
        $address->setPort(8080);
        $brokerEndpoints->setAddresses([$address]);

        $messageView = new \Apache\Rocketmq\MessageView($msg, 'receipt-handle-001', $brokerEndpoints);
        $result = $service->ackMessage($messageView);
        $this->assertTrue($result);
    }

    // Test 9: nackMessage with no receipt handle returns false
    public function testNackMessageNoReceiptHandle()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $consumer);

        $msg = new Message();
        $msg->setBody('test');

        $messageView = new \Apache\Rocketmq\MessageView($msg);
        $result = $service->nackMessage($messageView);
        $this->assertFalse($result);
    }

    // Test 10: forwardToDeadLetterQueue no receipt handle
    public function testForwardToDlqNoReceiptHandle()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $consumer);

        $msg = new Message();
        $msg->setBody('test');

        $messageView = new \Apache\Rocketmq\MessageView($msg);
        $result = $service->forwardToDeadLetterQueue($messageView);
        $this->assertFalse($result);
    }

    // Test 11: StandardConsumeService consume with empty messages
    public function testStandardConsumeServiceConsumeEmpty()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $consumer);

        // Create a ProcessQueue that has no cached messages
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

        $pq = new ProcessQueue($consumer, $mq, '*');

        // Should not throw with empty messages
        $service->consume($pq);
        $this->assertTrue(true);
    }

    // Test 12: FifoConsumeService consume with empty messages
    public function testFifoConsumeServiceConsumeEmpty()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };
        $service = new FifoConsumeService($logger, $listener, $consumer, false);

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

        $pq = new ProcessQueue($consumer, $mq, '*');

        // Should not throw with empty messages
        $service->consume($pq);
        $this->assertTrue(true);
    }

    // Test 13: FifoConsumeService with accelerator
    public function testFifoConsumeServiceWithAccelerator()
    {
        $consumer = $this->createMockConsumer();
        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };
        $service = new FifoConsumeService($logger, $listener, $consumer, true);
        $this->assertNotNull($service);
    }

    // Test 14: ackMessage retry on server error
    public function testAckMessageRetryOnServerError()
    {
        $endpoints = '127.0.0.2:8080';
        $consumerObj = $this->createTestConsumer($endpoints);

        // Response with server error code
        $errorResponse = new AckMessageResponse();
        $errorStatus = new Status();
        $errorStatus->setCode(50001); // Internal server error
        $errorResponse->setStatus($errorStatus);

        GrpcMockHelper::mockUnaryCall(
            RpcClientManager::getInstance()->getClient($endpoints),
            'AckMessage',
            $errorResponse,
            0
        );

        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $consumerObj);

        $msg = new Message();
        $msg->setBody('test');
        $topic = new Resource();
        $topic->setName('test-topic');
        $msg->setTopic($topic);
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-retry-001');
        $sysProps->setReceiptHandle('receipt-retry-001');
        $msg->setSystemProperties($sysProps);

        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost('127.0.0.2');
        $address->setPort(8080);
        $brokerEndpoints->setAddresses([$address]);

        $messageView = new \Apache\Rocketmq\MessageView($msg, 'receipt-retry-001', $brokerEndpoints);
        $result = $service->ackMessage($messageView);
        // Should exhaust retries and return false
        $this->assertFalse($result);
    }

    // Test 15: ackMessage gives up on invalid receipt handle (40003)
    public function testAckMessageGivesUpOnInvalidReceiptHandle()
    {
        $endpoints = '127.0.0.3:8080';
        $consumerObj = $this->createTestConsumer($endpoints);

        $errorResponse = new AckMessageResponse();
        $errorStatus = new Status();
        $errorStatus->setCode(40003); // INVALID_RECEIPT_HANDLE
        $errorResponse->setStatus($errorStatus);

        GrpcMockHelper::mockUnaryCall(
            RpcClientManager::getInstance()->getClient($endpoints),
            'AckMessage',
            $errorResponse,
            0
        );

        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $consumerObj);

        $msg = new Message();
        $msg->setBody('test');
        $topic = new Resource();
        $topic->setName('test-topic');
        $msg->setTopic($topic);
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-invalid-001');
        $sysProps->setReceiptHandle('receipt-invalid-001');
        $msg->setSystemProperties($sysProps);

        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost('127.0.0.3');
        $address->setPort(8080);
        $brokerEndpoints->setAddresses([$address]);

        $messageView = new \Apache\Rocketmq\MessageView($msg, 'receipt-invalid-001', $brokerEndpoints);
        $result = $service->ackMessage($messageView);
        // Should give up immediately on 40003
        $this->assertFalse($result);
    }

    // Test 16: forwardToDeadLetterQueue success with mock
    public function testForwardToDlqSuccess()
    {
        $endpoints = '127.0.0.4:8080';
        $consumerObj = $this->createTestConsumer($endpoints);

        $dlqResponse = new ForwardMessageToDeadLetterQueueResponse();
        $dlqStatus = new Status();
        $dlqStatus->setCode(20000);
        $dlqResponse->setStatus($dlqStatus);
        GrpcMockHelper::mockUnaryCall(
            RpcClientManager::getInstance()->getClient($endpoints),
            'ForwardMessageToDeadLetterQueue',
            $dlqResponse,
            0
        );

        $logger = Logger::getInstance('ConsumeServiceTest');
        $listener = function ($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $consumerObj);

        $msg = new Message();
        $msg->setBody('test');
        $topic = new Resource();
        $topic->setName('test-topic');
        $msg->setTopic($topic);
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-dlq-001');
        $sysProps->setReceiptHandle('receipt-dlq-001');
        $msg->setSystemProperties($sysProps);

        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost('127.0.0.4');
        $address->setPort(8080);
        $brokerEndpoints->setAddresses([$address]);

        $messageView = new \Apache\Rocketmq\MessageView($msg, 'receipt-dlq-001', $brokerEndpoints);
        $result = $service->forwardToDeadLetterQueue($messageView);
        $this->assertTrue($result);
    }
}
