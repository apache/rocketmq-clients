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

require_once __DIR__ . '/../SendMessageHandler.php';

use Apache\Rocketmq\SendMessageHandler;
use Apache\Rocketmq\ProducerSettings;
use Apache\Rocketmq\MessageValidator;
use Apache\Rocketmq\PublishingRouteManager;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\SendMessageRequest;
use Apache\Rocketmq\V2\SendMessageResponse;
use Apache\Rocketmq\V2\SendResultEntry;
use Apache\Rocketmq\V2\Status;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\MessageType;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\Resource;

/**
 * Fake gRPC unary call returning a preset [response, status] pair.
 */
class FakeSendCall
{
    public function __construct(private $response, private $status)
    {
    }

    public function wait(): array
    {
        return [$this->response, $this->status];
    }
}

/**
 * Tests for SendMessageHandler::sendMessageWithRetry() covering:
 * - transaction (half) messages keep the TRANSACTION type when the request is
 *   rebuilt for a retry
 * - the result reports the endpoint of the queue that actually succeeded
 */
class SendMessageHandlerRetryTest extends TestCase
{
    /** @var SendMessageRequest[] */
    private array $capturedRequests = [];

    private function buildQueue(string $brokerName, string $host, int $port): MessageQueue
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $address = new Address();
        $address->setHost($host);
        $address->setPort($port);
        $endpoints = new Endpoints();
        $endpoints->setAddresses([$address]);

        $broker = new Broker();
        $broker->setName($brokerName);
        $broker->setId(0);
        $broker->setEndpoints($endpoints);

        $queue = new MessageQueue();
        $queue->setTopic($topic);
        $queue->setId(0);
        $queue->setBroker($broker);

        return $queue;
    }

    private function buildMessage(): Message
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $message = new Message();
        $message->setTopic($topic);
        $message->setBody('half message body');

        return $message;
    }

    private function successResponse(): array
    {
        $status = new \stdClass();
        $status->code = 0;
        $status->details = '';

        $respStatus = new Status();
        $respStatus->setCode(20000);

        $entryStatus = new Status();
        $entryStatus->setCode(20000);

        $entry = new SendResultEntry();
        $entry->setMessageId('msg-001');
        $entry->setTransactionId('tx-001');
        $entry->setStatus($entryStatus);

        $response = new SendMessageResponse();
        $response->setStatus($respStatus);
        $response->setEntries([$entry]);

        return [$response, $status];
    }

    private function failedTransportStatus(): array
    {
        $status = new \stdClass();
        $status->code = 14; // UNAVAILABLE
        $status->details = 'transient transport failure';
        return [null, $status];
    }

    /**
     * Build a handler whose client fails the first N attempts and then succeeds,
     * capturing every request for inspection.
     */
    private function buildHandler(int $failuresBeforeSuccess): SendMessageHandler
    {
        $this->capturedRequests = [];
        $callCount = 0;

        $client = $this->createMock(MessagingServiceClient::class);
        $client->method('SendMessage')->willReturnCallback(
            function ($request) use (&$callCount, $failuresBeforeSuccess) {
                $this->capturedRequests[] = $request;
                $callCount++;
                if ($callCount <= $failuresBeforeSuccess) {
                    [$response, $status] = $this->failedTransportStatus();
                } else {
                    [$response, $status] = $this->successResponse();
                }
                return new FakeSendCall($response, $status);
            }
        );

        $routeManager = $this->createMock(PublishingRouteManager::class);

        return new SendMessageHandler(
            $client,
            new ProducerSettings('fake-endpoint:8081', ['maxAttempts' => 3]),
            new MessageValidator(4194304, false),
            $routeManager,
            function (string $hookPoint, array $context): void {},
            function (?int $timeoutMs): array { return []; },
            function (?int $overrideTimeout): array { return []; },
            function (string $operation): int { return 60_000_000; }
        );
    }

    /**
     * A transaction (half) message that fails transiently must be retried with
     * the TRANSACTION message type, never as a normal visible message.
     */
    public function testRetryPreservesTransactionMessageType()
    {
        $handler = $this->buildHandler(1);
        $message = $this->buildMessage();
        $candidates = [
            $this->buildQueue('broker-0', 'host-0', 8081),
            $this->buildQueue('broker-1', 'host-1', 8081),
            $this->buildQueue('broker-2', 'host-2', 8081),
        ];

        $request = $handler->wrapTransactionMessageRequest([$message], $candidates[0]);
        $result = $handler->sendMessageWithRetry($request, $message, $candidates, 3, true);

        $this->assertCount(2, $this->capturedRequests, "Should have failed once and retried once");
        foreach ($this->capturedRequests as $i => $captured) {
            $sentType = $captured->getMessages()[0]->getSystemProperties()->getMessageType();
            $this->assertEquals(
                MessageType::TRANSACTION,
                $sentType,
                "Attempt " . ($i + 1) . " must carry the TRANSACTION message type"
            );
        }
        $this->assertEquals('tx-001', $result['transactionId']);
    }

    /**
     * Without txEnabled, retried requests keep the NORMAL message type.
     */
    public function testRetryKeepsNormalMessageTypeByDefault()
    {
        $handler = $this->buildHandler(1);
        $message = $this->buildMessage();
        $candidates = [
            $this->buildQueue('broker-0', 'host-0', 8081),
            $this->buildQueue('broker-1', 'host-1', 8081),
            $this->buildQueue('broker-2', 'host-2', 8081),
        ];

        $request = $handler->wrapSendMessageRequest([$message], $candidates[0]);
        $handler->sendMessageWithRetry($request, $message, $candidates, 3);

        $this->assertCount(2, $this->capturedRequests);
        $retriedType = $this->capturedRequests[1]->getMessages()[0]->getSystemProperties()->getMessageType();
        $this->assertEquals(MessageType::NORMAL, $retriedType);
    }

    /**
     * The result must report the endpoint of the queue that actually succeeded,
     * which after a retry differs from the first candidate.
     */
    public function testResultReportsSuccessfulQueueEndpoints()
    {
        $handler = $this->buildHandler(1);
        $message = $this->buildMessage();
        $candidates = [
            $this->buildQueue('broker-0', 'host-0', 8081),
            $this->buildQueue('broker-1', 'host-1', 8081),
            $this->buildQueue('broker-2', 'host-2', 8081),
        ];

        $request = $handler->wrapTransactionMessageRequest([$message], $candidates[0]);
        $result = $handler->sendMessageWithRetry($request, $message, $candidates, 3, true);

        // Attempt 2 rotates to candidates[IntMath::mod(2, 3)] = candidates[2]
        $this->assertArrayHasKey('endpoints', $result);
        $this->assertNotNull($result['endpoints']);
        $this->assertEquals('host-2', $result['endpoints']->getAddresses()[0]->getHost());
    }

    /**
     * On first-attempt success the reported endpoint is the first candidate's.
     */
    public function testResultReportsFirstQueueEndpointsWithoutRetry()
    {
        $handler = $this->buildHandler(0);
        $message = $this->buildMessage();
        $candidates = [
            $this->buildQueue('broker-0', 'host-0', 8081),
            $this->buildQueue('broker-1', 'host-1', 8081),
        ];

        $request = $handler->wrapSendMessageRequest([$message], $candidates[0]);
        $result = $handler->sendMessageWithRetry($request, $message, $candidates, 3);

        $this->assertCount(1, $this->capturedRequests);
        $this->assertEquals('host-0', $result['endpoints']->getAddresses()[0]->getHost());
    }
}
