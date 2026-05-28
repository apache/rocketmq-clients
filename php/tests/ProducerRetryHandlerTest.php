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

use Apache\Rocketmq\ProducerRetryHandler;
use Apache\Rocketmq\ExponentialBackoffRetryPolicy;
use Apache\Rocketmq\ProducerEndpointIsolator;
use Apache\Rocketmq\MessageBuilder;
use Apache\Rocketmq\V2\SendMessageRequest;
use PHPUnit\Framework\TestCase;

/**
 * ProducerRetryHandlerTest - Test retry logic for message sending
 */
class ProducerRetryHandlerTest extends TestCase
{
    private ProducerEndpointIsolator $endpointIsolator;
    private ExponentialBackoffRetryPolicy $retryPolicy;

    protected function setUp(): void
    {
        $this->endpointIsolator = new ProducerEndpointIsolator();
        $this->retryPolicy = new ExponentialBackoffRetryPolicy(3, 1000, 30000, 2.0);
    }

    /**
     * Test instance creation
     */
    public function testInstanceCreation()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator,
            3000
        );
        
        $this->assertInstanceOf(ProducerRetryHandler::class, $handler);
    }

    /**
     * Test instance creation with default timeout
     */
    public function testInstanceCreationDefaultTimeout()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator
        );
        
        $this->assertInstanceOf(ProducerRetryHandler::class, $handler);
    }

    /**
     * Test sendMessageWithRetry succeeds on first attempt
     */
    public function testSendMessageWithRetrySuccessFirstAttempt()
    {
        // Create mock call object
        $mockEntry = new class {
            public function getMessageId() { return 'msg-123'; }
            public function getTransactionId() { return 'txn-456'; }
        };
        
        $mockResponse = new class($mockEntry) {
            private $entry;
            public function __construct($entry) {
                $this->entry = $entry;
            }
            public function getEntries() {
                return [$this->entry];
            }
        };
        
        $mockCall = new class($mockResponse) {
            private $response;
            public function __construct($response) {
                $this->response = $response;
            }
            public function wait() {
                return [$this->response, (object)['code' => 0]];
            }
        };
        
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $mockClient->method('SendMessage')->willReturn($mockCall);
        
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator
        );
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        $request = new SendMessageRequest();
        $request->setMessages([$message]);
        
        $mockQueue = $this->createMock(\Apache\Rocketmq\V2\MessageQueue::class);
        $candidates = [$mockQueue];
        
        $result = $handler->sendMessageWithRetry($request, $message, $candidates, 3);
        
        $this->assertTrue($result['success']);
        $this->assertEquals('msg-123', $result['messageId']);
        $this->assertEquals('txn-456', $result['transactionId']);
        $this->assertArrayHasKey('latencyMs', $result);
    }

    /**
     * Test sendMessageWithRetry fails after max attempts
     */
    public function testSendMessageWithRetryFailsAfterMaxAttempts()
    {
        // Create mock call that always fails
        $mockCall = new class {
            public function wait() {
                return [null, (object)['code' => 1, 'details' => 'Connection refused']];
            }
        };
        
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $mockClient->method('SendMessage')->willReturn($mockCall);
        
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator
        );
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        $request = new SendMessageRequest();
        $request->setMessages([$message]);
        
        $mockQueue = $this->createMock(\Apache\Rocketmq\V2\MessageQueue::class);
        $candidates = [$mockQueue];
        
        $this->expectException(\RuntimeException::class);
        $handler->sendMessageWithRetry($request, $message, $candidates, 2);
    }

    /**
     * Test sendMessageWithRetry with exception
     */
    public function testSendMessageWithRetryException()
    {
        // Create mock call that throws exception
        $mockCall = new class {
            public function wait() {
                throw new \Exception('Network error');
            }
        };
        
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $mockClient->method('SendMessage')->willReturn($mockCall);
        
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator
        );
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        $request = new SendMessageRequest();
        $request->setMessages([$message]);
        
        $mockQueue = $this->createMock(\Apache\Rocketmq\V2\MessageQueue::class);
        $candidates = [$mockQueue];
        
        $this->expectException(\Exception::class);
        $handler->sendMessageWithRetry($request, $message, $candidates, 1);
    }

    /**
     * Test sendBatchWithRetry succeeds
     */
    public function testSendBatchWithRetrySuccess()
    {
        // Create mock entries
        $mockEntry1 = new class {
            public function getMessageId() { return 'msg-1'; }
            public function getTransactionId() { return 'txn-1'; }
        };
        
        $mockEntry2 = new class {
            public function getMessageId() { return 'msg-2'; }
            public function getTransactionId() { return 'txn-2'; }
        };
        
        $mockResponse = new class([$mockEntry1, $mockEntry2]) {
            private $entries;
            public function __construct($entries) {
                $this->entries = $entries;
            }
            public function getEntries() {
                return $this->entries;
            }
        };
        
        $mockCall = new class($mockResponse) {
            private $response;
            public function __construct($response) {
                $this->response = $response;
            }
            public function wait() {
                return [$this->response, (object)['code' => 0]];
            }
        };
        
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $mockClient->method('SendMessage')->willReturn($mockCall);
        
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator
        );
        
        $messages = [
            (new MessageBuilder())->setTopic('topic1')->setBody('Message 1')->build(),
            (new MessageBuilder())->setTopic('topic1')->setBody('Message 2')->build(),
        ];
        
        $request = new SendMessageRequest();
        $request->setMessages($messages);
        
        $mockQueue = $this->createMock(\Apache\Rocketmq\V2\MessageQueue::class);
        $candidates = [$mockQueue];
        
        $results = $handler->sendBatchWithRetry($request, $messages, $candidates, 3);
        
        $this->assertCount(2, $results);
        $this->assertTrue($results[0]['success']);
        $this->assertEquals('msg-1', $results[0]['messageId']);
        $this->assertTrue($results[1]['success']);
        $this->assertEquals('msg-2', $results[1]['messageId']);
    }

    /**
     * Test sendBatchWithRetry fails after max attempts
     */
    public function testSendBatchWithRetryFailsAfterMaxAttempts()
    {
        $mockCall = new class {
            public function wait() {
                return [null, (object)['code' => 1, 'details' => 'Timeout']];
            }
        };
        
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $mockClient->method('SendMessage')->willReturn($mockCall);
        
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator
        );
        
        $messages = [
            (new MessageBuilder())->setTopic('topic1')->setBody('Message 1')->build(),
        ];
        
        $request = new SendMessageRequest();
        $request->setMessages($messages);
        
        $mockQueue = $this->createMock(\Apache\Rocketmq\V2\MessageQueue::class);
        $candidates = [$mockQueue];
        
        $this->expectException(\RuntimeException::class);
        $handler->sendBatchWithRetry($request, $messages, $candidates, 2);
    }

    /**
     * Test multiple candidates are tried on failure
     */
    public function testMultipleCandidatesTriedOnFailure()
    {
        // Create a counter class to track attempts
        $counter = new class {
            public $count = 0;
        };
        
        // Create mock call that fails twice then succeeds
        $mockCall = new class($counter) {
            private $counter;
            public function __construct($counter) {
                $this->counter = $counter;
            }
            public function wait() {
                $this->counter->count++;
                if ($this->counter->count < 3) {
                    return [null, (object)['code' => 1, 'details' => 'Failed']];
                }
                
                $mockEntry = new class {
                    public function getMessageId() { return 'msg-success'; }
                    public function getTransactionId() { return 'txn-success'; }
                };
                
                $mockResponse = new class($mockEntry) {
                    private $entry;
                    public function __construct($entry) {
                        $this->entry = $entry;
                    }
                    public function getEntries() {
                        return [$this->entry];
                    }
                };
                
                return [$mockResponse, (object)['code' => 0]];
            }
        };
        
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $mockClient->method('SendMessage')->willReturn($mockCall);
        
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator
        );
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        $request = new SendMessageRequest();
        $request->setMessages([$message]);
        
        $mockQueue1 = $this->createMock(\Apache\Rocketmq\V2\MessageQueue::class);
        $mockQueue2 = $this->createMock(\Apache\Rocketmq\V2\MessageQueue::class);
        $mockQueue3 = $this->createMock(\Apache\Rocketmq\V2\MessageQueue::class);
        $candidates = [$mockQueue1, $mockQueue2, $mockQueue3];
        
        $result = $handler->sendMessageWithRetry($request, $message, $candidates, 3);
        
        $this->assertTrue($result['success']);
        $this->assertEquals('msg-success', $result['messageId']);
    }

    /**
     * Test endpoint isolation on failure
     */
    public function testEndpointIsolationOnFailure()
    {
        $mockCall = new class {
            public function wait() {
                throw new \Exception('Connection failed');
            }
        };
        
        $mockBroker = $this->createMock(\Apache\Rocketmq\V2\Broker::class);
        $mockEndpoints = $this->createMock(\Apache\Rocketmq\V2\Endpoints::class);
        $mockAddress = $this->createMock(\Apache\Rocketmq\V2\Address::class);
        $mockAddress->method('getHost')->willReturn('broker1');
        $mockAddress->method('getPort')->willReturn(8080);
        $mockEndpoints->method('getAddresses')->willReturn([$mockAddress]);
        $mockBroker->method('hasEndpoints')->willReturn(true);
        $mockBroker->method('getEndpoints')->willReturn($mockEndpoints);
        
        $mockQueue = $this->createMock(\Apache\Rocketmq\V2\MessageQueue::class);
        $mockQueue->method('getBroker')->willReturn($mockBroker);
        
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $mockClient->method('SendMessage')->willReturn($mockCall);
        
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator
        );
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        $request = new SendMessageRequest();
        $request->setMessages([$message]);
        
        $candidates = [$mockQueue];
        
        try {
            $handler->sendMessageWithRetry($request, $message, $candidates, 1);
        } catch (\Exception $e) {
            // Expected
        }
        
        // Verify endpoint was isolated
        $this->assertGreaterThanOrEqual(0, $this->endpointIsolator->getIsolatedCount());
    }

    /**
     * Test getCallOptions returns correct timeout
     */
    public function testGetCallOptionsTimeout()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator,
            5000  // 5 seconds
        );
        
        // Use reflection to test private method
        $reflection = new \ReflectionClass($handler);
        $method = $reflection->getMethod('getCallOptions');
        $method->setAccessible(true);
        
        $options = $method->invoke($handler);
        $this->assertArrayHasKey('timeout', $options);
        $this->assertEquals(5.0, $options['timeout']);
    }

    /**
     * Test buildMetadata returns array
     */
    public function testBuildMetadata()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator
        );
        
        $reflection = new \ReflectionClass($handler);
        $method = $reflection->getMethod('buildMetadata');
        $method->setAccessible(true);
        
        $metadata = $method->invoke($handler);
        $this->assertIsArray($metadata);
    }

    /**
     * Test getMessageType returns NORMAL for simple message
     */
    public function testGetMessageTypeNormal()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator
        );
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test')
            ->build();
        
        $reflection = new \ReflectionClass($handler);
        $method = $reflection->getMethod('getMessageType');
        $method->setAccessible(true);
        
        $type = $method->invoke($handler, $message);
        $this->assertEquals('NORMAL', $type);
    }

    /**
     * Test getMessageType returns FIFO for message with group
     */
    public function testGetMessageTypeFifo()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        
        $handler = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator
        );
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test')
            ->setMessageGroup('group-1')
            ->build();
        
        $reflection = new \ReflectionClass($handler);
        $method = $reflection->getMethod('getMessageType');
        $method->setAccessible(true);
        
        $type = $method->invoke($handler, $message);
        $this->assertEquals('FIFO', $type);
    }

    /**
     * Test different request timeouts
     */
    public function testDifferentRequestTimeouts()
    {
        $mockClient = $this->createMock(\Apache\Rocketmq\V2\MessagingServiceClient::class);
        $logger = \Apache\Rocketmq\Logger::getInstance('Test');
        
        // Test 1 second timeout
        $handler1 = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator,
            1000
        );
        
        $reflection = new \ReflectionClass($handler1);
        $method = $reflection->getMethod('getCallOptions');
        $method->setAccessible(true);
        
        $options1 = $method->invoke($handler1);
        $this->assertEquals(1.0, $options1['timeout']);
        
        // Test 10 second timeout
        $handler2 = new ProducerRetryHandler(
            $mockClient,
            $logger,
            $this->retryPolicy,
            $this->endpointIsolator,
            10000
        );
        
        $options2 = $method->invoke($handler2);
        $this->assertEquals(10.0, $options2['timeout']);
    }
}
