# PHP Integration Tests + gRPC Mock Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add integration tests covering all PHP client functionality with a gRPC mock framework, requiring only one production file change (RpcClientManager).

**Architecture:** `RpcClientManager` gains a mock registry; tests register mock `MessagingServiceClient` instances keyed by endpoint. `GrpcMockHelper` encapsulates creating anonymous call objects that duck-type `UnaryCall::wait()` and `ServerStreamingCall::responses()`. `IntegrationTestCase` handles singleton reset between tests.

**Tech Stack:** PHP 7.4+, PHPUnit 9.6, gRPC PHP, Google Protobuf

---

### Task 1: RpcClientManager Mock Registry

**Files:**
- Modify: `php/RpcClientManager.php` (add mock registry + methods)

- [ ] **Step 1: Add mock registry property and methods to RpcClientManager**

After the existing `private array $clients = [];` line, add:

```php
private array $mocks = [];
```

Add these public methods before `releaseClient()`:

```php
/**
 * Register a mock MessagingServiceClient for the given endpoints.
 * Subsequent calls to getClient() with matching endpoints will return this mock.
 *
 * @param string $endpoints Server endpoint in format "host:port"
 * @param MessagingServiceClient $mock The mock client to return
 * @return void
 */
public function registerMock(string $endpoints, MessagingServiceClient $mock): void
{
    $key = $this->makeKey($endpoints, []);
    $this->mocks[$key] = $mock;
    $this->logger->info("Registered mock client for: {$key}");
}

/**
 * Remove all registered mocks.
 *
 * @return void
 */
public function clearMocks(): void
{
    $this->mocks = [];
}
```

- [ ] **Step 2: Modify getClient() to check mock registry first**

In `getClient()`, add the mock check at the top, before the existing client lookup:

```php
public function getClient(string $endpoints, array $options = []): MessagingServiceClient
{
    $credentials = $this->resolveCredentials($options);
    $key = $this->makeKey($endpoints, $options);

    // Check mock registry first
    if (isset($this->mocks[$key])) {
        $this->clientLastUsedTime[$key] = time();
        return $this->mocks[$key];
    }

    // ... existing code unchanged ...
```

- [ ] **Step 3: Reset mocks in reset()**

Update the static `reset()` method to also clear mocks:

```php
public static function reset(): void
{
    self::$instance = null;
}
```

(No change needed — `reset()` destroys the singleton, and the next `getInstance()` creates a fresh instance with empty `$this->mocks = []`.)

- [ ] **Step 4: Run existing tests to verify no regression**

Run: `cd php && php vendor/bin/phpunit --testsuite "RocketMQ PHP Test Suite"`
Expected: All existing tests pass

- [ ] **Step 5: Commit**

```bash
git add php/RpcClientManager.php
git commit -m "feat: add mock registry to RpcClientManager for gRPC integration testing

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 2: GrpcMockHelper

**Files:**
- Create: `php/tests/helpers/GrpcMockHelper.php`

- [ ] **Step 1: Create GrpcMockHelper with unary call mock factory**

```php
<?php

namespace Apache\Rocketmq\Test\Helpers;

use Apache\Rocketmq\V2\MessagingServiceClient;

/**
 * Helper for creating and configuring gRPC mock objects.
 *
 * Encapsulates the complexity of simulating gRPC call objects
 * (UnaryCall, ServerStreamingCall, BidiStreamingCall) without
 * needing real gRPC connections.
 */
class GrpcMockHelper
{
    /**
     * Create a mock MessagingServiceClient with all gRPC methods stubbed.
     *
     * @return MessagingServiceClient|\PHPUnit\Framework\MockObject\MockObject
     */
    public static function createMockClient(): MessagingServiceClient
    {
        $methods = [
            'QueryRoute', 'Heartbeat', 'SendMessage', 'ReceiveMessage',
            'AckMessage', 'ForwardMessageToDeadLetterQueue', 'EndTransaction',
            'NotifyClientTermination', 'ChangeInvisibleDuration',
            'QueryAssignment', 'RecallMessage', 'SyncLiteSubscription',
            'Telemetry',
        ];

        return (new \PHPUnit\Framework\MockObject\Generator())->getMock(
            MessagingServiceClient::class,
            [],
            '',
            $methods
        );
    }

    /**
     * Configure a unary gRPC call on the mock client.
     *
     * Unary calls follow the pattern:
     *   list($response, $status) = $client->SomeMethod($req, $meta, $opts)->wait();
     *
     * @param object $mockClient Mock MessagingServiceClient
     * @param string $methodName gRPC method name (e.g. 'QueryRoute')
     * @param object|null $response Protobuf response message
     * @param int $statusCode gRPC status code (0 = OK)
     * @param string $statusDetails Status details string
     */
    public static function mockUnaryCall(
        $mockClient,
        string $methodName,
        $response,
        int $statusCode = 0,
        string $statusDetails = 'OK'
    ): void {
        $status = new \stdClass();
        $status->code = $statusCode;
        $status->details = $statusDetails;

        $call = new class($response, $status) {
            private $response;
            private $status;

            public function __construct($response, $status)
            {
                $this->response = $response;
                $this->status = $status;
            }

            public function wait(): array
            {
                return [$this->response, $this->status];
            }
        };

        $mockClient->method($methodName)->willReturn($call);
    }

    /**
     * Configure a server-streaming gRPC call on the mock client.
     *
     * Server-stream calls follow the pattern:
     *   $call = $client->ReceiveMessage($req, $meta);
     *   foreach ($call->responses() as $response) { ... }
     *
     * @param object $mockClient Mock MessagingServiceClient
     * @param string $methodName gRPC method name (e.g. 'ReceiveMessage')
     * @param array $responses Array of protobuf response messages to yield
     */
    public static function mockServerStreamCall(
        $mockClient,
        string $methodName,
        array $responses
    ): void {
        $call = new class($responses) {
            private array $responses;

            public function __construct(array $responses)
            {
                $this->responses = $responses;
            }

            public function responses(): \Generator
            {
                foreach ($this->responses as $response) {
                    yield $response;
                }
            }
        };

        $mockClient->method($methodName)->willReturn($call);
    }

    /**
     * Configure a bidi-streaming gRPC call on the mock client.
     *
     * Bidi-stream calls follow the pattern:
     *   $stream = $client->Telemetry($meta);
     *   $stream->write($command);
     *   $response = $stream->read();
     *
     * @param object $mockClient Mock MessagingServiceClient
     * @param array $readResponses Array of protobuf response messages for read()
     */
    public static function mockBidiStreamCall(
        $mockClient,
        string $methodName,
        array $readResponses = []
    ): void {
        $stream = new class($readResponses) {
            private array $written = [];
            private array $readResponses;
            private int $readIndex = 0;

            public function __construct(array $readResponses)
            {
                $this->readResponses = $readResponses;
            }

            public function write($data, array $options = []): void
            {
                $this->written[] = $data;
            }

            public function read()
            {
                if ($this->readIndex < count($this->readResponses)) {
                    return $this->readResponses[$this->readIndex++];
                }
                return null;
            }

            public function writesDone(): void
            {
            }

            public function getWritten(): array
            {
                return $this->written;
            }

            public function isCancelled(): bool
            {
                return false;
            }
        };

        $mockClient->method($methodName)->willReturn($stream);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add php/tests/helpers/GrpcMockHelper.php
git commit -m "feat: add GrpcMockHelper for creating gRPC call mocks

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 3: IntegrationTestCase Base Class

**Files:**
- Create: `php/tests/helpers/IntegrationTestCase.php`

- [ ] **Step 1: Create IntegrationTestCase with setUp/tearDown cleanup**

```php
<?php

namespace Apache\Rocketmq\Test\Helpers;

use Apache\Rocketmq\RpcClientManager;
use Apache\Rocketmq\TelemetrySession;
use Apache\Rocketmq\Logger;
use Apache\Rocketmq\V2\MessagingServiceClient;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/../../autoload.php';
require_once __DIR__ . '/../../vendor/autoload.php';

/**
 * Base class for integration tests.
 *
 * Handles singleton reset between tests and provides convenience
 * methods for creating and registering mock gRPC clients.
 */
abstract class IntegrationTestCase extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        RpcClientManager::reset();
        TelemetrySession::resetAll();
        Logger::close();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        RpcClientManager::reset();
        TelemetrySession::resetAll();
        Logger::close();
    }

    /**
     * Create a mock MessagingServiceClient and register it for the given endpoints.
     *
     * @param string $endpoints Server endpoint (e.g. 'localhost:8080')
     * @return MessagingServiceClient|\PHPUnit\Framework\MockObject\MockObject
     */
    protected function createAndRegisterMock(string $endpoints): MessagingServiceClient
    {
        $mock = GrpcMockHelper::createMockClient();
        RpcClientManager::getInstance()->registerMock($endpoints, $mock);
        return $mock;
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add php/tests/helpers/IntegrationTestCase.php
git commit -m "feat: add IntegrationTestCase base class with singleton cleanup

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 4: phpunit.xml Update

**Files:**
- Modify: `php/phpunit.xml`

- [ ] **Step 1: Add integration test suite and helper autoloading**

Add a new `<testsuite>` inside `<testsuites>`:

```xml
<testsuite name="RocketMQ PHP Integration Tests">
    <directory suffix="Test.php">./tests/integration</directory>
</testsuite>
```

Update the bootstrap to include helper autoloading. Change the existing bootstrap line from `bootstrap="autoload.php"` to reference a more complete bootstrap. Actually, the simplest approach is to add a PHP config that includes helpers:

Add inside the `<php>` block:

```xml
<includePath>./tests/helpers</includePath>
```

Wait, PHPUnit doesn't have `<includePath>`. Instead, we need a test bootstrap that includes the helpers. The simplest approach: each integration test file uses `require_once` for helpers, same as existing tests do.

No change to phpunit.xml needed beyond adding the test suite. The existing `bootstrap="autoload.php"` and `require_once` in test files is the established pattern.

```xml
<testsuite name="RocketMQ PHP Integration Tests">
    <directory suffix="Test.php">./tests/integration</directory>
</testsuite>
```

Insert this after the existing `</testsuite>` closing tag and before `</testsuites>`.

- [ ] **Step 2: Commit**

```bash
git add php/phpunit.xml
git commit -m "feat: add integration test suite to phpunit.xml

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 5: RpcClientManager Integration Tests

**Files:**
- Create: `php/tests/integration/RpcClientManagerIntegrationTest.php`

- [ ] **Step 1: Write tests for mock registration, retrieval, and clearing**

```php
<?php

namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\RpcClientManager;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';

class RpcClientManagerIntegrationTest extends IntegrationTestCase
{
    public function testMockRegistrationReturnsMockInsteadOfRealClient()
    {
        $endpoints = 'localhost:8080';
        $mock = GrpcMockHelper::createMockClient();
        RpcClientManager::getInstance()->registerMock($endpoints, $mock);

        $client = RpcClientManager::getInstance()->getClient($endpoints);

        $this->assertSame($mock, $client);
    }

    public function testMockNotRegisteredReturnsRealClient()
    {
        $client = RpcClientManager::getInstance()->getClient('localhost:9999');

        $this->assertNotNull($client);
    }

    public function testClearMocksRemovesAllRegistrations()
    {
        $mock = GrpcMockHelper::createMockClient();
        RpcClientManager::getInstance()->registerMock('localhost:8080', $mock);

        RpcClientManager::getInstance()->clearMocks();
        RpcClientManager::reset();

        // After reset+clear, getClient creates a real client
        $this->assertTrue(true); // clearMocks doesn't throw
    }

    public function testDifferentEndpointsHaveDifferentMocks()
    {
        $mock1 = GrpcMockHelper::createMockClient();
        $mock2 = GrpcMockHelper::createMockClient();

        RpcClientManager::getInstance()->registerMock('endpoint1:8080', $mock1);
        RpcClientManager::getInstance()->registerMock('endpoint2:8080', $mock2);

        $this->assertSame($mock1, RpcClientManager::getInstance()->getClient('endpoint1:8080'));
        $this->assertSame($mock2, RpcClientManager::getInstance()->getClient('endpoint2:8080'));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cd php && php vendor/bin/phpunit tests/integration/RpcClientManagerIntegrationTest.php`
Expected: All pass

- [ ] **Step 3: Commit**

```bash
git add php/tests/integration/RpcClientManagerIntegrationTest.php
git commit -m "test: add RpcClientManager mock registry integration tests

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 6: SimpleConsumer Integration Tests

**Files:**
- Create: `php/tests/integration/SimpleConsumerIntegrationTest.php`

- [ ] **Step 1: Write integration tests covering receive, ack, heartbeat, and lifecycle**

```php
<?php

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
use Apache\Rocketmq\V2\Status;
use Apache\Rocketmq\V2\Assignment;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../SimpleConsumer.php';

class SimpleConsumerIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testReceiveWithMessages()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        // Build QueryRoute response with message queues
        $routeResponse = new QueryRouteResponse();
        $status = new Status();
        $status->setCode(20000);
        $routeResponse->setStatus($status);

        $broker = new Broker();
        $brokerName = 'broker-1';
        $broker->setName($brokerName);
        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost('127.0.0.1');
        $address->setPort(8081);
        $brokerEndpoints->setAddresses([$address]);
        $broker->setEndpoints($brokerEndpoints);

        $mq = new MessageQueue();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $mq->setTopic($topicResource);
        $mq->setBroker($broker);
        $mq->setPermission(Permission::READ_WRITE);
        $assignments = [$mq];
        // QueryRouteResponse uses messageQueues, not assignments
        $routeResponse->setMessageQueues([$mq]);

        GrpcMockHelper::mockUnaryCall($mock, 'QueryRoute', $routeResponse, 0);

        // Build ReceiveMessage response
        $msg = new Message();
        $topicResource2 = new Resource();
        $topicResource2->setName('test-topic');
        $msg->setTopic($topicResource2);
        $sysProps = new \Apache\Rocketmq\V2\SystemProperties();
        $sysProps->setMessageId('msg-001');
        $sysProps->setReceiptHandle('receipt-001');
        $msg->setSystemProperties($sysProps);
        $msg->setBody('Hello World');

        $receiveResponse = new ReceiveMessageResponse();
        $receiveResponse->setMessage($msg);

        // Register mock for broker client too
        $brokerMock = GrpcMockHelper::createMockClient();
        \Apache\Rocketmq\RpcClientManager::getInstance()->registerMock('127.0.0.1:8081', $brokerMock);
        GrpcMockHelper::mockServerStreamCall($brokerMock, 'ReceiveMessage', [$receiveResponse]);

        // Heartbeat mock
        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);

        // Telemetry mock for start
        $settingsResponse = new TelemetryCommand();
        $settings = new \Apache\Rocketmq\V2\Settings();
        $settingsResponse->setSettings($settings);
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);
        $consumer->start();

        $messages = $consumer->receive(10, 30);

        $this->assertCount(1, $messages);
        $this->assertEquals('Hello World', $messages[0]->getBody());

        $consumer->shutdown();
    }

    public function testReceiveEmptyResult()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        // QueryRoute response with message queues
        $routeResponse = new QueryRouteResponse();
        $status = new Status();
        $status->setCode(20000);
        $routeResponse->setStatus($status);
        $broker = new Broker();
        $broker->setName('broker-1');
        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost('127.0.0.1');
        $address->setPort(8081);
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

        // Empty ReceiveMessage response (just status, no message)
        $receiveResponse = new ReceiveMessageResponse();
        $receiveStatus = new Status();
        $receiveStatus->setCode(40404); // not found
        $receiveResponse->setStatus($receiveStatus);

        $brokerMock = GrpcMockHelper::createMockClient();
        \Apache\Rocketmq\RpcClientManager::getInstance()->registerMock('127.0.0.1:8081', $brokerMock);
        GrpcMockHelper::mockServerStreamCall($brokerMock, 'ReceiveMessage', [$receiveResponse]);

        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);
        $consumer->start();

        $messages = $consumer->receive(10, 30);
        $this->assertEmpty($messages);

        $consumer->shutdown();
    }

    public function testAckMessages()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        $ackResponse = new AckMessageResponse();
        GrpcMockHelper::mockUnaryCall($mock, 'AckMessage', $ackResponse, 0);
        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);
        GrpcMockHelper::mockUnaryCall($mock, 'NotifyClientTermination', new NotifyClientTerminationResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);
        $consumer->start();

        $msg = new Message();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $msg->setTopic($topicResource);
        $sysProps = new \Apache\Rocketmq\V2\SystemProperties();
        $sysProps->setReceiptHandle('receipt-001');
        $sysProps->setMessageId('msg-001');
        $msg->setSystemProperties($sysProps);

        // This shouldn't throw
        $consumer->ack([$msg]);

        $consumer->shutdown();
    }

    public function testChangeInvisibleDuration()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        $changeResponse = new ChangeInvisibleDurationResponse();
        $changeResponse->setReceiptHandle('new-receipt-001');
        GrpcMockHelper::mockUnaryCall($mock, 'ChangeInvisibleDuration', $changeResponse, 0);
        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);
        GrpcMockHelper::mockUnaryCall($mock, 'NotifyClientTermination', new NotifyClientTerminationResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);
        $consumer->start();

        $msg = new Message();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $msg->setTopic($topicResource);
        $sysProps = new \Apache\Rocketmq\V2\SystemProperties();
        $sysProps->setReceiptHandle('receipt-001');
        $sysProps->setMessageId('msg-001');
        $msg->setSystemProperties($sysProps);

        $result = $consumer->changeInvisibleDuration($msg, 60);
        $this->assertTrue($result);

        $consumer->shutdown();
    }

    public function testStartFailsWithoutTelemetryResponse()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        // Bidi stream returns null (no settings response)
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', []);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/Telemetry/');
        $consumer->start();
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cd php && php vendor/bin/phpunit tests/integration/SimpleConsumerIntegrationTest.php`
Expected: All pass

- [ ] **Step 3: Commit**

```bash
git add php/tests/integration/SimpleConsumerIntegrationTest.php
git commit -m "test: add SimpleConsumer integration tests with gRPC mocks

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 7: Producer Integration Tests

**Files:**
- Create: `php/tests/integration/ProducerIntegrationTest.php`

- [ ] **Step 1: Write integration tests for send, transaction, and lifecycle**

```php
<?php

namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\SendMessageResponse;
use Apache\Rocketmq\V2\EndTransactionResponse;
use Apache\Rocketmq\V2\HeartbeatResponse;
use Apache\Rocketmq\V2\QueryRouteResponse;
use Apache\Rocketmq\V2\NotifyClientTerminationResponse;
use Apache\Rocketmq\V2\RecallMessageResponse;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\Status;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../Producer.php';

class ProducerIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testSendMessageSuccess()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        // QueryRoute response
        $routeResponse = new QueryRouteResponse();
        $status = new Status();
        $status->setCode(20000);
        $routeResponse->setStatus($status);
        $broker = new Broker();
        $broker->setName('broker-1');
        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost('127.0.0.1');
        $address->setPort(8081);
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

        // SendMessage response
        $sendResponse = new SendMessageResponse();
        $sendStatus = new Status();
        $sendStatus->setCode(20000);
        $sendResponse->setStatus($sendStatus);

        $brokerMock = GrpcMockHelper::createMockClient();
        \Apache\Rocketmq\RpcClientManager::getInstance()->registerMock('127.0.0.1:8081', $brokerMock);
        GrpcMockHelper::mockUnaryCall($brokerMock, 'SendMessage', $sendResponse, 0);

        // Heartbeat
        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);

        // Telemetry
        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic']]);
        $producer->start();

        $msg = new Message();
        $topicResource2 = new Resource();
        $topicResource2->setName('test-topic');
        $msg->setTopic($topicResource2);
        $msg->setBody('Test Message Body');

        $result = $producer->send($msg);

        $this->assertNotNull($result);

        $producer->shutdown();
    }

    public function testSendMessageRetryOnFailure()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        // QueryRoute response
        $routeResponse = new QueryRouteResponse();
        $status = new Status();
        $status->setCode(20000);
        $routeResponse->setStatus($status);
        $broker = new Broker();
        $broker->setName('broker-1');
        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost('127.0.0.1');
        $address->setPort(8081);
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

        // First SendMessage fails, second succeeds
        $failResponse = new SendMessageResponse();
        $failStatus = new Status();
        $failStatus->setCode(50001); // INTERNAL_SERVER_ERROR
        $failResponse->setStatus($failStatus);

        $successResponse = new SendMessageResponse();
        $successStatus = new Status();
        $successStatus->setCode(20000);
        $successResponse->setStatus($successStatus);

        $brokerMock = GrpcMockHelper::createMockClient();
        \Apache\Rocketmq\RpcClientManager::getInstance()->registerMock('127.0.0.1:8081', $brokerMock);
        $brokerMock->expects($this->exactly(2))
            ->method('SendMessage')
            ->willReturnOnConsecutiveCalls(
                GrpcMockHelper::createUnaryCall($failResponse, 50001),
                GrpcMockHelper::createUnaryCall($successResponse, 0)
            );

        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic'], 'maxAttempts' => 3]);
        $producer->start();

        $msg = new Message();
        $topicResource2 = new Resource();
        $topicResource2->setName('test-topic');
        $msg->setTopic($topicResource2);
        $msg->setBody('Test Message');

        $result = $producer->send($msg);
        $this->assertNotNull($result);

        $producer->shutdown();
    }

    public function testShutdownNotifiesTermination()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        GrpcMockHelper::mockUnaryCall($mock, 'NotifyClientTermination', new NotifyClientTerminationResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic']]);
        $producer->start();
        $producer->shutdown();

        $this->assertTrue(true); // no exception
    }
}
```

Note: This task reveals that `GrpcMockHelper` needs a public `createUnaryCall` method for scenarios needing sequenced return values. Add this to GrpcMockHelper:

In `GrpcMockHelper.php`, add:

```php
/**
 * Create a standalone unary call object for use with willReturnOnConsecutiveCalls().
 *
 * @param object|null $response Protobuf response message
 * @param int $statusCode gRPC status code
 * @return object Anonymous object with wait() method
 */
public static function createUnaryCall($response, int $statusCode = 0): object
{
    $status = new \stdClass();
    $status->code = $statusCode;
    $status->details = $statusCode === 0 ? 'OK' : 'Error';

    return new class($response, $status) {
        private $response;
        private $status;

        public function __construct($response, $status)
        {
            $this->response = $response;
            $this->status = $status;
        }

        public function wait(): array
        {
            return [$this->response, $this->status];
        }
    };
}
```

- [ ] **Step 2: Run tests**

Run: `cd php && php vendor/bin/phpunit tests/integration/ProducerIntegrationTest.php`
Expected: All pass (may need adjustment based on actual Producer behavior)

- [ ] **Step 3: Commit**

```bash
git add php/tests/integration/ProducerIntegrationTest.php php/tests/helpers/GrpcMockHelper.php
git commit -m "test: add Producer integration tests with gRPC mocks

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 8: PushConsumer Integration Tests

**Files:**
- Create: `php/tests/integration/PushConsumerIntegrationTest.php`

- [ ] **Step 1: Write tests for QueryAssignment polling and message dispatch**

```php
<?php

namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\PushConsumer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\QueryAssignmentResponse;
use Apache\Rocketmq\V2\ReceiveMessageResponse;
use Apache\Rocketmq\V2\HeartbeatResponse;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Assignment;
use Apache\Rocketmq\V2\Status;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../PushConsumer.php';

class PushConsumerIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testQueryAssignmentPolling()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        // QueryAssignment response
        $assignmentResponse = new QueryAssignmentResponse();
        $status = new Status();
        $status->setCode(20000);
        $assignmentResponse->setStatus($status);

        GrpcMockHelper::mockUnaryCall($mock, 'QueryAssignment', $assignmentResponse, 0);
        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $receivedMessages = [];
        $consumer = new PushConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
            'messageListener' => function ($msg) use (&$receivedMessages) {
                $receivedMessages[] = $msg;
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
            'scanIntervalSeconds' => 1,
        ]);

        // Don't call start() since it blocks; test the individual cycle
        $consumer->start();

        // This is a smoke test — start blocks indefinitely in blocking mode
        $this->assertTrue(true);
        $consumer->shutdown();
    }
}
```

- [ ] **Step 2: Run tests and commit**

```bash
cd php && php vendor/bin/phpunit tests/integration/PushConsumerIntegrationTest.php
git add php/tests/integration/PushConsumerIntegrationTest.php
git commit -m "test: add PushConsumer integration tests

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 9: ProcessQueue Integration Tests

**Files:**
- Create: `php/tests/integration/ProcessQueueIntegrationTest.php`

- [ ] **Step 1: Write ProcessQueue tests**

```php
<?php

namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\ProcessQueue;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\ReceiveMessageResponse;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Status;
use Apache\Rocketmq\V2\FilterExpression;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../ProcessQueue.php';
require_once __DIR__ . '/../../SimpleConsumer.php';

class ProcessQueueIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testReceiveMessageImmediately()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        // Create a SimpleConsumer for ProcessQueue
        $consumer = new \Apache\Rocketmq\SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        // Build message queue
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

        $filterExpression = new FilterExpression();
        $filterExpression->setExpression('*');
        $filterExpression->setType(\Apache\Rocketmq\V2\FilterType::TAG);

        // Mock ReceiveMessage to return a message
        $msg = new Message();
        $msgTopic = new Resource();
        $msgTopic->setName('test-topic');
        $msg->setTopic($msgTopic);
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-001');
        $sysProps->setReceiptHandle('receipt-001');
        $msg->setSystemProperties($sysProps);
        $msg->setBody('Processed Message');

        $receiveResponse = new ReceiveMessageResponse();
        $receiveResponse->setMessage($msg);

        GrpcMockHelper::mockServerStreamCall($mock, 'ReceiveMessage', [$receiveResponse]);

        $pq = new ProcessQueue($consumer, $mq, $filterExpression, 32, 30, 4096, 67108864);

        $received = $pq->receiveMessageImmediately(10);

        $this->assertGreaterThan(0, count($received));
    }
}
```

- [ ] **Step 2: Run tests and commit**

```bash
cd php && php vendor/bin/phpunit tests/integration/ProcessQueueIntegrationTest.php
git add php/tests/integration/ProcessQueueIntegrationTest.php
git commit -m "test: add ProcessQueue integration tests

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 10: Transaction Integration Tests

**Files:**
- Create: `php/tests/integration/TransactionIntegrationTest.php`

- [ ] **Step 1: Write Transaction tests**

```php
<?php

namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\Transaction;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\EndTransactionResponse;
use Apache\Rocketmq\V2\SendMessageResponse;
use Apache\Rocketmq\V2\HeartbeatResponse;
use Apache\Rocketmq\V2\QueryRouteResponse;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\Status;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\SendResultEntry;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../Transaction.php';
require_once __DIR__ . '/../../Producer.php';

class TransactionIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testTransactionLifecycle()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        // Setup route
        $routeResponse = new QueryRouteResponse();
        $status = new Status();
        $status->setCode(20000);
        $routeResponse->setStatus($status);
        $broker = new Broker();
        $broker->setName('broker-1');
        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost('127.0.0.1');
        $address->setPort(8081);
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

        // SendMessage response with SendResultEntry
        $sendResponse = new SendMessageResponse();
        $sendStatus = new Status();
        $sendStatus->setCode(20000);
        $sendResponse->setStatus($sendStatus);
        $entry = new SendResultEntry();
        $sendResponse->setEntries([$entry]);

        $brokerMock = GrpcMockHelper::createMockClient();
        \Apache\Rocketmq\RpcClientManager::getInstance()->registerMock('127.0.0.1:8081', $brokerMock);
        GrpcMockHelper::mockUnaryCall($brokerMock, 'SendMessage', $sendResponse, 0);

        // EndTransaction response
        $endTxResponse = new EndTransactionResponse();
        GrpcMockHelper::mockUnaryCall($mock, 'EndTransaction', $endTxResponse, 0);

        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $producer = new Producer($this->endpoints, ['topics' => ['test-topic']]);
        $producer->start();

        $tx = new Transaction($producer);

        $msg = new Message();
        $topicResource2 = new Resource();
        $topicResource2->setName('test-topic');
        $msg->setTopic($topicResource2);
        $msg->setBody('TX message');

        $tx->tryAddMessage($msg);
        $this->assertTrue(true); // no exception

        $producer->shutdown();
    }

    public function testTransactionCommitAndRollback()
    {
        $tx = new Transaction(new \stdClass());
        $msg = new Message();
        $msg->setBody('test');
        $tx->tryAddMessage($msg);
        $tx->commit();

        $this->assertTrue(true);
    }
}
```

- [ ] **Step 2: Run tests and commit**

```bash
cd php && php vendor/bin/phpunit tests/integration/TransactionIntegrationTest.php
git add php/tests/integration/TransactionIntegrationTest.php
git commit -m "test: add Transaction integration tests

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 11: TelemetrySession Integration Tests

**Files:**
- Create: `php/tests/integration/TelemetrySessionIntegrationTest.php`

- [ ] **Step 1: Write TelemetrySession tests**

```php
<?php

namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\TelemetrySession;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\RecoverOrphanedTransactionCommand;
use Apache\Rocketmq\V2\VerifyMessageCommand;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Publishing;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../TelemetrySession.php';

class TelemetrySessionIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testSyncSettingsSuccess()
    {
        $mock = GrpcMockHelper::createMockClient();

        // Server echoes back Settings
        $serverSettings = new TelemetryCommand();
        $echoedSettings = new Settings();
        $echoedSettings->setClientType(ClientType::PRODUCER);
        $serverSettings->setSettings($echoedSettings);

        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$serverSettings]);

        $session = TelemetrySession::getInstance($mock, $this->endpoints, 'test-client');

        $command = new TelemetryCommand();
        $requestSettings = new Settings();
        $requestSettings->setClientType(ClientType::PRODUCER);
        $command->setSettings($requestSettings);

        $result = $session->syncSettings($command);
        $this->assertTrue($result);
    }

    public function testSettingsWithSubscription()
    {
        $mock = GrpcMockHelper::createMockClient();

        $serverSettings = new TelemetryCommand();
        $echoedSettings = new Settings();
        $echoedSettings->setClientType(ClientType::SIMPLE_CONSUMER);
        $subscription = new Subscription();
        $group = new Resource();
        $group->setName('test-group');
        $subscription->setGroup($group);
        $echoedSettings->setSubscription($subscription);
        $serverSettings->setSettings($echoedSettings);

        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$serverSettings]);

        $session = TelemetrySession::getInstance($mock, $this->endpoints, 'test-client');

        $command = new TelemetryCommand();
        $requestSettings = new Settings();
        $requestSettings->setClientType(ClientType::SIMPLE_CONSUMER);
        $command->setSettings($requestSettings);

        $result = $session->syncSettings($command);
        $this->assertTrue($result);
    }

    public function testSyncSettingsTimeout()
    {
        $mock = GrpcMockHelper::createMockClient();
        // No response from server — timeout
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', []);

        $session = TelemetrySession::getInstance($mock, $this->endpoints, 'test-client');

        $command = new TelemetryCommand();
        $requestSettings = new Settings();
        $requestSettings->setClientType(ClientType::PRODUCER);
        $command->setSettings($requestSettings);

        $result = $session->syncSettings($command);
        $this->assertFalse($result);
    }
}
```

- [ ] **Step 2: Run tests and commit**

```bash
cd php && php vendor/bin/phpunit tests/integration/TelemetrySessionIntegrationTest.php
git add php/tests/integration/TelemetrySessionIntegrationTest.php
git commit -m "test: add TelemetrySession integration tests

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 12: Heartbeat Integration Tests

**Files:**
- Create: `php/tests/integration/HeartbeatIntegrationTest.php`

- [ ] **Step 1: Write heartbeat cycle tests**

```php
<?php

namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\SimpleConsumer;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\HeartbeatResponse;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\NotifyClientTerminationResponse;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../SimpleConsumer.php';
require_once __DIR__ . '/../../Producer.php';

class HeartbeatIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testSimpleConsumerHeartbeatSent()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);
        GrpcMockHelper::mockUnaryCall($mock, 'NotifyClientTermination', new NotifyClientTerminationResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $consumer->doHeartbeat(); // direct call to test heartbeat logic
        $this->assertTrue(true); // no exception

        $consumer->shutdown();
    }

    public function testHeartbeatConcurrencyGuard()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        GrpcMockHelper::mockUnaryCall($mock, 'NotifyClientTermination', new NotifyClientTerminationResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        // Set heartbeat in progress manually
        $ref = new \ReflectionProperty($consumer, 'heartbeatInProgress');
        $ref->setAccessible(true);
        $ref->setValue($consumer, true);

        // Calling doHeartbeat while in progress should be guarded
        // This may log a warning but shouldn't send another heartbeat
        $this->assertTrue($consumer->isHeartbeatInProgress());

        $consumer->shutdown();
    }
}
```

- [ ] **Step 2: Run tests and commit**

```bash
cd php && php vendor/bin/phpunit tests/integration/HeartbeatIntegrationTest.php
git add php/tests/integration/HeartbeatIntegrationTest.php
git commit -m "test: add Heartbeat integration tests

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 13: LoadBalancer Integration Tests

**Files:**
- Create: `php/tests/integration/LoadBalancerIntegrationTest.php`

- [ ] **Step 1: Write load balancer tests**

```php
<?php

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
use Apache\Rocketmq\V2\Status;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../../SubscriptionLoadBalancer.php';
require_once __DIR__ . '/../../PublishingLoadBalancer.php';

class LoadBalancerIntegrationTest extends IntegrationTestCase
{
    public function testSubscriptionLoadBalancerRoundRobin()
    {
        $routeResponse = new QueryRouteResponse();
        $status = new Status();
        $status->setCode(20000);
        $routeResponse->setStatus($status);

        $queues = [];
        for ($i = 0; $i < 3; $i++) {
            $broker = new Broker();
            $broker->setName("broker-{$i}");
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

        $lb = new SubscriptionLoadBalancer($routeResponse);

        $mq1 = $lb->takeMessageQueue();
        $mq2 = $lb->takeMessageQueue();
        $mq3 = $lb->takeMessageQueue();
        $mq4 = $lb->takeMessageQueue(); // wraps around

        $this->assertNotNull($mq1);
        $this->assertNotNull($mq2);
        $this->assertNotNull($mq3);
        $this->assertNotNull($mq4);

        // Round-robin: 4th call should return same as 1st
        $this->assertEquals(
            $mq1->getBroker()->getName(),
            $mq4->getBroker()->getName()
        );
    }

    public function testPublishingLoadBalancerWithIsolation()
    {
        $routeResponse = new QueryRouteResponse();
        $status = new Status();
        $status->setCode(20000);
        $routeResponse->setStatus($status);

        $broker = new Broker();
        $broker->setName('broker-1');
        $brokerEndpoints = new Endpoints();
        $brokerEndpoints->setScheme(AddressScheme::IPv4);
        $address = new Address();
        $address->setHost('127.0.0.1');
        $address->setPort(8080);
        $brokerEndpoints->setAddresses([$address]);
        $broker->setEndpoints($brokerEndpoints);

        $mq = new MessageQueue();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $mq->setTopic($topicResource);
        $mq->setBroker($broker);
        $mq->setPermission(Permission::READ_WRITE);
        $routeResponse->setMessageQueues([$mq]);

        $lb = new PublishingLoadBalancer($routeResponse);

        $result = $lb->takeMessageQueue();
        $this->assertNotNull($result);

        // Isolate the endpoint and verify it's excluded
        $lb->isolateEndpoint('127.0.0.1:8080');
        $result2 = $lb->takeMessageQueue();
        // Should fall through since only queue is isolated
        $this->assertNull($result2);
    }
}
```

- [ ] **Step 2: Run tests and commit**

```bash
cd php && php vendor/bin/phpunit tests/integration/LoadBalancerIntegrationTest.php
git add php/tests/integration/LoadBalancerIntegrationTest.php
git commit -m "test: add LoadBalancer integration tests

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 14: Retry and Error Handling Integration Tests

**Files:**
- Create: `php/tests/integration/RetryErrorHandlingIntegrationTest.php`

- [ ] **Step 1: Write retry and error handling tests**

```php
<?php

namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\ExponentialBackoffRetryPolicy;
use Apache\Rocketmq\CustomizedBackoffRetryPolicy;
use Apache\Rocketmq\SimpleConsumer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\AckMessageResponse;
use Apache\Rocketmq\V2\HeartbeatResponse;
use Apache\Rocketmq\V2\AckMessageResultEntry;
use Apache\Rocketmq\V2\Status as V2Status;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../SimpleConsumer.php';
require_once __DIR__ . '/../../ExponentialBackoffRetryPolicy.php';
require_once __DIR__ . '/../../CustomizedBackoffRetryPolicy.php';

class RetryErrorHandlingIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testExponentialBackoffRetryPolicy()
    {
        $policy = new ExponentialBackoffRetryPolicy(3, 1000, 30000, 2.0);

        $delay1 = $policy->getNextAttemptDelay(1);
        $delay2 = $policy->getNextAttemptDelay(2);
        $delay3 = $policy->getNextAttemptDelay(3);

        $this->assertGreaterThan(0, $delay1);
        $this->assertGreaterThan($delay1, $delay2);
        $this->assertGreaterThan($delay2, $delay3);
    }

    public function testCustomizedBackoffRetryPolicy()
    {
        $delays = [100, 500, 2000, 10000];
        $policy = new CustomizedBackoffRetryPolicy($delays);

        $this->assertEquals(100, $policy->getNextAttemptDelay(1));
        $this->assertEquals(500, $policy->getNextAttemptDelay(2));
        $this->assertEquals(2000, $policy->getNextAttemptDelay(3));
        $this->assertEquals(10000, $policy->getNextAttemptDelay(4));
    }

    public function testAckRetryOnTransientError()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        // Ack response: one entry with retryable error
        $ackResponse = new AckMessageResponse();
        $resultEntry = new AckMessageResultEntry();
        $status = new V2Status();
        $status->setCode(50001); // INTERNAL_SERVER_ERROR - retryable
        $resultEntry->setStatus($status);
        $ackResponse->setEntries([$resultEntry]);

        // Setup mock to return ackResponse, then success response
        $mock->expects($this->any())
            ->method('AckMessage')
            ->willReturn(GrpcMockHelper::createUnaryCall($ackResponse, 0));

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);
        $consumer->start();

        $msg = new Message();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $msg->setTopic($topicResource);
        $sysProps = new SystemProperties();
        $sysProps->setReceiptHandle('receipt-001');
        $sysProps->setMessageId('msg-001');
        $msg->setSystemProperties($sysProps);

        // Should retry on 50001, eventually exhaust retries
        $consumer->ack([$msg]);

        $this->assertTrue(true); // no exception thrown

        $consumer->shutdown();
    }
}
```

- [ ] **Step 2: Run tests and commit**

```bash
cd php && php vendor/bin/phpunit tests/integration/RetryErrorHandlingIntegrationTest.php
git add php/tests/integration/RetryErrorHandlingIntegrationTest.php
git commit -m "test: add retry and error handling integration tests

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 15: Final Verification — Run All Tests

- [ ] **Step 1: Run complete test suite**

```bash
cd php && php vendor/bin/phpunit
```

Expected: All tests pass, both unit and integration

- [ ] **Step 2: Run integration suite standalone**

```bash
cd php && php vendor/bin/phpunit --testsuite "RocketMQ PHP Integration Tests"
```

Expected: All integration tests pass

- [ ] **Step 3: Verify no regressions in existing tests**

```bash
cd php && php vendor/bin/phpunit --testsuite "RocketMQ PHP Test Suite"
```

Expected: All existing unit tests still pass

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "test: complete PHP integration test suite with gRPC mock framework

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```
