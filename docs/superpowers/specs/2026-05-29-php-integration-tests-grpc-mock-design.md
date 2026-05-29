# PHP Integration Tests + gRPC Mock Framework Design

## Overview

Add integration tests covering all PHP client functionality, using PHPUnit mock objects injected via `RpcClientManager` to simulate gRPC server behavior. Only one production file (`RpcClientManager`) requires modification.

## Motivation

Current tests validate isolated state/validation logic only. No tests verify the full flow: request construction, gRPC call sequence, response handling, retry logic, load balancing, or inter-component coordination. Adding a gRPC mock layer enables end-to-end testing of business logic without a live RocketMQ broker.

## Design

### RpcClientManager: Mock Registry

`RpcClientManager` is the single factory for `MessagingServiceClient` — all 5 consuming classes create clients through it. Add a lightweight mock registry so tests can pre-register mock clients for specific endpoints:

```php
// New fields
private array $mocks = [];

// New methods
public function registerMock(string $endpoints, MessagingServiceClient $mock): void;
public function clearMocks(): void;
```

`getClient()` checks `$this->mocks[$key]` first. If a mock is registered for the given endpoints+options key, it's returned instead of creating a real client. Production code paths are unchanged — `getClient()` still creates real clients when no mock is registered.

**Key design constraint**: classes using `RpcClientManager` (SimpleConsumer, Producer, PushConsumer, ConsumeService, ProcessQueue) do NOT require constructor signature changes. Mock injection is transparent.

### GrpcMockHelper: Test Utility

`tests/helpers/GrpcMockHelper.php` — encapsulates PHPUnit mock setup for gRPC call patterns:

- **Unary calls** (`QueryRoute`, `Heartbeat`, `SendMessage`, `AckMessage`, etc.): `_simpleRequest()` returns an object whose `->wait()` returns `[$response, $status]`.
- **Server-stream calls** (`ReceiveMessage`): `_serverStreamRequest()` returns an object whose `->responses()` yields `$response` objects.
- **Bidi-stream calls** (`TelemetryCommand`): `_bidiStreamRequest()` returns a stream object with `->write()` and `->read()`.

Helper methods:
- `mockUnaryCall($client, $methodName, $response, $statusCode)` — configures a unary RPC to return the given response and status
- `mockServerStreamCall($client, $methodName, $response)` — configures a server-stream RPC to yield the given response
- `createMockClient()` — creates a `MessagingServiceClient` PHPUnit mock with all methods stubbed

### IntegrationTestCase: Base Class

`tests/helpers/IntegrationTestCase.php` — extends `PHPUnit\Framework\TestCase`:

- `setUp()`: calls `RpcClientManager::reset()`, `RpcClientManager::clearMocks()`, `TelemetrySession::resetAll()`, `Logger::close()`
- `tearDown()`: same cleanup
- `createAndRegisterMock(string $endpoints)`: creates a `MessagingServiceClient` mock and registers it

### Test Structure

```
tests/
├── helpers/
│   ├── GrpcMockHelper.php
│   └── IntegrationTestCase.php
├── integration/
│   ├── SimpleConsumerIntegrationTest.php
│   ├── ProducerIntegrationTest.php
│   ├── PushConsumerIntegrationTest.php
│   ├── LitePushConsumerIntegrationTest.php
│   ├── ConsumeServiceIntegrationTest.php
│   ├── ProcessQueueIntegrationTest.php
│   ├── TransactionIntegrationTest.php
│   ├── TelemetrySessionIntegrationTest.php
│   ├── HeartbeatIntegrationTest.php
│   ├── LoadBalancerIntegrationTest.php
│   └── RetryErrorHandlingIntegrationTest.php
└── ... (existing unit tests unchanged)
```

### Test Coverage Matrix

| Component | Scenarios |
|-----------|-----------|
| **SimpleConsumer** | receive with messages / empty / error; ack success / retry / permanent failure; changeInvisibleDuration; start with telemetry setup; shutdown with NotifyClientTermination; heartbeat cycle |
| **Producer** | send success / retry / isolation; sendAsync; transaction (half-send + commit/rollback); batch send; RecallMessage; interceptor hooks |
| **PushConsumer** | QueryAssignment poll → ProcessQueue dispatch → ConsumeService callback; scan loop |
| **LitePushConsumer** | Lite subscription sync; SyncLiteSubscription RPC; lite topic routing |
| **TelemetrySession** | Settings sync; TelemetryCommand write; server command dispatch (RecoverOrphanedTransaction, VerifyMessage, PrintThreadStackTrace) |
| **RpcClientManager** | mock registration / retrieval / clearing; connection counting with mocks |
| **Heartbeat** | periodic heartbeat via onHeartbeatTick; SIGALRM-triggered heartbeat; concurrency guard |
| **LoadBalancer** | SubscriptionLoadBalancer round-robin; PublishingLoadBalancer with isolation; route cache |
| **Retry/Error** | ExponentialBackoffRetryPolicy; CustomizedBackoffRetryPolicy; DEADLINE_EXCEEDED handling; gRPC status error propagation |

### phpunit.xml Update

Add a dedicated test suite for integration tests while keeping the existing suite:

```xml
<testsuite name="RocketMQ PHP Integration Tests">
    <directory suffix="Test.php">./tests/integration</directory>
</testsuite>
```

## Non-Goals

- No changes to gRPC generated code (`grpc/` directory)
- No new Composer dependencies
- No modification to existing unit tests
- No constructor signature changes on consumer/producer classes
- No changes to `TelemetrySession` (already accepts `object $client` as parameter)

## Risks

- **PHPUnit mock of `BaseStub`**: `MessagingServiceClient` extends `BaseStub`, which has complex internal state. Mocking methods that return `UnaryCall`/`ServerStreamingCall` objects requires understanding gRPC PHP internals. Mitigated by encapsulating this in `GrpcMockHelper`.
- **Singletons**: `RpcClientManager` and `TelemetrySession` are singletons — tests must reset state between cases. `IntegrationTestCase::setUp()` handles this.
