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

use Apache\Rocketmq\TransactionTrait;
use Apache\Rocketmq\TransactionCommitter;
use Apache\Rocketmq\TransactionChecker;
use Apache\Rocketmq\LocalTransactionExecuter;
use Apache\Rocketmq\Transaction;
use Apache\Rocketmq\MessageView;
use Apache\Rocketmq\MessageHookPoints;
use Apache\Rocketmq\Logger;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\TransactionResolution;
use Apache\Rocketmq\V2\TransactionSource;

/**
 * Fake ProducerSettings stub for TransactionTrait tests.
 */
class FakeProducerSettingsForTrait
{
    public function getMaxAttempts(): int { return 1; }
    public function getRequestTimeout(): int { return 3000; }
    public function getTopics(): array { return []; }
    public function getNamespace(): string { return ''; }
    public function getCredentials(): ?object { return null; }
    public function getTlsCredentials(): ?object { return null; }
    public function isSslEnabled(): bool { return true; }
    public function getEndpoints(): string { return 'fake-endpoint'; }
    public function getRetryPolicy(): object
    {
        return new class {
            public function getNextDelayWithJitterMs(int $attempt): int { return 0; }
        };
    }
    public function applyServerBackoffPolicy(object $settings, $logger): void {}
}

/**
 * Fake class using TransactionTrait for testing 2PC flow.
 * Provides minimal stubs for all dependencies the trait requires.
 */
class FakeProducerForTrait implements TransactionCommitter
{
    use TransactionTrait;

    public bool $isRunning = true;
    public int $maxAttempts = 1;
    public bool $validateMessageType = false;
    public ?object $routeManager = null;
    public ?object $client = null;
    public ?object $tlsCredentials = null;
    public bool $sslEnabled = true;
    public ?object $telemetrySession = null;
    public ?object $logger = null;
    public ?object $settings = null;
    public ?object $validator = null;

    public array $commitCalls = [];
    public array $rollbackCalls = [];
    public array $interceptorCalls = [];
    public array $endTransactionCalls = [];

    public function __construct()
    {
        $this->logger = Logger::getInstance('FakeProducerForTrait');
        $this->routeManager = new FakeRouteManagerForTrait();
        $this->client = new FakeGrpcClientForTrait($this);
        $this->telemetrySession = new FakeTelemetrySessionForTrait();
        $this->settings = new FakeProducerSettingsForTrait();
        $this->validator = new \Apache\Rocketmq\MessageValidator(4194304, false);
    }

    public function validateMessage(Message $message): void
    {
        // No-op for testing
    }

    public function detectMessageType(Message $message, bool $transactional = false): int
    {
        return 0; // NORMAL
    }

    public function wrapTransactionMessageRequest(array $messages, $messageQueue): object
    {
        return new \stdClass();
    }

    public function sendMessageWithRetry($request, Message $message, array $messageQueue, int $maxAttempts): array
    {
        return [
            'messageId' => 'fake-msg-id-' . uniqid(),
            'transactionId' => 'fake-tx-id-' . uniqid(),
            'recallHandle' => null,
        ];
    }

    public function getOperationTimeout(string $operation): int
    {
        return 10_000_000; // 10 seconds in microseconds
    }

    public function buildMetadata(int $timeoutMs): array
    {
        return ['timeout' => $timeoutMs];
    }

    public function executeInterceptors(string $hookPoint, array $args): void
    {
        $this->interceptorCalls[] = ['hookPoint' => $hookPoint, 'args' => $args];
    }

    // TransactionCommitter interface
    public function commitTransaction(string $messageId, string $transactionId, string $topic, ?Endpoints $endpoints = null): void
    {
        $this->commitCalls[] = [
            'messageId' => $messageId,
            'transactionId' => $transactionId,
            'topic' => $topic,
            'endpoints' => $endpoints,
        ];
    }

    public function rollbackTransaction(string $messageId, string $transactionId, string $topic, ?Endpoints $endpoints = null): void
    {
        $this->rollbackCalls[] = [
            'messageId' => $messageId,
            'transactionId' => $transactionId,
            'topic' => $topic,
            'endpoints' => $endpoints,
        ];
    }

    // Expose private methods via reflection for testing
    public function testEndTransaction(string $messageId, string $transactionId, string $topic, int $resolution, ?Endpoints $endpoints = null, int $source = TransactionSource::SOURCE_CLIENT): void
    {
        $this->endTransactionCalls[] = [
            'messageId' => $messageId,
            'transactionId' => $transactionId,
            'topic' => $topic,
            'resolution' => $resolution,
            'source' => $source,
        ];

        $method = new \ReflectionMethod(self::class, 'endTransaction');
        $method->setAccessible(true);
        $method->invoke($this, $messageId, $transactionId, $topic, $resolution, $endpoints, $source);
    }

    public function testHandleOrphanedTransaction(object $command): void
    {
        $method = new \ReflectionMethod(self::class, 'handleOrphanedTransaction');
        $method->setAccessible(true);
        $method->invoke($this, $command);
    }

    public function testRegisterTransactionCheckerCallback(): void
    {
        $method = new \ReflectionMethod(self::class, 'registerTransactionCheckerCallback');
        $method->setAccessible(true);
        $method->invoke($this);
    }

    private function buildMessage(string $topic, string $body): Message
    {
        $message = new Message();
        $message->setBody($body);
        $topicResource = new Resource();
        $topicResource->setName($topic);
        $message->setTopic($topicResource);
        return $message;
    }
}

/**
 * Fake RouteManager for trait testing.
 */
class FakeRouteManagerForTrait
{
    public function getPublishingLoadBalancer(string $topic): object
    {
        return new FakeLoadBalancerForTrait();
    }

    public function getIsolatedBrokerNames(): array
    {
        return [];
    }
}

/**
 * Fake PublishingLoadBalancer for trait testing.
 */
class FakeLoadBalancerForTrait
{
    public function takeMessageQueue(array $isolatedBrokerNames, int $maxAttempts): array
    {
        $queue = new \Apache\Rocketmq\V2\MessageQueue();
        $topic = new Resource();
        $topic->setName('test-topic');
        $queue->setTopic($topic);
        $queue->setId(0);

        $broker = new \Apache\Rocketmq\V2\Broker();
        $broker->setName('broker-0');
        $address = new Address();
        $address->setHost('localhost');
        $address->setPort(10911);
        $endpoints = new Endpoints();
        $endpoints->setAddresses([$address]);
        $broker->setEndpoints($endpoints);
        $queue->setBroker($broker);

        return [$queue];
    }

    public function validateMessageTypeAgainstQueue($queue, int $msgType, string $topic): void
    {
        // No-op
    }
}

/**
 * Fake gRPC client for trait testing.
 */
class FakeGrpcClientForTrait
{
    private $producer;

    public function __construct($producer)
    {
        $this->producer = $producer;
    }

    public function EndTransaction($request, $metadata, $callOptions)
    {
        return new FakeUnaryCallForTrait();
    }
}

/**
 * Fake gRPC unary call.
 */
class FakeUnaryCallForTrait
{
    public function wait(): array
    {
        $status = new \stdClass();
        $status->code = 0;
        $status->details = '';

        $response = new \Apache\Rocketmq\V2\EndTransactionResponse();
        $respStatus = new \Apache\Rocketmq\V2\Status();
        $respStatus->setCode(\Apache\Rocketmq\V2\Code::OK);
        $response->setStatus($respStatus);

        return [$response, $status];
    }
}

/**
 * Fake TelemetrySession for trait testing.
 */
class FakeTelemetrySessionForTrait
{
    public $onRecoverOrphanedTransactionCallback = null;

    public function setOnRecoverOrphanedTransaction(callable $callback): void
    {
        $this->onRecoverOrphanedTransactionCallback = $callback;
    }
}

/**
 * Fake TransactionChecker implementations.
 */
class CommitChecker implements TransactionChecker
{
    public function check(MessageView $messageView): int
    {
        return TransactionResolution::COMMIT;
    }
}

class RollbackChecker implements TransactionChecker
{
    public function check(MessageView $messageView): int
    {
        return TransactionResolution::ROLLBACK;
    }
}

class UnspecifiedChecker implements TransactionChecker
{
    public function check(MessageView $messageView): int
    {
        return TransactionResolution::TRANSACTION_RESOLUTION_UNSPECIFIED;
    }
}

/**
 * Fake LocalTransactionExecuter implementations.
 */
class CommitExecuter implements LocalTransactionExecuter
{
    public bool $executed = false;

    public function execute(MessageView $messageView): int
    {
        $this->executed = true;
        return TransactionResolution::COMMIT;
    }
}

class RollbackExecuter implements LocalTransactionExecuter
{
    public bool $executed = false;

    public function execute(MessageView $messageView): int
    {
        $this->executed = true;
        return TransactionResolution::ROLLBACK;
    }
}

/**
 * TransactionTrait tests covering the 2PC (two-phase commit) flow.
 *
 * 2PC flow:
 * 1. Client sends half-message to broker (not visible to consumers)
 * 2. Client executes local transaction
 * 3. Based on result, client commits or rolls back the half-message
 * 4. If client crashes, broker sends RecoverOrphanedTransactionCommand
 *    and TransactionChecker resolves the orphaned transaction
 */
class TransactionTraitTest extends TestCase
{
    public function setUp(): void
    {
        Logger::close();
    }

    private function buildMessage(string $topic = 'test-topic', string $body = 'test body'): Message
    {
        $message = new Message();
        $message->setBody($body);
        $topicResource = new Resource();
        $topicResource->setName($topic);
        $message->setTopic($topicResource);
        return $message;
    }

    // ========================
    // 2PC Phase 1: Send half-message
    // ========================

    /**
     * Test that sendWithTransaction sends a half-message and returns result.
     */
    public function testSendWithTransactionReturnsResult()
    {
        $producer = new FakeProducerForTrait();
        $message = $this->buildMessage();
        $transaction = new Transaction($producer);

        $result = $producer->sendWithTransaction($message, $transaction);

        $this->assertArrayHasKey('messageId', $result, "Result should contain messageId");
        $this->assertArrayHasKey('transactionId', $result, "Result should contain transactionId");
    }

    /**
     * Test that sendWithTransaction adds message to transaction.
     */
    public function testSendWithTransactionAddsMessageToTransaction()
    {
        $producer = new FakeProducerForTrait();
        $message = $this->buildMessage();
        $transaction = new Transaction($producer);

        $producer->sendWithTransaction($message, $transaction);

        $this->assertCount(1, $transaction->getMessages(), "Transaction should have 1 message");
    }

    /**
     * Test that sendWithTransaction adds receipt to transaction.
     */
    public function testSendWithTransactionAddsReceipt()
    {
        $producer = new FakeProducerForTrait();
        $message = $this->buildMessage();
        $transaction = new Transaction($producer);

        $producer->sendWithTransaction($message, $transaction);

        $this->assertCount(1, $transaction->getReceipts(), "Transaction should have 1 receipt");
    }

    // ========================
    // 2PC Phase 2: Local transaction execution + commit/rollback
    // ========================

    /**
     * Test that executor returning COMMIT triggers transaction commit.
     */
    public function testSendWithTransactionAutoCommit()
    {
        $producer = new FakeProducerForTrait();
        $message = $this->buildMessage();
        $transaction = new Transaction($producer);
        $executer = new CommitExecuter();

        $producer->sendWithTransaction($message, $transaction, $executer);

        $this->assertTrue($executer->executed, "Executer should be called");
        $this->assertTrue($transaction->isCommitted(), "Transaction should be committed");
        $this->assertFalse($transaction->isRolledBack(), "Transaction should not be rolled back");
    }

    /**
     * Test that executor returning ROLLBACK triggers transaction rollback.
     */
    public function testSendWithTransactionAutoRollback()
    {
        $producer = new FakeProducerForTrait();
        $message = $this->buildMessage();
        $transaction = new Transaction($producer);
        $executer = new RollbackExecuter();

        $producer->sendWithTransaction($message, $transaction, $executer);

        $this->assertTrue($executer->executed, "Executer should be called");
        $this->assertTrue($transaction->isRolledBack(), "Transaction should be rolled back");
        $this->assertFalse($transaction->isCommitted(), "Transaction should not be committed");
    }

    /**
     * Test that without executor, transaction is not auto-resolved.
     */
    public function testSendWithTransactionNoAutoResolve()
    {
        $producer = new FakeProducerForTrait();
        $message = $this->buildMessage();
        $transaction = new Transaction($producer);

        $producer->sendWithTransaction($message, $transaction);

        $this->assertFalse($transaction->isCommitted(), "Transaction should not be committed without executor");
        $this->assertFalse($transaction->isRolledBack(), "Transaction should not be rolled back without executor");
    }

    // ========================
    // 2PC Phase 3: Manual commit/rollback
    // ========================

    /**
     * Test manual commit after sendWithTransaction.
     */
    public function testManualCommitAfterSend()
    {
        $producer = new FakeProducerForTrait();
        $message = $this->buildMessage();
        $transaction = new Transaction($producer);

        $producer->sendWithTransaction($message, $transaction);

        $this->assertFalse($transaction->isCommitted());
        $transaction->commit();
        $this->assertTrue($transaction->isCommitted(), "Manual commit should succeed");
    }

    /**
     * Test manual rollback after sendWithTransaction.
     */
    public function testManualRollbackAfterSend()
    {
        $producer = new FakeProducerForTrait();
        $message = $this->buildMessage();
        $transaction = new Transaction($producer);

        $producer->sendWithTransaction($message, $transaction);

        $this->assertFalse($transaction->isRolledBack());
        $transaction->rollback();
        $this->assertTrue($transaction->isRolledBack(), "Manual rollback should succeed");
    }

    // ========================
    // beginTransaction
    // ========================

    /**
     * Test beginTransaction without checker throws.
     */
    public function testBeginTransactionWithoutCheckerThrows()
    {
        $producer = new FakeProducerForTrait();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/checker/i');
        $producer->beginTransaction();
    }

    /**
     * Test beginTransaction with checker returns Transaction.
     */
    public function testBeginTransactionWithChecker()
    {
        $producer = new FakeProducerForTrait();
        $producer->setTransactionChecker(new CommitChecker());

        $transaction = $producer->beginTransaction();

        $this->assertInstanceOf(Transaction::class, $transaction);
    }

    /**
     * Test beginTransaction when producer is not running throws.
     */
    public function testBeginTransactionNotRunning()
    {
        $producer = new FakeProducerForTrait();
        $producer->isRunning = false;
        $producer->setTransactionChecker(new CommitChecker());

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/not running/i');
        $producer->beginTransaction();
    }

    // ========================
    // sendWithTransaction validations
    // ========================

    /**
     * Test sendWithTransaction when producer is not running throws.
     */
    public function testSendWithTransactionNotRunning()
    {
        $producer = new FakeProducerForTrait();
        $producer->isRunning = false;
        $message = $this->buildMessage();
        $transaction = new Transaction($producer);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/not running/i');
        $producer->sendWithTransaction($message, $transaction);
    }

    /**
     * Test sendWithTransaction rejects message with messageGroup.
     */
    public function testSendWithTransactionRejectsMessageGroup()
    {
        $producer = new FakeProducerForTrait();
        $message = $this->buildMessage();
        $sysProps = new SystemProperties();
        $sysProps->setMessageGroup('test-group');
        $message->setSystemProperties($sysProps);

        $transaction = new Transaction($producer);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/messageGroup/i');
        $producer->sendWithTransaction($message, $transaction);
    }

    /**
     * Test sendWithTransaction rejects message with deliveryTimestamp.
     */
    public function testSendWithTransactionRejectsDeliveryTimestamp()
    {
        $producer = new FakeProducerForTrait();
        $message = $this->buildMessage();
        $sysProps = new SystemProperties();
        $ts = new \Google\Protobuf\Timestamp();
        $ts->setSeconds(time() + 3600);
        $sysProps->setDeliveryTimestamp($ts);
        $message->setSystemProperties($sysProps);

        $transaction = new Transaction($producer);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/deliveryTimestamp/i');
        $producer->sendWithTransaction($message, $transaction);
    }

    // ========================
    // Orphaned transaction recovery (2PC Phase 4)
    // ========================

    /**
     * Test handleOrphanedTransaction with CommitChecker triggers commit.
     */
    public function testHandleOrphanedTransactionCommit()
    {
        $producer = new FakeProducerForTrait();
        $producer->setTransactionChecker(new CommitChecker());

        // Build orphaned transaction command with a message
        $message = $this->buildMessage('orphan-topic');
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('orphan-msg-id');
        $message->setSystemProperties($sysProps);

        $cmd = new \Apache\Rocketmq\V2\RecoverOrphanedTransactionCommand();
        $cmd->setTransactionId('orphan-tx-id');
        $cmd->setMessage($message);

        $producer->testHandleOrphanedTransaction($cmd);

        // Verify endTransaction was called by checking interceptor hooks (side effect of endTransaction)
        $commitHooks = array_filter($producer->interceptorCalls, fn($c) => $c['hookPoint'] === MessageHookPoints::COMMIT_TRANSACTION);
        $this->assertNotEmpty($commitHooks, "COMMIT_TRANSACTION hook should be triggered for orphaned tx recovery");
        $hookArgs = array_values($commitHooks)[0]['args'];
        $this->assertEquals('orphan-msg-id', $hookArgs['messageId']);
        $this->assertEquals('orphan-tx-id', $hookArgs['transactionId']);
        $this->assertEquals('orphan-topic', $hookArgs['topic']);
    }

    /**
     * Test handleOrphanedTransaction with RollbackChecker triggers rollback.
     */
    public function testHandleOrphanedTransactionRollback()
    {
        $producer = new FakeProducerForTrait();
        $producer->setTransactionChecker(new RollbackChecker());

        $message = $this->buildMessage('rollback-topic');
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('rb-msg-id');
        $message->setSystemProperties($sysProps);

        $cmd = new \Apache\Rocketmq\V2\RecoverOrphanedTransactionCommand();
        $cmd->setTransactionId('rb-tx-id');
        $cmd->setMessage($message);

        $producer->testHandleOrphanedTransaction($cmd);

        // Verify endTransaction was called via interceptor hooks
        $rollbackHooks = array_filter($producer->interceptorCalls, fn($c) => $c['hookPoint'] === MessageHookPoints::ROLLBACK_TRANSACTION);
        $this->assertNotEmpty($rollbackHooks, "ROLLBACK_TRANSACTION hook should be triggered for orphaned tx recovery");
    }

    /**
     * Test handleOrphanedTransaction with UnspecifiedChecker does not resolve.
     */
    public function testHandleOrphanedTransactionUnspecified()
    {
        $producer = new FakeProducerForTrait();
        $producer->setTransactionChecker(new UnspecifiedChecker());

        $message = $this->buildMessage('unspec-topic');
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('unspec-msg-id');
        $message->setSystemProperties($sysProps);

        $cmd = new \Apache\Rocketmq\V2\RecoverOrphanedTransactionCommand();
        $cmd->setTransactionId('unspec-tx-id');
        $cmd->setMessage($message);

        $producer->testHandleOrphanedTransaction($cmd);

        // UNSPECIFIED should NOT trigger endTransaction, so no interceptor hooks
        $this->assertEmpty($producer->interceptorCalls, "UNSPECIFIED resolution should not trigger endTransaction");
    }

    /**
     * Test handleOrphanedTransaction without checker logs warning.
     */
    public function testHandleOrphanedTransactionNoChecker()
    {
        $producer = new FakeProducerForTrait();
        // No checker set

        $cmd = new \Apache\Rocketmq\V2\RecoverOrphanedTransactionCommand();

        // Should not throw
        $producer->testHandleOrphanedTransaction($cmd);

        $this->assertEmpty($producer->interceptorCalls, "No checker should not trigger endTransaction");
    }

    /**
     * Test handleOrphanedTransaction with command that has no message.
     */
    public function testHandleOrphanedTransactionNoMessage()
    {
        $producer = new FakeProducerForTrait();
        $producer->setTransactionChecker(new CommitChecker());

        $cmd = new \Apache\Rocketmq\V2\RecoverOrphanedTransactionCommand();
        // No message set

        $producer->testHandleOrphanedTransaction($cmd);

        $this->assertEmpty($producer->interceptorCalls, "No message should not trigger endTransaction");
    }

    // ========================
    // TransactionChecker registration
    // ========================

    /**
     * Test registerTransactionCheckerCallback registers on TelemetrySession.
     */
    public function testRegisterTransactionCheckerCallback()
    {
        $producer = new FakeProducerForTrait();
        $producer->setTransactionChecker(new CommitChecker());

        $producer->testRegisterTransactionCheckerCallback();

        $this->assertNotNull(
            $producer->telemetrySession->onRecoverOrphanedTransactionCallback,
            "Callback should be registered on TelemetrySession"
        );
    }

    /**
     * Test registerTransactionCheckerCallback without checker does nothing.
     */
    public function testRegisterTransactionCheckerCallbackNoChecker()
    {
        $producer = new FakeProducerForTrait();
        // No checker set

        $producer->testRegisterTransactionCheckerCallback();

        $this->assertNull(
            $producer->telemetrySession->onRecoverOrphanedTransactionCallback,
            "No callback should be registered without checker"
        );
    }

    // ========================
    // Setter methods
    // ========================

    /**
     * Test setTransactionChecker returns self for chaining.
     */
    public function testSetTransactionCheckerReturnsSelf()
    {
        $producer = new FakeProducerForTrait();
        $result = $producer->setTransactionChecker(new CommitChecker());

        $this->assertSame($producer, $result, "setTransactionChecker should return self");
    }

    /**
     * Test setLocalTransactionExecuter returns self for chaining.
     */
    public function testSetLocalTransactionExecuterReturnsSelf()
    {
        $producer = new FakeProducerForTrait();
        $result = $producer->setLocalTransactionExecuter(new CommitExecuter());

        $this->assertSame($producer, $result, "setLocalTransactionExecuter should return self");
    }

    // ========================
    // Full 2PC flow integration
    // ========================

    /**
     * Test complete 2PC flow: send half-message -> execute local tx -> commit.
     */
    public function testFullTwoPhaseCommitFlow()
    {
        $producer = new FakeProducerForTrait();
        $producer->setTransactionChecker(new CommitChecker());

        // Phase 1: Begin transaction and send half-message
        $transaction = $producer->beginTransaction();
        $message = $this->buildMessage('order-topic', 'order-data');

        // Phase 2: Execute local transaction (auto-commit)
        $executer = new CommitExecuter();
        $result = $producer->sendWithTransaction($message, $transaction, $executer);

        // Verify full flow
        $this->assertTrue($executer->executed, "Local transaction should be executed");
        $this->assertTrue($transaction->isCommitted(), "Transaction should be committed");
        $this->assertArrayHasKey('messageId', $result, "Result should contain messageId");
        $this->assertArrayHasKey('transactionId', $result, "Result should contain transactionId");
    }

    /**
     * Test complete 2PC flow with manual commit.
     */
    public function testFullTwoPhaseCommitFlowManual()
    {
        $producer = new FakeProducerForTrait();
        $producer->setTransactionChecker(new CommitChecker());

        // Phase 1: Begin transaction and send half-message
        $transaction = $producer->beginTransaction();
        $message = $this->buildMessage('payment-topic', 'payment-data');

        // Send without executor
        $result = $producer->sendWithTransaction($message, $transaction);

        // Phase 2: Manual commit
        $this->assertFalse($transaction->isCommitted());
        $transaction->commit();
        $this->assertTrue($transaction->isCommitted(), "Manual commit should succeed");

        // Verify commit was forwarded to committer
        $this->assertCount(1, $producer->commitCalls, "commitTransaction should be called once");
    }

    /**
     * Test complete 2PC flow with manual rollback.
     */
    public function testFullTwoPhaseRollbackFlow()
    {
        $producer = new FakeProducerForTrait();
        $producer->setTransactionChecker(new CommitChecker());

        $transaction = $producer->beginTransaction();
        $message = $this->buildMessage('cancel-topic', 'cancel-data');

        $result = $producer->sendWithTransaction($message, $transaction);

        $this->assertFalse($transaction->isRolledBack());
        $transaction->rollback();
        $this->assertTrue($transaction->isRolledBack(), "Manual rollback should succeed");

        $this->assertCount(1, $producer->rollbackCalls, "rollbackTransaction should be called once");
    }

    /**
     * Test orphaned transaction recovery after client crash simulation.
     */
    public function testOrphanedTransactionRecoveryFlow()
    {
        // Simulate: Producer crashes after sending half-message
        // Broker sends RecoverOrphanedTransactionCommand to new producer instance

        $producer = new FakeProducerForTrait();
        $checker = new CommitChecker();
        $producer->setTransactionChecker($checker);

        // Register callback
        $producer->testRegisterTransactionCheckerCallback();
        $this->assertNotNull($producer->telemetrySession->onRecoverOrphanedTransactionCallback);

        // Simulate broker sending orphaned transaction command
        $message = $this->buildMessage('orphan-recovery-topic');
        $sysProps = new SystemProperties();
        $sysProps->setMessageId('orphan-recovery-msg-id');
        $message->setSystemProperties($sysProps);

        $cmd = new \Apache\Rocketmq\V2\RecoverOrphanedTransactionCommand();
        $cmd->setTransactionId('orphan-recovery-tx-id');
        $cmd->setMessage($message);

        // Invoke the callback (simulating what TelemetrySession would do)
        $callback = $producer->telemetrySession->onRecoverOrphanedTransactionCallback;
        $callback($cmd);

        // Verify orphaned transaction was resolved via interceptor hooks
        $commitHooks = array_filter($producer->interceptorCalls, fn($c) => $c['hookPoint'] === MessageHookPoints::COMMIT_TRANSACTION);
        $this->assertNotEmpty($commitHooks, "Orphaned transaction should be resolved with COMMIT");
        $hookArgs = array_values($commitHooks)[0]['args'];
        $this->assertEquals('orphan-recovery-msg-id', $hookArgs['messageId']);
        $this->assertEquals('orphan-recovery-tx-id', $hookArgs['transactionId']);
    }

    // ========================
    // Interceptor integration
    // ========================

    /**
     * Test that commit triggers COMMIT_TRANSACTION interceptor hook.
     */
    public function testCommitTriggersInterceptor()
    {
        $producer = new FakeProducerForTrait();

        $producer->testEndTransaction('msg-1', 'tx-1', 'test-topic', TransactionResolution::COMMIT);

        $commitHooks = array_filter($producer->interceptorCalls, function ($call) {
            return $call['hookPoint'] === MessageHookPoints::COMMIT_TRANSACTION;
        });
        $this->assertNotEmpty($commitHooks, "COMMIT_TRANSACTION hook should be triggered");
    }

    /**
     * Test that rollback triggers ROLLBACK_TRANSACTION interceptor hook.
     */
    public function testRollbackTriggersInterceptor()
    {
        $producer = new FakeProducerForTrait();

        $producer->testEndTransaction('msg-1', 'tx-1', 'test-topic', TransactionResolution::ROLLBACK);

        $rollbackHooks = array_filter($producer->interceptorCalls, function ($call) {
            return $call['hookPoint'] === MessageHookPoints::ROLLBACK_TRANSACTION;
        });
        $this->assertNotEmpty($rollbackHooks, "ROLLBACK_TRANSACTION hook should be triggered");
    }
}
