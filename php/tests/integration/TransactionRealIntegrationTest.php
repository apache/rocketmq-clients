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
use Apache\Rocketmq\Transaction;
use Apache\Rocketmq\TransactionChecker;
use Apache\Rocketmq\LocalTransactionExecuter;
use Apache\Rocketmq\MessageView;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\TransactionResolution;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../../Producer.php';
require_once __DIR__ . '/../../Transaction.php';

/**
 * TransactionRealIntegrationTest - Tests transaction messages against a real RocketMQ cluster.
 *
 * Prerequisites:
 * - NameServer cluster running at 127.0.0.1:8081
 * - Topic "TopicTestForNormal" exists (auto-create enabled)
 * - Producer GID auto-created
 *
 * Tests:
 * 1. Send transaction message + commit
 * 2. Send transaction message + rollback
 * 3. Send transaction message with LocalTransactionExecuter (auto commit)
 * 4. Send transaction message with LocalTransactionExecuter (auto rollback)
 * 5. Multiple sequential transaction commits
 * 6. Transaction state verification (isCommitted / isRolledBack)
 */
class TransactionRealIntegrationTest extends IntegrationTestCase
{
    private const ENDPOINTS = '127.0.0.1:8081';
    private const TOPIC = 'TopicTestForNormal';

    private ?Producer $producer = null;

    protected function setUp(): void
    {
        parent::setUp();
    }

    protected function tearDown(): void
    {
        if ($this->producer !== null) {
            try {
                $this->producer->shutdown();
            } catch (\Throwable $e) {
                // ignore
            }
            $this->producer = null;
        }
        parent::tearDown();
    }

    /**
     * Create a Producer connected to the real cluster.
     */
    private function createProducer(): Producer
    {
        $producer = new Producer(self::ENDPOINTS, [
            'topics' => [self::TOPIC],
            'sslEnabled' => false,
            'maxAttempts' => 3,
            'requestTimeout' => 5000,
        ]);

        // Set a TransactionChecker (required for beginTransaction)
        $producer->setTransactionChecker(new class implements TransactionChecker {
            public function check(MessageView $messageView): int
            {
                // Default: commit orphaned transactions
                return TransactionResolution::COMMIT;
            }
        });

        $producer->start();
        $this->producer = $producer;
        return $producer;
    }

    /**
     * Build a message for the test topic.
     */
    private function buildMessage(string $body, string $tag = ''): Message
    {
        $topicResource = new Resource();
        $topicResource->setName(self::TOPIC);

        $sysProps = new SystemProperties();
        if (!empty($tag)) {
            $sysProps->setTag($tag);
        }
        $sysProps->setKeys(['tx-test-' . uniqid()]);

        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody($body);
        $message->setSystemProperties($sysProps);

        return $message;
    }

    // ==================== Test Cases ====================

    /**
     * Test 1: Send transaction half-message and COMMIT.
     * Verifies: half-message sends successfully, commit completes without error.
     */
    public function testTransactionCommit(): void
    {
        $producer = $this->createProducer();

        $message = $this->buildMessage('Transaction commit test - ' . date('Y-m-d H:i:s'), 'commit-tag');
        $transaction = $producer->beginTransaction();

        // Send half-message
        $result = $producer->sendWithTransaction($message, $transaction);

        $this->assertArrayHasKey('messageId', $result, 'Send result should contain messageId');
        $this->assertArrayHasKey('transactionId', $result, 'Send result should contain transactionId');
        $this->assertNotEmpty($result['messageId'], 'messageId should not be empty');
        $this->assertNotEmpty($result['transactionId'], 'transactionId should not be empty');

        echo "\n[COMMIT TEST] Half-message sent: messageId={$result['messageId']}, transactionId={$result['transactionId']}";

        // Commit the transaction
        $transaction->commit();

        $this->assertTrue($transaction->isCommitted(), 'Transaction should be committed');
        $this->assertFalse($transaction->isRolledBack(), 'Transaction should not be rolled back');

        echo "\n[COMMIT TEST] Transaction committed successfully\n";
    }

    /**
     * Test 2: Send transaction half-message and ROLLBACK.
     * Verifies: half-message sends successfully, rollback completes without error.
     */
    public function testTransactionRollback(): void
    {
        $producer = $this->createProducer();

        $message = $this->buildMessage('Transaction rollback test - ' . date('Y-m-d H:i:s'), 'rollback-tag');
        $transaction = $producer->beginTransaction();

        // Send half-message
        $result = $producer->sendWithTransaction($message, $transaction);

        $this->assertArrayHasKey('messageId', $result, 'Send result should contain messageId');
        $this->assertArrayHasKey('transactionId', $result, 'Send result should contain transactionId');
        $this->assertNotEmpty($result['messageId'], 'messageId should not be empty');
        $this->assertNotEmpty($result['transactionId'], 'transactionId should not be empty');

        echo "\n[ROLLBACK TEST] Half-message sent: messageId={$result['messageId']}, transactionId={$result['transactionId']}";

        // Rollback the transaction
        $transaction->rollback();

        $this->assertTrue($transaction->isRolledBack(), 'Transaction should be rolled back');
        $this->assertFalse($transaction->isCommitted(), 'Transaction should not be committed');

        echo "\n[ROLLBACK TEST] Transaction rolled back successfully\n";
    }

    /**
     * Test 3: Transaction with LocalTransactionExecuter that returns COMMIT.
     * Verifies: executor is called, transaction auto-commits.
     */
    public function testTransactionWithExecutorCommit(): void
    {
        $producer = $this->createProducer();

        $tracker = new \stdClass();
        $tracker->called = false;
        $executor = new class($tracker) implements LocalTransactionExecuter {
            private \stdClass $tracker;
            public function __construct(\stdClass $tracker)
            {
                $this->tracker = $tracker;
            }
            public function execute(MessageView $messageView): int
            {
                $this->tracker->called = true;
                echo "\n[EXECUTOR-COMMIT TEST] Local transaction executed, returning COMMIT";
                return TransactionResolution::COMMIT;
            }
        };

        $message = $this->buildMessage('Transaction executor-commit test - ' . date('Y-m-d H:i:s'), 'exec-commit-tag');
        $transaction = $producer->beginTransaction();

        // Send half-message with executor - auto commits
        $result = $producer->sendWithTransaction($message, $transaction, $executor);

        $this->assertArrayHasKey('messageId', $result, 'Send result should contain messageId');
        $this->assertTrue($tracker->called, 'Executor should have been called');
        $this->assertTrue($transaction->isCommitted(), 'Transaction should be auto-committed by executor');

        echo "\n[EXECUTOR-COMMIT TEST] Transaction auto-committed via executor, messageId={$result['messageId']}\n";
    }

    /**
     * Test 4: Transaction with LocalTransactionExecuter that returns ROLLBACK.
     * Verifies: executor is called, transaction auto-rolls back.
     */
    public function testTransactionWithExecutorRollback(): void
    {
        $producer = $this->createProducer();

        $tracker = new \stdClass();
        $tracker->called = false;
        $executor = new class($tracker) implements LocalTransactionExecuter {
            private \stdClass $tracker;
            public function __construct(\stdClass $tracker)
            {
                $this->tracker = $tracker;
            }
            public function execute(MessageView $messageView): int
            {
                $this->tracker->called = true;
                echo "\n[EXECUTOR-ROLLBACK TEST] Local transaction executed, returning ROLLBACK";
                return TransactionResolution::ROLLBACK;
            }
        };

        $message = $this->buildMessage('Transaction executor-rollback test - ' . date('Y-m-d H:i:s'), 'exec-rollback-tag');
        $transaction = $producer->beginTransaction();

        // Send half-message with executor - auto rolls back
        $result = $producer->sendWithTransaction($message, $transaction, $executor);

        $this->assertArrayHasKey('messageId', $result, 'Send result should contain messageId');
        $this->assertTrue($tracker->called, 'Executor should have been called');
        $this->assertTrue($transaction->isRolledBack(), 'Transaction should be auto-rolled-back by executor');

        echo "\n[EXECUTOR-ROLLBACK TEST] Transaction auto-rolled-back via executor, messageId={$result['messageId']}\n";
    }

    /**
     * Test 5: Multiple sequential transaction commits.
     * Verifies: can send and commit multiple transactions in sequence.
     */
    public function testMultipleSequentialTransactionCommits(): void
    {
        $producer = $this->createProducer();
        $commitCount = 3;

        for ($i = 1; $i <= $commitCount; $i++) {
            $message = $this->buildMessage("Sequential tx commit #{$i} - " . date('Y-m-d H:i:s'), "seq-commit-{$i}");
            $transaction = $producer->beginTransaction();

            $result = $producer->sendWithTransaction($message, $transaction);
            $this->assertNotEmpty($result['messageId'], "Message #{$i} should have messageId");

            $transaction->commit();
            $this->assertTrue($transaction->isCommitted(), "Transaction #{$i} should be committed");

            echo "\n[SEQUENTIAL TEST] Transaction #{$i} committed: messageId={$result['messageId']}";
        }

        echo "\n[SEQUENTIAL TEST] All {$commitCount} transactions committed successfully\n";
    }

    /**
     * Test 6: Transaction state transitions - cannot commit after rollback and vice versa.
     */
    public function testTransactionStateTransitions(): void
    {
        $producer = $this->createProducer();

        // Test: cannot commit after rollback
        $message1 = $this->buildMessage('State test - rollback then commit attempt');
        $tx1 = $producer->beginTransaction();
        $producer->sendWithTransaction($message1, $tx1);
        $tx1->rollback();

        $this->expectException(\RuntimeException::class);
        $tx1->commit(); // should throw
    }

    /**
     * Test 7: Cannot rollback after commit.
     */
    public function testCannotRollbackAfterCommit(): void
    {
        $producer = $this->createProducer();

        $message = $this->buildMessage('State test - commit then rollback attempt');
        $tx = $producer->beginTransaction();
        $producer->sendWithTransaction($message, $tx);
        $tx->commit();

        $this->expectException(\RuntimeException::class);
        $tx->rollback(); // should throw
    }

    /**
     * Test 8: Transaction without TransactionChecker should fail on beginTransaction.
     */
    public function testBeginTransactionWithoutCheckerThrows(): void
    {
        $producer = new Producer(self::ENDPOINTS, [
            'topics' => [self::TOPIC],
            'sslEnabled' => false,
            'maxAttempts' => 3,
            'requestTimeout' => 5000,
        ]);
        // Do NOT set TransactionChecker
        $producer->start();
        $this->producer = $producer;

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/TransactionChecker/i');
        $producer->beginTransaction();
    }

    /**
     * Test 9: Send transaction message on Producer that is not started should fail.
     */
    public function testSendTransactionOnStoppedProducerThrows(): void
    {
        $producer = new Producer(self::ENDPOINTS, [
            'topics' => [self::TOPIC],
            'sslEnabled' => false,
        ]);
        $producer->setTransactionChecker(new class implements TransactionChecker {
            public function check(MessageView $messageView): int
            {
                return TransactionResolution::COMMIT;
            }
        });
        // Do NOT start

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/not running/i');
        $producer->beginTransaction();
    }

    /**
     * Test 10: Transaction message should not allow messageGroup (FIFO).
     */
    public function testTransactionMessageRejectsMessageGroup(): void
    {
        $producer = $this->createProducer();

        $topicResource = new Resource();
        $topicResource->setName(self::TOPIC);

        $sysProps = new SystemProperties();
        $sysProps->setMessageGroup('test-group');

        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody('FIFO message in transaction');
        $message->setSystemProperties($sysProps);

        $transaction = $producer->beginTransaction();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/messageGroup|deliveryTimestamp|liteTopic|priority/i');
        $producer->sendWithTransaction($message, $transaction);
    }

    /**
     * Test 11: TransactionChecker returning COMMIT resolves orphaned transactions.
     * Verifies: the checker is set correctly and the producer starts without errors.
     */
    public function testTransactionCheckerIsRegistered(): void
    {
        $checkerCalled = false;
        $producer = $this->createProducer();

        // The checker was set in createProducer(). We verify the producer starts
        // successfully with a TransactionChecker registered (the callback is wired up
        // during start()). If the checker registration failed, start() would still succeed
        // but orphaned transactions would not be handled.
        $this->assertTrue($producer->isRunning(), 'Producer should be running with TransactionChecker registered');

        // Verify beginTransaction works (requires TransactionChecker)
        $tx = $producer->beginTransaction();
        $this->assertInstanceOf(Transaction::class, $tx, 'beginTransaction should return a Transaction instance');

        echo "\n[CHECKER TEST] TransactionChecker registered and beginTransaction works\n";
    }

    /**
     * Test 12: Mixed commit and rollback sequence.
     * Verifies: interleaved commit and rollback operations work correctly.
     */
    public function testMixedCommitAndRollbackSequence(): void
    {
        $producer = $this->createProducer();
        $operations = ['commit', 'rollback', 'commit', 'commit', 'rollback'];

        foreach ($operations as $i => $op) {
            $message = $this->buildMessage("Mixed op #{$i}: {$op} - " . date('Y-m-d H:i:s'), "mixed-{$op}");
            $transaction = $producer->beginTransaction();
            $result = $producer->sendWithTransaction($message, $transaction);

            $this->assertNotEmpty($result['messageId'], "Message #{$i} should have messageId");

            if ($op === 'commit') {
                $transaction->commit();
                $this->assertTrue($transaction->isCommitted(), "Op #{$i} should be committed");
            } else {
                $transaction->rollback();
                $this->assertTrue($transaction->isRolledBack(), "Op #{$i} should be rolled back");
            }

            echo "\n[MIXED TEST] Op #{$i} ({$op}): messageId={$result['messageId']}";
        }

        echo "\n[MIXED TEST] All " . count($operations) . " operations completed successfully\n";
    }
}
