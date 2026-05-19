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

require_once __DIR__ . '/TestRunner.php';
require_once __DIR__ . '/../ProducerOptimized.php';

use Apache\Rocketmq\Transaction;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\Test\TestRunner;

/**
 * Fake producer for extended transaction testing.
 */
class FakeProducerForExtended
{
    public $commitCalls = [];
    public $rollbackCalls = [];

    public function commitTransaction($messageId, $transactionId, $topic)
    {
        $this->commitCalls[] = [
            'messageId' => $messageId,
            'transactionId' => $transactionId,
            'topic' => $topic,
        ];
    }

    public function rollbackTransaction($messageId, $transactionId, $topic)
    {
        $this->rollbackCalls[] = [
            'messageId' => $messageId,
            'transactionId' => $transactionId,
            'topic' => $topic,
        ];
    }
}

/**
 * Extended transaction tests mirroring Java's TransactionImplTest.
 * Tests: tryAddExceededMessages, tryAddReceiptNotContained,
 * commitWithNoReceipts, rollbackWithNoReceipts.
 *
 * Note: PHP's Transaction class does not enforce the single-message
 * limit or receipt containment checks that Java's TransactionImpl does.
 * These tests verify PHP's actual behavior.
 */
class TransactionExtendedTest
{
    /**
     * Mirrors Java: testTryAddExceededMessages.
     * Java limits a transaction to one message. PHP does not have this limit,
     * so adding multiple messages should all succeed.
     */
    public function testTryAddMultipleMessages()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        $msg1 = $this->buildMessage('topic-1', 'body-1');
        $msg2 = $this->buildMessage('topic-2', 'body-2');

        // Both adds should succeed (PHP has no single-message limit)
        $transaction->tryAddMessage($msg1);
        $transaction->tryAddMessage($msg2);

        TestRunner::assertTrueWithMessage(true,
            "PHP Transaction allows multiple messages (unlike Java's single-message limit)");
    }

    /**
     * Mirrors Java: testTryAddReceiptNotContained.
     * Java checks that the message was added before adding a receipt.
     * PHP does not validate this, so adding a receipt for any message succeeds.
     */
    public function testTryAddReceiptForUnaddedMessage()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        $msg = $this->buildMessage('topic-1', 'body-1');

        // In Java this throws; in PHP it succeeds
        $sendResult = [
            'messageId' => 'msg-id-1',
            'transactionId' => 'tx-id-1',
        ];
        $transaction->tryAddReceipt($msg, $sendResult);

        TestRunner::assertTrueWithMessage(true,
            "PHP Transaction allows adding receipt without prior tryAddMessage");
    }

    /**
     * Mirrors Java: testCommitWithNoReceipts.
     * Java throws IllegalStateException. PHP silently does nothing.
     */
    public function testCommitWithNoReceipts()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        // No messages or receipts added
        $transaction->commit();

        TestRunner::assertEqualsWithMessage(
            0,
            count($fakeProducer->commitCalls),
            "PHP commit with no receipts should do nothing (no exception)"
        );
    }

    /**
     * Mirrors Java: testRollbackWithNoReceipts.
     * Java throws IllegalStateException. PHP silently does nothing.
     */
    public function testRollbackWithNoReceipts()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        // No messages or receipts added
        $transaction->rollback();

        TestRunner::assertEqualsWithMessage(
            0,
            count($fakeProducer->rollbackCalls),
            "PHP rollback with no receipts should do nothing (no exception)"
        );
    }

    /**
     * Full transaction flow: add messages with different topics,
     * add receipts, commit, verify all are committed.
     */
    public function testCommitMultipleTopics()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        $topics = ['order-topic', 'payment-topic', 'notification-topic'];
        foreach ($topics as $i => $topic) {
            $msg = $this->buildMessage($topic, "body-{$i}");
            $sendResult = [
                'messageId' => "msg-id-{$i}",
                'transactionId' => "tx-id-{$i}",
            ];
            $transaction->tryAddReceipt($msg, $sendResult);
        }

        $transaction->commit();

        TestRunner::assertEqualsWithMessage(
            3,
            count($fakeProducer->commitCalls),
            "All 3 messages should be committed"
        );

        for ($i = 0; $i < 3; $i++) {
            TestRunner::assertEqualsWithMessage(
                "msg-id-{$i}",
                $fakeProducer->commitCalls[$i]['messageId'],
                "Commit {$i} should have correct messageId"
            );
            TestRunner::assertEqualsWithMessage(
                "tx-id-{$i}",
                $fakeProducer->commitCalls[$i]['transactionId'],
                "Commit {$i} should have correct transactionId"
            );
            TestRunner::assertEqualsWithMessage(
                $topics[$i],
                $fakeProducer->commitCalls[$i]['topic'],
                "Commit {$i} should have correct topic"
            );
        }
    }

    /**
     * Rollback multiple messages and verify all are rolled back.
     */
    public function testRollbackMultipleMessages()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        for ($i = 0; $i < 2; $i++) {
            $msg = $this->buildMessage("rollback-topic-{$i}", "body-{$i}");
            $sendResult = [
                'messageId' => "rb-msg-id-{$i}",
                'transactionId' => "rb-tx-id-{$i}",
            ];
            $transaction->tryAddReceipt($msg, $sendResult);
        }

        $transaction->rollback();

        TestRunner::assertEqualsWithMessage(
            2,
            count($fakeProducer->rollbackCalls),
            "All 2 messages should be rolled back"
        );
    }

    /**
     * Tests that after commit, a second commit does nothing
     * (receipts are cleared after commit).
     */
    public function testDoubleCommitDoesNothing()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        $msg = $this->buildMessage('test-topic', 'test body');
        $sendResult = [
            'messageId' => 'msg-id',
            'transactionId' => 'tx-id',
        ];
        $transaction->tryAddReceipt($msg, $sendResult);

        $transaction->commit();
        $transaction->commit(); // Second commit

        TestRunner::assertEqualsWithMessage(
            1,
            count($fakeProducer->commitCalls),
            "Second commit should do nothing (receipts cleared after first)"
        );
    }

    /**
     * Tests the addReceipt() alias method.
     */
    public function testAddReceiptAlias()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        $msg = $this->buildMessage('test-topic', 'test body');
        $sendResult = [
            'messageId' => 'alias-msg-id',
            'transactionId' => 'alias-tx-id',
        ];

        // Use the addReceipt alias instead of tryAddReceipt
        $transaction->addReceipt($msg, $sendResult);
        $transaction->commit();

        TestRunner::assertEqualsWithMessage(
            1,
            count($fakeProducer->commitCalls),
            "addReceipt alias should work same as tryAddReceipt"
        );
        TestRunner::assertEqualsWithMessage(
            'alias-msg-id',
            $fakeProducer->commitCalls[0]['messageId'],
            "Alias should record correct messageId"
        );
    }

    /**
     * Helper to build a Message protobuf object.
     */
    private function buildMessage($topic, $body)
    {
        $message = new Message();
        $message->setBody($body);
        $topicResource = new Resource();
        $topicResource->setName($topic);
        $message->setTopic($topicResource);
        return $message;
    }
}

echo "=== TransactionExtendedTest ===\n";
$test = new TransactionExtendedTest();
$test->testTryAddMultipleMessages();
echo "  [OK] testTryAddMultipleMessages\n";
$test->testTryAddReceiptForUnaddedMessage();
echo "  [OK] testTryAddReceiptForUnaddedMessage\n";
$test->testCommitWithNoReceipts();
echo "  [OK] testCommitWithNoReceipts\n";
$test->testRollbackWithNoReceipts();
echo "  [OK] testRollbackWithNoReceipts\n";
$test->testCommitMultipleTopics();
echo "  [OK] testCommitMultipleTopics\n";
$test->testRollbackMultipleMessages();
echo "  [OK] testRollbackMultipleMessages\n";
$test->testDoubleCommitDoesNothing();
echo "  [OK] testDoubleCommitDoesNothing\n";
$test->testAddReceiptAlias();
echo "  [OK] testAddReceiptAlias\n";
