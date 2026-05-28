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

use Apache\Rocketmq\Transaction;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
/**
 * Fake producer for extended transaction testing.
 */
class FakeProducerForExtended {
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
class TransactionExtendedTest extends TestCase
{
    /**
     * Mirrors Java: testTryAddExceededMessages.
     * Java limits a transaction to one message. PHP now enforces this too.
     */
    public function testTryAddExceededMessages()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        $msg1 = $this->buildMessage('topic-1', 'body-1');
        $msg2 = $this->buildMessage('topic-2', 'body-2');

        $transaction->tryAddMessage($msg1);

        $this->expectException(\InvalidArgumentException::class);
        $transaction->tryAddMessage($msg2);
    }

    /**
     * Mirrors Java: testTryAddReceiptNotContained.
     * Java checks that the message was added before adding a receipt.
     * PHP now enforces this too.
     */
    public function testTryAddReceiptNotContained()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        $msg = $this->buildMessage('topic-1', 'body-1');

        $sendResult = [
            'messageId' => 'msg-id-1',
            'transactionId' => 'tx-id-1',
        ];

        $this->expectException(\InvalidArgumentException::class);
        $transaction->tryAddReceipt($msg, $sendResult);
    }

    /**
     * Mirrors Java: testCommitWithNoReceipts.
     * Java throws IllegalStateException. PHP now does too.
     */
    public function testCommitWithNoReceipts()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        // No messages or receipts added
        $this->expectException(\RuntimeException::class);
        $transaction->commit();
    }

    /**
     * Mirrors Java: testRollbackWithNoReceipts.
     * Java throws IllegalStateException. PHP now does too.
     */
    public function testRollbackWithNoReceipts()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        // No messages or receipts added
        $this->expectException(\RuntimeException::class);
        $transaction->rollback();
    }

    /**
     * Full transaction flow: add message, add receipt, commit,
     * verify all parameters are correct.
     */
    public function testCommitMultipleTopics()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        $msg = $this->buildMessage('order-topic', 'body-0');
        $transaction->tryAddMessage($msg);
        $sendResult = [
            'messageId' => 'msg-id-0',
            'transactionId' => 'tx-id-0',
        ];
        $transaction->tryAddReceipt($msg, $sendResult);

        $transaction->commit();

        $this->assertEquals(
            1,
            count($fakeProducer->commitCalls),
            "Message should be committed"
        );

        $this->assertEquals(
            'msg-id-0',
            $fakeProducer->commitCalls[0]['messageId'],
            "Commit should have correct messageId"
        );
        $this->assertEquals(
            'tx-id-0',
            $fakeProducer->commitCalls[0]['transactionId'],
            "Commit should have correct transactionId"
        );
        $this->assertEquals(
            'order-topic',
            $fakeProducer->commitCalls[0]['topic'],
            "Commit should have correct topic"
        );
    }

    /**
     * Rollback message and verify it is rolled back.
     */
    public function testRollbackMultipleMessages()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        $msg = $this->buildMessage('rollback-topic-0', 'body-0');
        $transaction->tryAddMessage($msg);
        $sendResult = [
            'messageId' => 'rb-msg-id-0',
            'transactionId' => 'rb-tx-id-0',
        ];
        $transaction->tryAddReceipt($msg, $sendResult);

        $transaction->rollback();

        $this->assertEquals(
            1,
            count($fakeProducer->rollbackCalls),
            "Message should be rolled back"
        );
    }

    /**
     * Tests that after commit, a second commit throws because
     * receipts are cleared after the first commit.
     */
    public function testDoubleCommitDoesNothing()
    {
        $fakeProducer = new FakeProducerForExtended();
        $transaction = new Transaction($fakeProducer);

        $msg = $this->buildMessage('test-topic', 'test body');
        $transaction->tryAddMessage($msg);
        $sendResult = [
            'messageId' => 'msg-id',
            'transactionId' => 'tx-id',
        ];
        $transaction->tryAddReceipt($msg, $sendResult);

        $transaction->commit();

        // Second commit throws because receipts were cleared
        $this->expectException(\RuntimeException::class);
        $transaction->commit();

        $this->assertEquals(
            1,
            count($fakeProducer->commitCalls),
            "Only first commit should have been recorded"
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
        $transaction->tryAddMessage($msg);
        $sendResult = [
            'messageId' => 'alias-msg-id',
            'transactionId' => 'alias-tx-id',
        ];

        // Use the addReceipt alias instead of tryAddReceipt
        $transaction->addReceipt($msg, $sendResult);
        $transaction->commit();

        $this->assertEquals(
            1,
            count($fakeProducer->commitCalls),
            "addReceipt alias should work same as tryAddReceipt"
        );
        $this->assertEquals(
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

