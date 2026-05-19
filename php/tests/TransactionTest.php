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
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\Test\TestRunner;

/**
 * Fake producer for transaction testing without network calls.
 */
class FakeProducerForTransaction
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

class TransactionTest
{
    public function testTryAddMessage()
    {
        $fakeProducer = new FakeProducerForTransaction();
        $transaction = new Transaction($fakeProducer);

        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody('test body');

        $transaction->tryAddMessage($message);
        TestRunner::assertTrueWithMessage(true, "Message should be added to transaction");
    }

    public function testTryAddReceipt()
    {
        $fakeProducer = new FakeProducerForTransaction();
        $transaction = new Transaction($fakeProducer);

        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody('test body');

        $sendResult = [
            'messageId' => 'test-msg-id-1',
            'transactionId' => 'test-tx-id-1',
        ];

        $transaction->tryAddReceipt($message, $sendResult);
        TestRunner::assertTrueWithMessage(true, "Receipt should be recorded");
    }

    public function testCommit()
    {
        $fakeProducer = new FakeProducerForTransaction();
        $transaction = new Transaction($fakeProducer);

        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody('test body');

        $sendResult = [
            'messageId' => 'test-msg-id-1',
            'transactionId' => 'test-tx-id-1',
        ];

        $transaction->tryAddReceipt($message, $sendResult);
        $transaction->commit();

        TestRunner::assertEqualsWithMessage(
            1,
            count($fakeProducer->commitCalls),
            "Commit should be called once"
        );
        TestRunner::assertEqualsWithMessage(
            'test-msg-id-1',
            $fakeProducer->commitCalls[0]['messageId'],
            "Commit should use the correct message ID"
        );
        TestRunner::assertEqualsWithMessage(
            'test-tx-id-1',
            $fakeProducer->commitCalls[0]['transactionId'],
            "Commit should use the correct transaction ID"
        );
    }

    public function testRollback()
    {
        $fakeProducer = new FakeProducerForTransaction();
        $transaction = new Transaction($fakeProducer);

        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody('test body');

        $sendResult = [
            'messageId' => 'test-msg-id-2',
            'transactionId' => 'test-tx-id-2',
        ];

        $transaction->tryAddReceipt($message, $sendResult);
        $transaction->rollback();

        TestRunner::assertEqualsWithMessage(
            1,
            count($fakeProducer->rollbackCalls),
            "Rollback should be called once"
        );
        TestRunner::assertEqualsWithMessage(
            'test-msg-id-2',
            $fakeProducer->rollbackCalls[0]['messageId'],
            "Rollback should use the correct message ID"
        );
    }

    public function testCommitClearsReceipts()
    {
        $fakeProducer = new FakeProducerForTransaction();
        $transaction = new Transaction($fakeProducer);

        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody('test body');

        $sendResult = ['messageId' => 'msg-1', 'transactionId' => 'tx-1'];
        $transaction->tryAddReceipt($message, $sendResult);
        $transaction->commit();

        // After commit, receipts should be cleared (commit/rollback would call on empty array)
        TestRunner::assertEqualsWithMessage(
            1,
            count($fakeProducer->commitCalls),
            "Only one commit should have been recorded"
        );
    }

    public function testMultipleMessagesInTransaction()
    {
        $fakeProducer = new FakeProducerForTransaction();
        $transaction = new Transaction($fakeProducer);

        for ($i = 0; $i < 3; $i++) {
            $topicResource = new Resource();
            $topicResource->setName("test-topic-{$i}");
            $message = new Message();
            $message->setTopic($topicResource);
            $message->setBody("body-{$i}");

            $sendResult = [
                'messageId' => "msg-id-{$i}",
                'transactionId' => "tx-id-{$i}",
            ];
            $transaction->tryAddReceipt($message, $sendResult);
        }

        $transaction->commit();

        TestRunner::assertEqualsWithMessage(
            3,
            count($fakeProducer->commitCalls),
            "Should commit all 3 messages"
        );
    }
}

echo "=== TransactionTest ===\n";
$test = new TransactionTest();
$test->testTryAddMessage();
echo "  [OK] testTryAddMessage\n";
$test->testTryAddReceipt();
echo "  [OK] testTryAddReceipt\n";
$test->testCommit();
echo "  [OK] testCommit\n";
$test->testRollback();
echo "  [OK] testRollback\n";
$test->testCommitClearsReceipts();
echo "  [OK] testCommitClearsReceipts\n";
$test->testMultipleMessagesInTransaction();
echo "  [OK] testMultipleMessagesInTransaction\n";
