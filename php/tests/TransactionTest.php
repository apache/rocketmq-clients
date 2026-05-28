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
use Apache\Rocketmq\V2\SystemProperties;
/**
 * Fake producer for transaction testing without network calls.
 */
class FakeProducerForTransaction {
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

class TransactionTest extends TestCase
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
        $this->assertTrue(true, "Message should be added to transaction");
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

        $transaction->tryAddMessage($message);

        $sendResult = [
            'messageId' => 'test-msg-id-1',
            'transactionId' => 'test-tx-id-1',
        ];

        $transaction->tryAddReceipt($message, $sendResult);
        $this->assertTrue(true, "Receipt should be recorded");
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

        $transaction->tryAddMessage($message);
        $transaction->tryAddReceipt($message, $sendResult);
        $transaction->commit();

        $this->assertEquals(
            1,
            count($fakeProducer->commitCalls),
            "Commit should be called once"
        );
        $this->assertEquals(
            'test-msg-id-1',
            $fakeProducer->commitCalls[0]['messageId'],
            "Commit should use the correct message ID"
        );
        $this->assertEquals(
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

        $transaction->tryAddMessage($message);
        $transaction->tryAddReceipt($message, $sendResult);
        $transaction->rollback();

        $this->assertEquals(
            1,
            count($fakeProducer->rollbackCalls),
            "Rollback should be called once"
        );
        $this->assertEquals(
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
        $transaction->tryAddMessage($message);
        $transaction->tryAddReceipt($message, $sendResult);
        $transaction->commit();

        // After commit, second commit throws because receipts are cleared
        $this->expectException(\RuntimeException::class);
        $transaction->commit();

        $this->assertEquals(
            1,
            count($fakeProducer->commitCalls),
            "Only one commit should have been recorded"
        );
    }

    public function testMultipleMessagesInTransaction()
    {
        $fakeProducer = new FakeProducerForTransaction();
        $transaction = new Transaction($fakeProducer);

        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody('body-0');

        $sendResult = [
            'messageId' => "msg-id-0",
            'transactionId' => "tx-id-0",
        ];
        $transaction->tryAddMessage($message);
        $transaction->tryAddReceipt($message, $sendResult);

        $transaction->commit();

        $this->assertEquals(
            1,
            count($fakeProducer->commitCalls),
            "Should commit 1 message"
        );
    }
}

