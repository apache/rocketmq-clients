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

use Apache\Rocketmq\Transaction;
use Apache\Rocketmq\TransactionCommitter;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../../Transaction.php';
require_once __DIR__ . '/../../TransactionCommitter.php';

class TransactionIntegrationTest extends IntegrationTestCase
{
    /**
     * Create a mock producer with commitTransaction and rollbackTransaction stubs.
     */
    private function createMockProducer(): TransactionCommitter
    {
        return new class implements TransactionCommitter {
            public function commitTransaction(
                string $messageId,
                string $transactionId,
                string $topic,
                ?Endpoints $endpoints = null
            ): void {
                // no-op
            }

            public function rollbackTransaction(
                string $messageId,
                string $transactionId,
                string $topic,
                ?Endpoints $endpoints = null
            ): void {
                // no-op
            }
        };
    }

    public function testTryAddMessage()
    {
        $tx = new Transaction($this->createMockProducer());

        $msg = new Message();
        $msg->setBody('TX message');

        $tx->tryAddMessage($msg);
        $this->assertTrue(true);
    }

    public function testTryAddReceiptAddsReceipt()
    {
        $tx = new Transaction($this->createMockProducer());

        $msg = new Message();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $msg->setTopic($topicResource);
        $msg->setBody('TX message');
        $tx->tryAddMessage($msg);

        $tx->tryAddReceipt($msg, [
            'messageId' => 'msg-001',
            'transactionId' => 'tx-001',
        ]);

        $receipts = $tx->getReceipts();
        $this->assertCount(1, $receipts);
        $this->assertEquals('msg-001', $receipts[0]['messageId']);
        $this->assertEquals('tx-001', $receipts[0]['transactionId']);
    }

    public function testCommitFailsWithoutReceipt()
    {
        $tx = new Transaction($this->createMockProducer());

        $msg = new Message();
        $msg->setBody('TX message');
        $tx->tryAddMessage($msg);

        $this->expectException(\RuntimeException::class);
        $tx->commit();
    }

    public function testRollbackFailsWithoutReceipt()
    {
        $tx = new Transaction($this->createMockProducer());

        $msg = new Message();
        $msg->setBody('TX message');
        $tx->tryAddMessage($msg);

        $this->expectException(\RuntimeException::class);
        $tx->rollback();
    }

    public function testTryAddMessageAfterCommitThrows()
    {
        $tx = new Transaction($this->createMockProducer());

        $msg1 = new Message();
        $topicResource = new Resource();
        $topicResource->setName('test-topic');
        $msg1->setTopic($topicResource);
        $msg1->setBody('msg1');
        $tx->tryAddMessage($msg1);
        $tx->tryAddReceipt($msg1, [
            'messageId' => 'msg-001',
            'transactionId' => 'tx-001',
        ]);
        $tx->commit();

        $this->expectException(\RuntimeException::class);
        $msg2 = new Message();
        $msg2->setBody('msg2');
        $tx->tryAddMessage($msg2);
    }

    public function testTryAddSecondMessageThrows()
    {
        $tx = new Transaction($this->createMockProducer());

        $msg1 = new Message();
        $msg1->setBody('msg1');
        $tx->tryAddMessage($msg1);

        $this->expectException(\InvalidArgumentException::class);
        $msg2 = new Message();
        $msg2->setBody('msg2');
        $tx->tryAddMessage($msg2);
    }

    public function testIsCommittedAndIsRolledBack()
    {
        $tx = new Transaction($this->createMockProducer());
        $this->assertFalse($tx->isCommitted());
        $this->assertFalse($tx->isRolledBack());
    }
}
