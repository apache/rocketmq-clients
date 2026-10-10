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

use Apache\Rocketmq\LiteSimpleConsumer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\SyncLiteSubscriptionResponse;
use Apache\Rocketmq\V2\Status;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../LiteSimpleConsumer.php';

class LiteSimpleConsumerIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testConstructorCreatesGprcClient()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic');
        $this->assertNotNull($consumer->getClient());
    }

    public function testConstructorThrowsOnEmptyParentTopic()
    {
        $this->expectException(\InvalidArgumentException::class);
        new LiteSimpleConsumer($this->endpoints, 'test-group', '');
    }

    public function testSubscribeLiteAddsTopic()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic');
        $result = $consumer->subscribeLite('lite-topic-1');
        $this->assertSame($consumer, $result);
        $this->assertContains('lite-topic-1', $consumer->getLiteTopics());
    }

    public function testSubscribeLiteThrowsOnMaxLength()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'maxLiteTopicSize' => 10,
        ]);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('exceeds max length');
        $consumer->subscribeLite(str_repeat('a', 20));
    }

    public function testSubscribeLiteThrowsOnQuotaExceeded()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'liteSubscriptionQuota' => 1,
        ]);
        $consumer->subscribeLite('topic-1');
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('quota exceeded');
        $consumer->subscribeLite('topic-2');
    }

    public function testUnsubscribeLiteRemovesTopic()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic');
        $consumer->subscribeLite('lite-topic-1');
        $consumer->subscribeLite('lite-topic-2');
        $this->assertCount(2, $consumer->getLiteTopics());

        $result = $consumer->unsubscribeLite('lite-topic-1');
        $this->assertSame($consumer, $result);
        $this->assertNotContains('lite-topic-1', $consumer->getLiteTopics());
        $this->assertContains('lite-topic-2', $consumer->getLiteTopics());
    }

    public function testGetLiteTopicsInitiallyEmpty()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic');
        $this->assertIsArray($consumer->getLiteTopics());
        $this->assertEmpty($consumer->getLiteTopics());
    }

    public function testStartThrowsWithoutLiteTopics()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic');
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('no lite topics');
        $consumer->start();
    }

    public function testGetClientId()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'clientId' => 'lite-client-001',
        ]);
        $this->assertEquals('lite-client-001', $consumer->getClientId());
    }

    public function testHandleUnsubscribeLite()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic');
        $consumer->subscribeLite('lite-topic-1');
        $consumer->subscribeLite('lite-topic-2');

        $consumer->handleUnsubscribeLite('lite-topic-1');
        $this->assertNotContains('lite-topic-1', $consumer->getLiteTopics());
    }

    public function testHandleUnsubscribeLiteIgnoresUnknown()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic');
        $consumer->subscribeLite('lite-topic-1');

        $consumer->handleUnsubscribeLite('nonexistent');
        $this->assertContains('lite-topic-1', $consumer->getLiteTopics());
    }

    public function testSyncLiteSubscriptionsWithMock()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        $syncResponse = new SyncLiteSubscriptionResponse();
        $syncStatus = new Status();
        $syncStatus->setCode(20000);
        $syncResponse->setStatus($syncStatus);
        GrpcMockHelper::mockUnaryCall($mock, 'SyncLiteSubscription', $syncResponse, 0);

        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic');
        $consumer->subscribeLite('lite-topic-1');

        // Should not throw
        $consumer->syncLiteSubscriptions();
        $this->assertTrue(true);
    }

    public function testIsRunningInitiallyFalse()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic');
        $this->assertFalse($consumer->isRunning());
    }

    public function testShutdownBeforeStartIsSafe()
    {
        $consumer = new LiteSimpleConsumer($this->endpoints, 'test-group', 'parent-topic');
        $consumer->shutdown();
        $this->assertTrue(true);
    }

    /**
     * End-to-end check against a real RocketMQ 5.x cluster with LMQ enabled and a
     * CLUSTER-mode proxy. Skipped unless ROCKETMQ_PHP_LITE_ENDPOINTS is set.
     *
     * Prerequisites on the broker:
     *   - broker.conf: enableLmq=true, enableMultiDispatch=true
     *   - proxy running in CLUSTER mode
     *   - parent topic created as message.type=LITE
     *   - consumer group pre-created with +lite.bind.topic=<parentTopic>
     *
     * Flow mirrors the Rust/C# ports: produce 5 lite messages first, then receive,
     * assert lite_topic on every message, ack them all, and shut down.
     */
    public function testLiteReceiveAndAck()
    {
        $endpoints = getenv('ROCKETMQ_PHP_LITE_ENDPOINTS');
        if ($endpoints === false || $endpoints === '') {
            $this->markTestSkipped('ROCKETMQ_PHP_LITE_ENDPOINTS not set; skipping real-cluster test');
        }

        $parentTopic = getenv('ROCKETMQ_PHP_LITE_PARENT_TOPIC') ?: 'lite_parent_topic';
        $liteTopic = getenv('ROCKETMQ_PHP_LITE_TOPIC') ?: 'lite_topic';
        $consumerGroup = getenv('ROCKETMQ_PHP_LITE_GROUP') ?: 'php_lite_simple_group';

        // 1. Produce lite messages BEFORE starting the consumer so the receive
        //    loop finds data immediately instead of long-polling empty queues.
        $producer = new \Apache\Rocketmq\Producer($endpoints, ['sslEnabled' => false]);
        $producer->start();
        $sent = 0;
        for ($i = 0; $i < 5; $i++) {
            $message = (new \Apache\Rocketmq\MessageBuilder())
                ->setTopic($parentTopic)
                ->setBody("lite-e2e-{$i}")
                ->setLiteTopic($liteTopic)
                ->build();
            $receipts = $producer->send($message);
            $this->assertNotEmpty($receipts, "send #{$i} must return a receipt");
            $sent++;
        }
        $this->assertSame(5, $sent);

        // 2. Start the lite simple consumer and pull the messages back.
        $consumer = (new \Apache\Rocketmq\LiteSimpleConsumerBuilder())
            ->setEndpoints($endpoints)
            ->setConsumerGroup($consumerGroup)
            ->bindTopic($parentTopic)
            ->subscriptionLite($liteTopic)
            ->setSslEnabled(false)
            ->setAwaitDuration(2)
            ->build();

        try {
            $received = [];
            for ($cycle = 0; $cycle < 10 && count($received) < 5; $cycle++) {
                foreach ($consumer->receive(32, 30) as $message) {
                    $this->assertNotEmpty($message->getTopic());
                    // The message must carry the logical lite topic in its system properties.
                    $this->assertSame($liteTopic, $message->getLiteTopic());
                    $received[] = $message;
                    if (count($received) >= 5) {
                        break;
                    }
                }
            }
            $this->assertSame(5, count($received), 'must receive the 5 produced lite messages');

            // 3. ack() addresses the parent topic and forwards lite_topic automatically
            //    so the proxy can resolve the LMQ receipt handles (no 50001).
            $consumer->ack($received);
            $this->assertTrue(true);
        } finally {
            $consumer->shutdown();
            $producer->shutdown();
        }
    }
}
