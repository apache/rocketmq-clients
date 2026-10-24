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

use Apache\Rocketmq\LitePushConsumer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\HeartbeatResponse;
use Apache\Rocketmq\V2\NotifyClientTerminationResponse;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\SyncLiteSubscriptionResponse;
use Apache\Rocketmq\V2\QueryAssignmentResponse;
use Apache\Rocketmq\V2\Status;
use Apache\Rocketmq\V2\Assignment;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Permission;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../LitePushConsumer.php';

class LitePushConsumerIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testConstructorCreatesGprcClient()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $this->assertNotNull($consumer->getClient());
    }

    public function testConstructorThrowsOnEmptyParentTopic()
    {
        $this->expectException(\InvalidArgumentException::class);
        new LitePushConsumer($this->endpoints, 'test-group', '', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
    }

    public function testSubscribeLiteAddsTopic()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $result = $consumer->subscribeLite('lite-topic-1');
        $this->assertSame($consumer, $result);
        $this->assertContains('lite-topic-1', $consumer->getLiteTopics());
    }

    public function testSubscribeLiteThrowsOnMaxLength()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'maxLiteTopicSize' => 10,
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('exceeds max length');
        $consumer->subscribeLite(str_repeat('a', 20));
    }

    public function testSubscribeLiteThrowsOnQuotaExceeded()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'liteSubscriptionQuota' => 1,
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $consumer->subscribeLite('topic-1');
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('quota exceeded');
        $consumer->subscribeLite('topic-2');
    }

    public function testUnsubscribeLiteRemovesTopic()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
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
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $this->assertIsArray($consumer->getLiteTopics());
        $this->assertEmpty($consumer->getLiteTopics());
    }

    public function testSetLiteMessageListener()
    {
        $listener = function ($msg) {
            return \Apache\Rocketmq\ConsumeResult::SUCCESS;
        };
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => $listener,
        ]);
        $newListener = function ($msg) {
            return \Apache\Rocketmq\ConsumeResult::FAILURE;
        };
        $consumer->setLiteMessageListener($newListener);
        // setLiteMessageListener returns $this for chaining
        $this->assertTrue(true);
    }

    public function testStartThrowsWithoutLiteTopics()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('no lite topics');
        $consumer->start();
    }

    public function testStartThrowsWithoutMessageListener()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            // no messageListener
        ]);
        $consumer->subscribeLite('topic-1');
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('no lite message listener');
        $consumer->start();
    }

    public function testIsLiteConsumerReturnsTrue()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $this->assertTrue($consumer->isLiteConsumer());
    }

    public function testGetClientId()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'clientId' => 'lite-client-001',
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $this->assertEquals('lite-client-001', $consumer->getClientId());
    }

    public function testHandleUnsubscribeLite()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $consumer->subscribeLite('lite-topic-1');
        $consumer->subscribeLite('lite-topic-2');

        $consumer->handleUnsubscribeLite('lite-topic-1');
        $this->assertNotContains('lite-topic-1', $consumer->getLiteTopics());
    }

    public function testHandleUnsubscribeLiteIgnoresUnknown()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $consumer->subscribeLite('lite-topic-1');

        // Should not throw for unknown topic
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

        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $consumer->subscribeLite('lite-topic-1');

        // Should not throw
        $consumer->syncLiteSubscriptions();
        $this->assertTrue(true);
    }

    public function testIsRunningInitiallyFalse()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $this->assertFalse($consumer->isRunning());
    }

    public function testShutdownBeforeStartIsSafe()
    {
        $consumer = new LitePushConsumer($this->endpoints, 'test-group', 'parent-topic', [
            'messageListener' => function ($msg) {
                return \Apache\Rocketmq\ConsumeResult::SUCCESS;
            },
        ]);
        $consumer->shutdown();
        $this->assertTrue(true);
    }
}
