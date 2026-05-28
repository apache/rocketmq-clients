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

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Logger.php';
require_once __DIR__ . '/../ConsumeResult.php';
require_once __DIR__ . '/../MessageView.php';
require_once __DIR__ . '/../ProcessQueue.php';
require_once __DIR__ . '/../ConsumeService.php';

use Apache\Rocketmq\ConsumeResult;
use Apache\Rocketmq\StandardConsumeService;
use Apache\Rocketmq\FifoConsumeService;
use Apache\Rocketmq\Logger;
use Apache\Rocketmq\MessageView;
use Apache\Rocketmq\ProcessQueue;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;

/**
 * Tests for ConsumeTask-like dispatch logic.
 * Mirrors Java's ConsumeTaskTest which tests that listener exceptions
 * are caught and converted to ConsumeResult.FAILURE.
 */
class ConsumeTaskTest extends TestCase
{
    public function setUp(): void
    {
        Logger::close();
    }

    /**
     * Mirrors Java: testCallWithConsumeSuccess
     * Tests that a successful listener call returns SUCCESS.
     */
    public function testCallWithConsumeSuccess()
    {
        $consumeCount = 0;
        $service = new TestableStandardConsumeService(function($msg) use (&$consumeCount) {
            $consumeCount++;
            return ConsumeResult::SUCCESS;
        });

        $msg = $this->buildMessageView('test-topic', 'success-body');
        $result = $service->consumeMessage($msg);

        $this->assertEquals(
            ConsumeResult::SUCCESS,
            $result,
            "Successful consume should return SUCCESS"
        );
        $this->assertEquals(
            1,
            $consumeCount,
            "Listener should be called exactly once"
        );
    }

    /**
     * Mirrors Java: testCallWithConsumeException
     * Tests that a listener throwing RuntimeException returns FAILURE.
     */
    public function testCallWithConsumeException()
    {
        $service = new TestableStandardConsumeService(function($msg) {
            throw new \RuntimeException("Simulated consume failure");
        });

        $msg = $this->buildMessageView('test-topic', 'error-body');
        $result = $service->consumeMessage($msg);

        $this->assertEquals(
            ConsumeResult::FAILURE,
            $result,
            "Listener exception should return FAILURE"
        );
    }

    /**
     * Tests FIFO consume success path.
     */
    public function testFifoConsumeSuccess()
    {
        $consumeCount = 0;
        $service = new TestableFifoConsumeService(function($msg) use (&$consumeCount) {
            $consumeCount++;
            return ConsumeResult::SUCCESS;
        });

        $msg = $this->buildMessageView('fifo-topic', 'fifo-body');
        $result = $service->consumeMessage($msg);

        $this->assertEquals(
            ConsumeResult::SUCCESS,
            $result,
            "FIFO consume success should return SUCCESS"
        );
        $this->assertEquals(
            1,
            $consumeCount,
            "FIFO listener should be called once"
        );
    }

    /**
     * Tests FIFO consume exception path.
     */
    public function testFifoConsumeException()
    {
        $service = new TestableFifoConsumeService(function($msg) {
            throw new \RuntimeException("FIFO consume failure");
        });

        $msg = $this->buildMessageView('fifo-topic', 'fifo-error-body');
        $result = $service->consumeMessage($msg);

        $this->assertEquals(
            ConsumeResult::FAILURE,
            $result,
            "FIFO listener exception should return FAILURE"
        );
    }

    /**
     * Tests that listener receives the correct message.
     */
    public function testListenerReceivesCorrectMessage()
    {
        $receivedBody = null;
        $receivedTopic = null;
        $service = new TestableStandardConsumeService(function($msg) use (&$receivedBody, &$receivedTopic) {
            $receivedBody = $msg->getBody();
            $receivedTopic = $msg->getTopic();
            return ConsumeResult::SUCCESS;
        });

        $msg = $this->buildMessageView('verify-topic', 'verify-body-content');
        $service->consumeMessage($msg);

        $this->assertEquals(
            'verify-body-content',
            $receivedBody,
            "Listener should receive correct message body"
        );
        $this->assertEquals(
            'verify-topic',
            $receivedTopic,
            "Listener should receive correct topic"
        );
    }

    /**
     * Tests that Error (not Exception) is also caught as FAILURE.
     */
    public function testListenerErrorReturnsFailure()
    {
        $service = new TestableStandardConsumeService(function($msg) {
            throw new \Error("Fatal listener error");
        });

        $msg = $this->buildMessageView('test-topic', 'error-body');
        $result = $service->consumeMessage($msg);

        $this->assertEquals(
            ConsumeResult::FAILURE,
            $result,
            "Listener Error should also return FAILURE"
        );
    }

    /**
     * Helper to build a MessageView with a specific topic and body.
     */
    private function buildMessageView($topic, $body)
    {
        $message = new Message();
        $message->setBody($body);
        $topicResource = new Resource();
        $topicResource->setName($topic);
        $message->setTopic($topicResource);

        $sysProps = new SystemProperties();
        $sysProps->setMessageId('test-msg-id');
        $message->setSystemProperties($sysProps);

        return new MessageView($message, 'test-receipt-handle', null, 1);
    }
}

/**
 * Testable StandardConsumeService that exposes consumeMessage.
 */
class TestableStandardConsumeService extends StandardConsumeService
{
    public function __construct($listener)
    {
        $logger = Logger::getInstance('TestableStandardConsumeService');
        $fakeConsumer = $this->createFakeConsumer();
        parent::__construct($logger, $listener, $fakeConsumer);
    }

    public function consume(ProcessQueue $pq): void
    {
        // Not used in these tests
    }

    public function consumeMessage($messageView)
    {
        return parent::consumeMessage($messageView);
    }

    private function createFakeConsumer()
    {
        return new class {
            public function getGroupResource() {
                $r = new Resource();
                $r->setName('test-group');
                return $r;
            }
            public function getAwaitDuration() { return 30; }
            public function getClient() { return null; }
        };
    }
}

/**
 * Testable FifoConsumeService that exposes consumeMessage.
 */
class TestableFifoConsumeService extends FifoConsumeService
{
    public function __construct($listener)
    {
        $logger = Logger::getInstance('TestableFifoConsumeService');
        $fakeConsumer = $this->createFakeConsumer();
        parent::__construct($logger, $listener, $fakeConsumer, false);
    }

    public function consume(ProcessQueue $pq): void
    {
        // Not used in these tests
    }

    public function consumeMessage($messageView)
    {
        return parent::consumeMessage($messageView);
    }

    private function createFakeConsumer()
    {
        return new class {
            public function getGroupResource() {
                $r = new Resource();
                $r->setName('test-group');
                return $r;
            }
            public function getAwaitDuration() { return 30; }
            public function getClient() { return null; }
        };
    }
}

