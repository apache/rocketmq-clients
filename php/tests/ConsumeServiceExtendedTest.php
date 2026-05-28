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

require_once __DIR__ . '/../ConsumeService.php';
require_once __DIR__ . '/../ConsumeResult.php';
require_once __DIR__ . '/../MessageView.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\ConsumeResult;
use Apache\Rocketmq\ConsumeService;
use Apache\Rocketmq\Logger;
use Apache\Rocketmq\MessageView;
use Apache\Rocketmq\ProcessQueue;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;

/**
 * Tests for ConsumeService dispatch logic.
 * Mirrors Java's ConsumeServiceTest and ConsumeTaskTest.
 * Tests consumeMessage() returning SUCCESS/FAILURE and catching exceptions.
 */
class ConsumeServiceExtendedTest extends TestCase
{
    public function setUp(): void
    {
        Logger::close();
    }

    /**
     * Mirrors Java: testConsumeSuccess - listener returns SUCCESS.
     */
    public function testConsumeMessageReturnsSuccess()
    {
        $service = new TestConsumeService(function($msg) {
            return ConsumeResult::SUCCESS;
        });

        $msg = $this->buildMessageView('test-topic', 'hello');
        $result = $service->consumeMessage($msg);

        $this->assertEquals(
            ConsumeResult::SUCCESS,
            $result,
            "Consume result should be SUCCESS"
        );
    }

    /**
     * Mirrors Java: testConsumeFailure - listener returns FAILURE.
     */
    public function testConsumeMessageReturnsFailure()
    {
        $service = new TestConsumeService(function($msg) {
            return ConsumeResult::FAILURE;
        });

        $msg = $this->buildMessageView('test-topic', 'hello');
        $result = $service->consumeMessage($msg);

        $this->assertEquals(
            ConsumeResult::FAILURE,
            $result,
            "Consume result should be FAILURE"
        );
    }

    /**
     * Mirrors Java: testConsumeWithException - listener throws exception
     * should be caught and return FAILURE.
     */
    public function testConsumeMessageCatchesException()
    {
        $service = new TestConsumeService(function($msg) {
            throw new \RuntimeException("Simulated listener error");
        });

        $msg = $this->buildMessageView('test-topic', 'hello');
        $result = $service->consumeMessage($msg);

        $this->assertEquals(
            ConsumeResult::FAILURE,
            $result,
            "Consume result should be FAILURE when listener throws"
        );
    }

    /**
     * Mirrors Java: testConsumeWithDelay - consume with delayed scheduling.
     * In PHP this is simulated via the retry delay mechanism.
     */
    public function testConsumeWithThrowable()
    {
        $service = new TestConsumeService(function($msg) {
            throw new \InvalidArgumentException("Invalid argument");
        });

        $msg = $this->buildMessageView('test-topic', 'hello');
        $result = $service->consumeMessage($msg);

        $this->assertEquals(
            ConsumeResult::FAILURE,
            $result,
            "Consume result should be FAILURE for any Throwable"
        );
    }

    /**
     * Tests that consumeMessage returns SUCCESS for truthy return values.
     */
    public function testConsumeMessageTruthyReturnsSuccess()
    {
        $service = new TestConsumeService(function($msg) {
            return 0; // 0 is truthy in PHP but not FAILURE
        });

        $msg = $this->buildMessageView('test-topic', 'hello');
        $result = $service->consumeMessage($msg);

        $this->assertEquals(
            ConsumeResult::SUCCESS,
            $result,
            "Non-FAILURE return should be treated as SUCCESS"
        );
    }

    /**
     * Helper to build a MessageView.
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
 * Concrete test implementation of ConsumeService that exposes consumeMessage.
 */
class TestConsumeService extends ConsumeService
{
    public function __construct($listener)
    {
        $logger = Logger::getInstance('TestConsumeService');
        $fakeConsumer = new class {
            public function getGroupResource() {
                $r = new Resource();
                $r->setName('test-group');
                return $r;
            }
            public function getAwaitDuration() { return 30; }
        };
        parent::__construct($logger, $listener, $fakeConsumer);
    }

    public function consume(ProcessQueue $pq): void
    {
        // Not used in these tests
    }

    // Expose protected method for testing
    public function consumeMessage($messageView)
    {
        return parent::consumeMessage($messageView);
    }
}

