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

use FakeConsumer;
use PHPUnit\Framework\TestCase;
require_once __DIR__ . '/../autoload.php';

require_once __DIR__ . '/../ConsumeResult.php';
require_once __DIR__ . '/../ConsumeService.php';
require_once __DIR__ . '/../Logger.php';
require_once __DIR__ . '/helpers/FakeConsumer.php';

use Apache\Rocketmq\ConsumeResult;
use Apache\Rocketmq\StandardConsumeService;
use Apache\Rocketmq\MessageViewInterface;

/**
 * Fake message view for testing.
 */
class FakeMessageView implements MessageViewInterface {
    private $systemProperties;
    private $body;
    private $topic;

    public function __construct($body = 'test body', $topic = 'test-topic', $receiptHandle = null, $messageId = null)
    {
        $this->body = $body;
        $this->systemProperties = new FakeSystemProps($receiptHandle, $messageId);
        $this->topic = $topic;
    }

    public function getSystemProperties(): ?object { return $this->systemProperties; }
    public function getBody() { return $this->body; }
    public function getTopic(): string { return $this->topic; }
    public function getMessageId(): string { return $this->systemProperties?->getMessageId() ?? ''; }
    public function getDeliveryAttempt(): int { return 1; }
    public function incrementDeliveryAttempt(): void {}
    public function isCorrupted(): bool { return false; }
    public function getEndpoints(): ?object { return null; }
}

class FakeSystemProps {
    private $receiptHandle;
    private $messageId;

    public function __construct($receiptHandle = null, $messageId = null)
    {
        $this->receiptHandle = $receiptHandle;
        $this->messageId = $messageId;
    }

    public function getReceiptHandle() { return $this->receiptHandle; }
    public function getMessageId() { return $this->messageId; }
    public function hasReceiptHandle() { return $this->receiptHandle !== null; }
    public function hasMessageId() { return $this->messageId !== null; }
}

class FakeTopic {
    private $name;
    public function __construct($name) { $this->name = $name; }
    public function getName() { return $this->name; }
    public function hasName() { return !empty($this->name); }
}

/**
 * Fake consumer for testing ConsumeService.
 */
class FakeConsumerForConsume extends FakeConsumer{
    private ?\Apache\Rocketmq\V2\MessagingServiceClient $client = null;

    public function __construct(string $clientId = 'test-consumer')
    {
        parent::__construct('test-client-id');
    }

    public function getGroupResource()
    {
        $resource = new \Apache\Rocketmq\V2\Resource();
        $resource->setName('test-group');
        return $resource;
    }

    public function getClient(): ?\Apache\Rocketmq\V2\MessagingServiceClient { return $this->client; }
    public function setClient($client) { $this->client = $client; }
}

class StandardConsumeServiceTest extends TestCase
{
    public function setUp(): void
    {
        \Apache\Rocketmq\Logger::close();
    }

    /**
     * Test consumeMessage dispatch logic via reflection.
     * This mirrors Java's StandardConsumeServiceTest.testDispatch() which is empty
     * because the real logic requires full PushConsumer mock setup.
     */
    public function testConsumeMessageReturnsSuccess()
    {
        $fakeConsumer = new FakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('StdConsumeDispatch');

        $listener = function($msg) {
            return ConsumeResult::SUCCESS;
        };

        $service = new StandardConsumeService($logger, $listener, $fakeConsumer);
        $msg = new FakeMessageView('test', 'topic');

        $method = new \ReflectionMethod($service, 'consumeMessage');
        $method->setAccessible(true);
        $result = $method->invoke($service, $msg);

        $this->assertEquals(
            ConsumeResult::SUCCESS,
            $result,
            "consumeMessage should return SUCCESS when listener returns SUCCESS"
        );
    }

    public function testConsumeMessageReturnsFailure()
    {
        $fakeConsumer = new FakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('StdConsumeFail');

        $listener = function($msg) {
            return ConsumeResult::FAILURE;
        };

        $service = new StandardConsumeService($logger, $listener, $fakeConsumer);
        $msg = new FakeMessageView('test', 'topic');

        $method = new \ReflectionMethod($service, 'consumeMessage');
        $method->setAccessible(true);
        $result = $method->invoke($service, $msg);

        $this->assertEquals(
            ConsumeResult::FAILURE,
            $result,
            "consumeMessage should return FAILURE when listener returns FAILURE"
        );
    }

    public function testConsumeMessageCatchesException()
    {
        $fakeConsumer = new FakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('StdConsumeException');

        $listener = function($msg) {
            throw new \RuntimeException("Test exception");
        };

        $service = new StandardConsumeService($logger, $listener, $fakeConsumer);
        $msg = new FakeMessageView('test', 'topic');

        $method = new \ReflectionMethod($service, 'consumeMessage');
        $method->setAccessible(true);
        $result = $method->invoke($service, $msg);

        $this->assertEquals(
            ConsumeResult::FAILURE,
            $result,
            "consumeMessage should return FAILURE when listener throws exception"
        );
    }

    public function testExtractReceiptHandle()
    {
        $fakeConsumer = new FakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('StdConsumeExtract');

        $listener = function($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $fakeConsumer);

        $msgWithHandle = new FakeMessageView('body', 'topic', 'receipt-handle-123');

        $method = new \ReflectionMethod($service, 'extractReceiptHandle');
        $method->setAccessible(true);
        $handle = $method->invoke($service, $msgWithHandle);

        $this->assertEquals(
            'receipt-handle-123',
            $handle,
            "Should extract receipt handle from message"
        );
    }

    public function testExtractMessageId()
    {
        $fakeConsumer = new FakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('StdConsumeExtractId');

        $listener = function($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $fakeConsumer);

        $msgWithId = new FakeMessageView('body', 'topic', null, 'msg-id-456');

        $method = new \ReflectionMethod($service, 'extractMessageId');
        $method->setAccessible(true);
        $id = $method->invoke($service, $msgWithId);

        $this->assertEquals('msg-id-456', $id, "Should extract message ID");
    }

    public function testExtractTopic()
    {
        $fakeConsumer = new FakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('StdConsumeExtractTopic');

        $listener = function($msg) { return ConsumeResult::SUCCESS; };
        $service = new StandardConsumeService($logger, $listener, $fakeConsumer);

        $msg = new FakeMessageView('body', 'my-test-topic');

        $method = new \ReflectionMethod($service, 'extractTopic');
        $method->setAccessible(true);
        $topic = $method->invoke($service, $msg);

        $this->assertEquals('my-test-topic', $topic, "Should extract topic name");
    }
}

