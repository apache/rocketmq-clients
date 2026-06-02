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
use Apache\Rocketmq\FifoConsumeService;

/**
 * Fake message view for FifoConsumeService testing.
 */
class FifoFakeMessageView
{
    private $systemProperties;

    public function __construct(
        private string $body,
        private string $topic,
        private ?string $receiptHandle = null
    ) {
        $this->systemProperties = new FifoFakeSystemProps($receiptHandle);
    }

    public function getBody(): string
    {
        return $this->body;
    }

    public function getTopic(): string
    {
        return $this->topic;
    }

    public function getReceiptHandle(): ?string
    {
        return $this->receiptHandle;
    }

    public function getMessageGroup(): ?string
    {
        return null;
    }

    public function getSystemProperties(): FifoFakeSystemProps
    {
        return $this->systemProperties;
    }
}

class FifoFakeSystemProps
{
    private ?string $receiptHandle;

    public function __construct(?string $receiptHandle = null)
    {
        $this->receiptHandle = $receiptHandle;
    }

    public function getReceiptHandle(): ?string
    {
        return $this->receiptHandle;
    }

    public function hasReceiptHandle(): bool
    {
        return $this->receiptHandle !== null;
    }

    public function hasMessageGroup(): bool
    {
        return false;
    }
}

class FifoFakeConsumerForConsume extends FakeConsumer
{
    public function __construct()
    {
        parent::__construct('test-fifo-consumer');
    }
}

class FifoConsumeServiceTest extends TestCase
{
    public function setUp(): void
    {
        \Apache\Rocketmq\Logger::close();
    }

    public function testConsumeMessageSingleMessage()
    {
        $fakeConsumer = new FifoFakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('FifoConsumeSingle');

        $listener = function($msg) { return ConsumeResult::SUCCESS; };
        $service = new FifoConsumeService($logger, $listener, $fakeConsumer, false);

        $method = new \ReflectionMethod($service, 'consumeMessage');
        $method->setAccessible(true);
        $result = $method->invoke($service, new FifoFakeMessageView('msg', 'topic'));

        $this->assertEquals(ConsumeResult::SUCCESS, $result, "consumeMessage should return SUCCESS");
    }

    public function testDefaultGroupKeyForNonGroupedMessages()
    {
        $fakeConsumer = new FifoFakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('FifoConsumeDefaultKey');

        $service = new FifoConsumeService($logger, function($msg) { return ConsumeResult::SUCCESS; }, $fakeConsumer, false);
        $msg = new FifoFakeMessageView('msg', 'topic');

        $method = new \ReflectionMethod($service, 'getMessageGroupKey');
        $method->setAccessible(true);
        $groupKey = $method->invoke($service, $msg);

        $this->assertEquals('default', $groupKey, "Non-grouped message should have 'default' group key");
    }

    public function testExtractReceiptHandle()
    {
        $fakeConsumer = new FifoFakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('FifoConsumeExtract');

        $listener = function($msg) { return ConsumeResult::SUCCESS; };
        $service = new FifoConsumeService($logger, $listener, $fakeConsumer, false);

        $msgWithHandle = new FifoFakeMessageView('body', 'topic', 'receipt-handle-789');

        $method = new \ReflectionMethod($service, 'extractReceiptHandle');
        $method->setAccessible(true);
        $handle = $method->invoke($service, $msgWithHandle);

        $this->assertEquals('receipt-handle-789', $handle, "Should extract receipt handle");
    }
}
