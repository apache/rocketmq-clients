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
require_once __DIR__ . '/../ConsumeResult.php';
require_once __DIR__ . '/../ConsumeService.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\ConsumeResult;
use Apache\Rocketmq\FifoConsumeService;

class FifoConsumeServiceTest
{
    public function testConsumeMessageSingleMessage()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('FifoConsumeSingle');

        $listener = function($msg) { return ConsumeResult::SUCCESS; };
        $service = new FifoConsumeService($logger, $listener, $fakeConsumer, false);

        $method = new \ReflectionMethod($service, 'consumeMessage');
        $method->setAccessible(true);
        $result = $method->invoke($service, new FakeMessageView('msg', 'topic'));

        TestRunner::assertEqualsWithMessage(ConsumeResult::SUCCESS, $result, "consumeMessage should return SUCCESS");
    }

    public function testDefaultGroupKeyForNonGroupedMessages()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('FifoConsumeDefaultKey');

        $service = new FifoConsumeService($logger, function($msg) { return ConsumeResult::SUCCESS; }, $fakeConsumer, false);
        $msg = new FakeMessageView('msg', 'topic');

        $method = new \ReflectionMethod($service, 'getMessageGroupKey');
        $method->setAccessible(true);
        $groupKey = $method->invoke($service, $msg);

        TestRunner::assertEqualsWithMessage('default', $groupKey, "Non-grouped message should have 'default' group key");
    }

    public function testExtractReceiptHandle()
    {
        \Apache\Rocketmq\Logger::close();
        $fakeConsumer = new FakeConsumerForConsume();
        $logger = \Apache\Rocketmq\Logger::getInstance('FifoConsumeExtract');

        $listener = function($msg) { return ConsumeResult::SUCCESS; };
        $service = new FifoConsumeService($logger, $listener, $fakeConsumer, false);

        $msgWithHandle = new FakeMessageView('body', 'topic', 'receipt-handle-789');

        $method = new \ReflectionMethod($service, 'extractReceiptHandle');
        $method->setAccessible(true);
        $handle = $method->invoke($service, $msgWithHandle);

        TestRunner::assertEqualsWithMessage('receipt-handle-789', $handle, "Should extract receipt handle");
    }
}

echo "=== FifoConsumeServiceTest ===\n";
$test = new FifoConsumeServiceTest();
$test->testConsumeMessageSingleMessage();
echo "  [OK] testConsumeMessageSingleMessage\n";
$test->testDefaultGroupKeyForNonGroupedMessages();
echo "  [OK] testDefaultGroupKeyForNonGroupedMessages\n";
$test->testExtractReceiptHandle();
echo "  [OK] testExtractReceiptHandle\n";
