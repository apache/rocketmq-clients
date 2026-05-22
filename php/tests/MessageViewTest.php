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
require_once __DIR__ . '/../MessageView.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\MessageView;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;

class MessageViewTest
{
    private function createTestMessage()
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $sysProps = new \Apache\Rocketmq\V2\SystemProperties();
        $sysProps->setMessageId('test-msg-id-001');
        $sysProps->setTag('test-tag');
        $sysProps->setKeys(['key1']);
        $sysProps->setMessageGroup('test-group');

        $msg = new Message();
        $msg->setTopic($topic);
        $msg->setBody('test body content');
        $msg->setSystemProperties($sysProps);
        $msg->getUserProperties()['env'] = 'test';

        return $msg;
    }

    public function testGetTopic()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertEquals('test-topic', $view->getTopic(), "Topic should match");
    }

    public function testGetBody()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertEquals('test body content', $view->getBody(), "Body should match");
    }

    public function testGetMessageId()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertEquals('test-msg-id-001', $view->getMessageId(), "Message ID should match");
    }

    public function testGetTag()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertEquals('test-tag', $view->getTag(), "Tag should match");
    }

    public function testGetKeys()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        $keys = $view->getKeys();
        TestRunner::assertEquals(1, count($keys), "Should have 1 key");
        TestRunner::assertEquals('key1', $keys[0], "Key should match");
    }

    public function testGetMessageGroup()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertEquals('test-group', $view->getMessageGroup(), "Message group should match");
    }

    public function testGetReceiptHandle()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg, 'receipt-handle-abc');

        TestRunner::assertEquals('receipt-handle-abc', $view->getReceiptHandle(), "Receipt handle should match");
    }

    public function testGetDeliveryAttempt()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg, null, null, 3);

        TestRunner::assertEquals(3, $view->getDeliveryAttempt(), "Delivery attempt should be 3");
    }

    public function testGetUserProperties()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertEquals('test', $view->getProperty('env'), "User property should match");
        TestRunner::assertNull($view->getProperty('nonexistent'), "Non-existent property should be null");
    }

    public function testIsFifo()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertTrue($view->isFifo(), "Should be FIFO when messageGroup is set");

        // Non-FIFO message
        $topic = new Resource();
        $topic->setName('normal-topic');
        $normalMsg = new Message();
        $normalMsg->setTopic($topic);
        $normalMsg->setBody('normal body');

        $normalView = new MessageView($normalMsg);
        TestRunner::assertFalse($normalView->isFifo(), "Should not be FIFO without messageGroup");
    }

    public function testToString()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        $str = (string)$view;
        TestRunner::assertTrue(
            strpos($str, 'test-topic') !== false,
            "__toString should contain topic"
        );
        TestRunner::assertTrue(
            strpos($str, 'test-msg-id-001') !== false,
            "__toString should contain message ID"
        );
    }

    public function testGetMessage()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertTrue(
            $view->getMessage() === $protoMsg,
            "getMessage should return the original protobuf message"
        );
    }
}

echo "=== MessageViewTest ===\n";
TestRunner::run(new MessageViewTest());
