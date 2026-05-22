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
require_once __DIR__ . '/../Logger.php';
require_once __DIR__ . '/../MessageView.php';

use Apache\Rocketmq\MessageView;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;

/**
 * Tests for MessageView accessors and behavior.
 */
class MessageViewExtendedTest
{
    public function testBornTimestamp()
    {
        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);

        TestRunner::assertTrue(
            $view->getBornTimestamp() >= 0,
            "Born timestamp should be non-negative"
        );
    }

    public function testBornHost()
    {
        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);

        TestRunner::assertTrue(
            is_string($view->getBornHost()),
            "Born host should be a string"
        );
    }

    public function testDecodeTimestamp()
    {
        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);
        $beforeDecode = $view->getDecodeTimestamp();

        TestRunner::assertTrue(
            $beforeDecode > 0,
            "Decode timestamp should be positive"
        );
    }

    public function testIncrementDeliveryAttempt()
    {
        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);

        TestRunner::assertEquals(
            1,
            $view->getDeliveryAttempt(),
            "Initial delivery attempt should be 1"
        );

        $view->incrementDeliveryAttempt();
        TestRunner::assertEquals(
            2,
            $view->getDeliveryAttempt(),
            "After increment, delivery attempt should be 2"
        );

        $view->incrementDeliveryAttempt();
        TestRunner::assertEquals(
            3,
            $view->getDeliveryAttempt(),
            "After second increment, delivery attempt should be 3"
        );
    }

    public function testDeliveryAttemptMinimum()
    {
        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 0);

        TestRunner::assertTrue(
            $view->getDeliveryAttempt() >= 1,
            "Delivery attempt should be at least 1 even if passed 0"
        );
    }

    public function testGetProperty()
    {
        $message = new Message();
        $message->setBody('hello');
        $topic = new Resource();
        $topic->setName('test-topic');
        $message->setTopic($topic);
        $message->getUserProperties()['env'] = 'production';

        $view = new MessageView($message);

        TestRunner::assertEquals(
            'production',
            $view->getProperty('env'),
            "getProperty should return the correct value"
        );
        TestRunner::assertNull(
            $view->getProperty('nonexistent'),
            "getProperty should return null for missing key"
        );
    }

    public function testGetPropertiesReturnsNativeArray()
    {
        $message = new Message();
        $message->setBody('hello');
        $topic = new Resource();
        $topic->setName('test-topic');
        $message->setTopic($topic);
        $map = $message->getUserProperties();
        $map['key1'] = 'val1';
        $map['key2'] = 'val2';

        $view = new MessageView($message);
        $props = $view->getProperties();

        TestRunner::assertTrue(
            is_array($props),
            "getProperties should return a native PHP array"
        );
        TestRunner::assertEquals(
            'val1',
            $props['key1'],
            "Property key1 should be accessible"
        );
    }

    public function testGetKeysReturnsNativeArray()
    {
        $message = $this->buildMessage();
        $sysProps = new SystemProperties();
        $sysProps->setKeys(['order-123', 'user-456']);
        $message->setSystemProperties($sysProps);

        $view = new MessageView($message);
        $keys = $view->getKeys();

        TestRunner::assertTrue(
            is_array($keys),
            "getKeys should return a native PHP array"
        );
        TestRunner::assertEquals(
            2,
            count($keys),
            "Should have 2 keys"
        );
    }

    public function testToStringIncludesTopic()
    {
        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);
        $str = (string)$view;

        TestRunner::assertTrue(
            strpos($str, 'test-topic') !== false,
            "toString should include topic name"
        );
    }

    public function testToStringIncludesMessageId()
    {
        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);
        $str = (string)$view;

        TestRunner::assertTrue(
            strpos($str, 'msg-abc-123') !== false,
            "toString should include message ID"
        );
    }

    public function testToStringIncludesCorrupted()
    {
        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);

        // Use reflection to set corrupted flag
        $ref = new \ReflectionProperty($view, 'corrupted');
        $ref->setAccessible(true);
        $ref->setValue($view, true);

        $str = (string)$view;

        TestRunner::assertTrue(
            strpos($str, 'CORRUPTED') !== false,
            "toString should include CORRUPTED marker"
        );
    }

    /**
     * Helper to build a base Message.
     */
    private function buildMessage()
    {
        $message = new Message();
        $message->setBody('hello world');
        $topic = new Resource();
        $topic->setName('test-topic');
        $message->setTopic($topic);

        $sysProps = new SystemProperties();
        $sysProps->setMessageId('msg-abc-123');
        $sysProps->setTag('test-tag');
        $message->setSystemProperties($sysProps);

        return $message;
    }
}

echo "=== MessageViewExtendedTest ===\n";
TestRunner::run(new MessageViewExtendedTest());
