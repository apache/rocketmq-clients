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
 * Mirrors Java's GeneralMessageImplTest and MessageViewImpl basics.
 * Covers topic, body, messageId, tag, keys, messageGroup, properties, etc.
 */
class MessageViewExtendedTest
{
    /**
     * Tests born timestamp extraction from system properties.
     */
    public function testBornTimestamp()
    {
        \Apache\Rocketmq\Logger::close();

        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);

        // bornTimestamp should be non-negative (0 if not set by broker)
        TestRunner::assertTrue(
            $view->getBornTimestamp() >= 0,
            "Born timestamp should be non-negative"
        );
    }

    /**
     * Tests born host extraction from system properties.
     */
    public function testBornHost()
    {
        \Apache\Rocketmq\Logger::close();

        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);

        // bornHost is a string (empty if not set)
        TestRunner::assertTrue(
            is_string($view->getBornHost()),
            "Born host should be a string"
        );
    }

    /**
     * Tests decode timestamp is set on construction.
     */
    public function testDecodeTimestamp()
    {
        \Apache\Rocketmq\Logger::close();

        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);
        $beforeDecode = $view->getDecodeTimestamp();

        TestRunner::assertTrue(
            $beforeDecode > 0,
            "Decode timestamp should be positive"
        );
    }

    /**
     * Tests delivery attempt increment.
     */
    public function testIncrementDeliveryAttempt()
    {
        \Apache\Rocketmq\Logger::close();

        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);

        TestRunner::assertEqualsWithMessage(
            1,
            $view->getDeliveryAttempt(),
            "Initial delivery attempt should be 1"
        );

        $view->incrementDeliveryAttempt();
        TestRunner::assertEqualsWithMessage(
            2,
            $view->getDeliveryAttempt(),
            "After increment, delivery attempt should be 2"
        );

        $view->incrementDeliveryAttempt();
        TestRunner::assertEqualsWithMessage(
            3,
            $view->getDeliveryAttempt(),
            "After second increment, delivery attempt should be 3"
        );
    }

    /**
     * Tests message delivery attempt starts at 1 minimum.
     */
    public function testDeliveryAttemptMinimum()
    {
        \Apache\Rocketmq\Logger::close();

        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 0);

        TestRunner::assertTrue(
            $view->getDeliveryAttempt() >= 1,
            "Delivery attempt should be at least 1 even if passed 0"
        );
    }

    /**
     * Tests getProperty for user properties.
     */
    public function testGetProperty()
    {
        \Apache\Rocketmq\Logger::close();

        $message = new Message();
        $message->setBody('hello');
        $topic = new Resource();
        $topic->setName('test-topic');
        $message->setTopic($topic);
        $message->getUserProperties()['env'] = 'production';

        $view = new MessageView($message);

        TestRunner::assertEqualsWithMessage(
            'production',
            $view->getProperty('env'),
            "getProperty should return the correct value"
        );
        TestRunner::assertNull(
            $view->getProperty('nonexistent'),
            "getProperty should return null for missing key"
        );
    }

    /**
     * Tests getProperties returns native PHP array.
     */
    public function testGetPropertiesReturnsNativeArray()
    {
        \Apache\Rocketmq\Logger::close();

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
        TestRunner::assertEqualsWithMessage(
            'val1',
            $props['key1'],
            "Property key1 should be accessible"
        );
    }

    /**
     * Tests getKeys returns native PHP array.
     */
    public function testGetKeysReturnsNativeArray()
    {
        \Apache\Rocketmq\Logger::close();

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
        TestRunner::assertEqualsWithMessage(
            2,
            count($keys),
            "Should have 2 keys"
        );
    }

    /**
     * Tests __toString includes key info.
     */
    public function testToStringIncludesTopic()
    {
        \Apache\Rocketmq\Logger::close();

        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);
        $str = (string)$view;

        TestRunner::assertTrue(
            strpos($str, 'test-topic') !== false,
            "toString should include topic name"
        );
    }

    /**
     * Tests __toString includes messageId.
     */
    public function testToStringIncludesMessageId()
    {
        \Apache\Rocketmq\Logger::close();

        $msg = $this->buildMessage();
        $view = new MessageView($msg, null, null, 1);
        $str = (string)$view;

        TestRunner::assertTrue(
            strpos($str, 'msg-abc-123') !== false,
            "toString should include message ID"
        );
    }

    /**
     * Tests __toString includes CORRUPTED when message is corrupted.
     */
    public function testToStringIncludesCorrupted()
    {
        \Apache\Rocketmq\Logger::close();

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
$test = new MessageViewExtendedTest();
$test->testBornTimestamp();
echo "  [OK] testBornTimestamp\n";
$test->testBornHost();
echo "  [OK] testBornHost\n";
$test->testDecodeTimestamp();
echo "  [OK] testDecodeTimestamp\n";
$test->testIncrementDeliveryAttempt();
echo "  [OK] testIncrementDeliveryAttempt\n";
$test->testDeliveryAttemptMinimum();
echo "  [OK] testDeliveryAttemptMinimum\n";
$test->testGetProperty();
echo "  [OK] testGetProperty\n";
$test->testGetPropertiesReturnsNativeArray();
echo "  [OK] testGetPropertiesReturnsNativeArray\n";
$test->testGetKeysReturnsNativeArray();
echo "  [OK] testGetKeysReturnsNativeArray\n";
$test->testToStringIncludesTopic();
echo "  [OK] testToStringIncludesTopic\n";
$test->testToStringIncludesMessageId();
echo "  [OK] testToStringIncludesMessageId\n";
$test->testToStringIncludesCorrupted();
echo "  [OK] testToStringIncludesCorrupted\n";
