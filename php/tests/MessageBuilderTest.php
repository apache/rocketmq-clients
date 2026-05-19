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
require_once __DIR__ . '/../MessageBuilder.php';
require_once __DIR__ . '/../MessageView.php';
require_once __DIR__ . '/../autoload.php';

use Apache\Rocketmq\MessageBuilder;
use Apache\Rocketmq\MessageView;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;

class MessageBuilderTest
{
    public function testBuildMinimalMessage()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('hello world')
            ->build();

        TestRunner::assertEqualsWithMessage('test-topic', $msg->getTopic()->getName(), "Topic should be set");
        TestRunner::assertEqualsWithMessage('hello world', $msg->getBody(), "Body should be set");
        TestRunner::assertFalse($msg->hasSystemProperties(), "Should have no system properties for minimal message");
    }

    public function testBuildMessageWithTag()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setTag('my-tag')
            ->build();

        TestRunner::assertTrue($msg->hasSystemProperties(), "Should have system properties");
        TestRunner::assertEqualsWithMessage('my-tag', $msg->getSystemProperties()->getTag(), "Tag should match");
    }

    public function testBuildMessageWithKeys()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setKeys(['key1', 'key2'])
            ->build();

        $keys = $msg->getSystemProperties()->getKeys();
        TestRunner::assertEqualsWithMessage(2, count($keys), "Should have 2 keys");
    }

    public function testBuildFifoMessage()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setMessageGroup('order-group-1')
            ->build();

        $sysProps = $msg->getSystemProperties();
        TestRunner::assertTrueWithMessage($sysProps->hasMessageGroup(), "Should have message group");
        TestRunner::assertEqualsWithMessage('order-group-1', $sysProps->getMessageGroup(), "Message group should match");
    }

    public function testBuildDelayMessage()
    {
        $deliveryTimeMs = (time() + 60) * 1000;
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setDeliveryTimestamp($deliveryTimeMs)
            ->build();

        $sysProps = $msg->getSystemProperties();
        TestRunner::assertTrueWithMessage($sysProps->hasDeliveryTimestamp(), "Should have delivery timestamp");
    }

    public function testBuildPriorityMessage()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setPriority(1)
            ->build();

        $sysProps = $msg->getSystemProperties();
        TestRunner::assertTrueWithMessage($sysProps->hasPriority(), "Should have priority");
        TestRunner::assertEqualsWithMessage(1, $sysProps->getPriority(), "Priority should be 1");
    }

    public function testBuildLiteMessage()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setLiteTopic('lite-subtopic')
            ->build();

        $sysProps = $msg->getSystemProperties();
        TestRunner::assertTrueWithMessage($sysProps->hasLiteTopic(), "Should have lite topic");
    }

    public function testBuildMessageWithUserProperties()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->addProperty('custom-key', 'custom-value')
            ->addProperty('another-key', 'another-value')
            ->build();

        $props = $msg->getUserProperties();
        TestRunner::assertEqualsWithMessage('custom-value', $props['custom-key'], "User property should match");
        TestRunner::assertEqualsWithMessage(2, count($props), "Should have 2 user properties");
    }

    public function testBuildFullMessage()
    {
        $msg = (new MessageBuilder())
            ->setTopic('full-topic')
            ->setBody('full body content')
            ->setTag('full-tag')
            ->setKeys(['key-a', 'key-b'])
            ->addProperty('env', 'production')
            ->build();

        TestRunner::assertEqualsWithMessage('full-topic', $msg->getTopic()->getName(), "Topic should match");
        TestRunner::assertEqualsWithMessage('full body content', $msg->getBody(), "Body should match");
        TestRunner::assertEqualsWithMessage('full-tag', $msg->getSystemProperties()->getTag(), "Tag should match");
    }

    public function testBuilderReturnsThisForChaining()
    {
        $builder = new MessageBuilder();
        $result = $builder->setTopic('test');
        TestRunner::assertTrueWithMessage($result === $builder, "setTopic should return \$this");
    }

    // --- Validation tests (mirrors Java MessageImplTest) ---

    public function testRejectsEmptyTopic()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())->setTopic('')->setBody('body')->build();
        }, "Empty topic should be rejected");
    }

    public function testRejectsMissingTopic()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())->setBody('body')->build();
        }, "Missing topic should be rejected");
    }

    public function testRejectsMissingBody()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())->setTopic('test')->build();
        }, "Missing body should be rejected");
    }

    public function testRejectsTagWithVerticalBar()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())->setTopic('test')->setBody('body')->setTag('|')->build();
        }, "Tag with '|' should be rejected");
    }

    public function testRejectsTagWithWhitespace()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())->setTopic('test')->setBody('body')->setTag("tag value")->build();
        }, "Tag with whitespace should be rejected");
    }

    public function testRejectsBlankKey()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())->setTopic('test')->setBody('body')->addKey('  ')->build();
        }, "Blank key should be rejected");
    }

    public function testRejectsBlankLiteTopic()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())->setTopic('test')->setBody('body')->setLiteTopic('   ')->build();
        }, "Blank lite topic should be rejected");
    }

    public function testRejectsInvalidPriority()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())->setTopic('test')->setBody('body')->setPriority(0)->build();
        }, "Priority 0 should be rejected");

        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())->setTopic('test')->setBody('body')->setPriority(10)->build();
        }, "Priority 10 should be rejected");
    }

    public function testRejectsMessageTypeConflict()
    {
        // delay + fifo conflict
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())
                ->setTopic('test')
                ->setBody('body')
                ->setDeliveryTimestamp(time() * 1000)
                ->setMessageGroup('group')
                ->build();
        }, "Delay + FIFO should conflict");

        // fifo + lite conflict
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())
                ->setTopic('test')
                ->setBody('body')
                ->setMessageGroup('group')
                ->setLiteTopic('lite')
                ->build();
        }, "FIFO + Lite should conflict");

        // priority + delay conflict
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())
                ->setTopic('test')
                ->setBody('body')
                ->setPriority(1)
                ->setDeliveryTimestamp(time() * 1000)
                ->build();
        }, "Priority + Delay should conflict");
    }

    /**
     * Mirrors Java: testTopicSetterWithNull - setTopic with null/empty.
     */
    public function testTopicSetterWithEmptyString()
    {
        TestRunner::assertThrows(\InvalidArgumentException::class, function() {
            (new MessageBuilder())->setTopic('  ')->build();
        }, "Whitespace-only topic should be rejected");
    }

    /**
     * Mirrors Java: testTagSetter with valid tag.
     */
    public function testTagSetterReturnsValidTag()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setTag('tagA')
            ->build();

        TestRunner::assertTrue($msg->hasSystemProperties(), "Should have system properties");
        TestRunner::assertEqualsWithMessage('tagA', $msg->getSystemProperties()->getTag(), "Tag should be tagA");
    }

    /**
     * Mirrors Java: testKeySetter - valid key.
     */
    public function testKeySetterValidKey()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setKeys(['keyA'])
            ->build();

        $keys = $msg->getSystemProperties()->getKeys();
        TestRunner::assertTrue(count($keys) > 0, "Should have at least 1 key");
    }

    /**
     * Mirrors Java: testBuild - verify optional fields are not present.
     */
    public function testBuildNoOptionalFields()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->build();

        // No system properties should be set for minimal message
        TestRunner::assertFalse($msg->hasSystemProperties(), "Should not have system properties");
    }

    /**
     * Mirrors Java: testMessagePropertiesGetterImmutability - properties are copied,
     * not referenced, so clearing the returned map doesn't affect the message.
     * In PHP we test that adding a property works correctly.
     */
    public function testMultiplePropertiesAdd()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->addProperty('foo', 'value')
            ->addProperty('bar', 'value2')
            ->build();

        $props = $msg->getUserProperties();
        TestRunner::assertEqualsWithMessage(2, count($props), "Should have 2 user properties");
        TestRunner::assertEqualsWithMessage('value', $props['foo'], "Property 'foo' should match");
        TestRunner::assertEqualsWithMessage('value2', $props['bar'], "Property 'bar' should match");
    }
}

class MessageViewTest
{
    private function createTestMessage()
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $sysProps = new SystemProperties();
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

        TestRunner::assertEqualsWithMessage('test-topic', $view->getTopic(), "Topic should match");
    }

    public function testGetBody()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertEqualsWithMessage('test body content', $view->getBody(), "Body should match");
    }

    public function testGetMessageId()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertEqualsWithMessage('test-msg-id-001', $view->getMessageId(), "Message ID should match");
    }

    public function testGetTag()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertEqualsWithMessage('test-tag', $view->getTag(), "Tag should match");
    }

    public function testGetKeys()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        $keys = $view->getKeys();
        TestRunner::assertEqualsWithMessage(1, count($keys), "Should have 1 key");
        TestRunner::assertEqualsWithMessage('key1', $keys[0], "Key should match");
    }

    public function testGetMessageGroup()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertEqualsWithMessage('test-group', $view->getMessageGroup(), "Message group should match");
    }

    public function testGetReceiptHandle()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg, 'receipt-handle-abc');

        TestRunner::assertEqualsWithMessage('receipt-handle-abc', $view->getReceiptHandle(), "Receipt handle should match");
    }

    public function testGetDeliveryAttempt()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg, null, null, 3);

        TestRunner::assertEqualsWithMessage(3, $view->getDeliveryAttempt(), "Delivery attempt should be 3");
    }

    public function testGetUserProperties()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertEqualsWithMessage('test', $view->getProperty('env'), "User property should match");
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
        TestRunner::assertTrueWithMessage(
            strpos($str, 'test-topic') !== false,
            "__toString should contain topic"
        );
        TestRunner::assertTrueWithMessage(
            strpos($str, 'test-msg-id-001') !== false,
            "__toString should contain message ID"
        );
    }

    public function testGetMessage()
    {
        $protoMsg = $this->createTestMessage();
        $view = new MessageView($protoMsg);

        TestRunner::assertTrueWithMessage(
            $view->getMessage() === $protoMsg,
            "getMessage should return the original protobuf message"
        );
    }
}

echo "=== MessageBuilderTest ===\n";
$test = new MessageBuilderTest();
$test->testBuildMinimalMessage();
echo "  [OK] testBuildMinimalMessage\n";
$test->testBuildMessageWithTag();
echo "  [OK] testBuildMessageWithTag\n";
$test->testBuildMessageWithKeys();
echo "  [OK] testBuildMessageWithKeys\n";
$test->testBuildFifoMessage();
echo "  [OK] testBuildFifoMessage\n";
$test->testBuildDelayMessage();
echo "  [OK] testBuildDelayMessage\n";
$test->testBuildPriorityMessage();
echo "  [OK] testBuildPriorityMessage\n";
$test->testBuildLiteMessage();
echo "  [OK] testBuildLiteMessage\n";
$test->testBuildMessageWithUserProperties();
echo "  [OK] testBuildMessageWithUserProperties\n";
$test->testBuildFullMessage();
echo "  [OK] testBuildFullMessage\n";
$test->testBuilderReturnsThisForChaining();
echo "  [OK] testBuilderReturnsThisForChaining\n";
$test->testRejectsEmptyTopic();
echo "  [OK] testRejectsEmptyTopic\n";
$test->testRejectsMissingTopic();
echo "  [OK] testRejectsMissingTopic\n";
$test->testRejectsMissingBody();
echo "  [OK] testRejectsMissingBody\n";
$test->testRejectsTagWithVerticalBar();
echo "  [OK] testRejectsTagWithVerticalBar\n";
$test->testRejectsTagWithWhitespace();
echo "  [OK] testRejectsTagWithWhitespace\n";
$test->testRejectsBlankKey();
echo "  [OK] testRejectsBlankKey\n";
$test->testRejectsBlankLiteTopic();
echo "  [OK] testRejectsBlankLiteTopic\n";
$test->testRejectsInvalidPriority();
echo "  [OK] testRejectsInvalidPriority\n";
$test->testRejectsMessageTypeConflict();
echo "  [OK] testRejectsMessageTypeConflict\n";
$test->testTopicSetterWithEmptyString();
echo "  [OK] testTopicSetterWithEmptyString\n";
$test->testTagSetterReturnsValidTag();
echo "  [OK] testTagSetterReturnsValidTag\n";
$test->testKeySetterValidKey();
echo "  [OK] testKeySetterValidKey\n";
$test->testBuildNoOptionalFields();
echo "  [OK] testBuildNoOptionalFields\n";
$test->testMultiplePropertiesAdd();
echo "  [OK] testMultiplePropertiesAdd\n";

echo "\n=== MessageViewTest ===\n";
$test = new MessageViewTest();
$test->testGetTopic();
echo "  [OK] testGetTopic\n";
$test->testGetBody();
echo "  [OK] testGetBody\n";
$test->testGetMessageId();
echo "  [OK] testGetMessageId\n";
$test->testGetTag();
echo "  [OK] testGetTag\n";
$test->testGetKeys();
echo "  [OK] testGetKeys\n";
$test->testGetMessageGroup();
echo "  [OK] testGetMessageGroup\n";
$test->testGetReceiptHandle();
echo "  [OK] testGetReceiptHandle\n";
$test->testGetDeliveryAttempt();
echo "  [OK] testGetDeliveryAttempt\n";
$test->testGetUserProperties();
echo "  [OK] testGetUserProperties\n";
$test->testIsFifo();
echo "  [OK] testIsFifo\n";
$test->testToString();
echo "  [OK] testToString\n";
$test->testGetMessage();
echo "  [OK] testGetMessage\n";
