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
require_once __DIR__ . '/../MessageBuilder.php';
require_once __DIR__ . '/../autoload.php';

use Apache\Rocketmq\MessageBuilder;

class MessageBuilderTest extends TestCase
{
    public function testBuildMinimalMessage()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('hello world')
            ->build();

        $this->assertEquals('test-topic', $msg->getTopic()->getName(), "Topic should be set");
        $this->assertEquals('hello world', $msg->getBody(), "Body should be set");
        $this->assertFalse($msg->hasSystemProperties(), "Should have no system properties for minimal message");
    }

    public function testBuildMessageWithTag()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setTag('my-tag')
            ->build();

        $this->assertTrue($msg->hasSystemProperties(), "Should have system properties");
        $this->assertEquals('my-tag', $msg->getSystemProperties()->getTag(), "Tag should match");
    }

    public function testBuildMessageWithKeys()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setKeys(['key1', 'key2'])
            ->build();

        $keys = $msg->getSystemProperties()->getKeys();
        $this->assertEquals(2, count($keys), "Should have 2 keys");
    }

    public function testBuildFifoMessage()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setMessageGroup('order-group-1')
            ->build();

        $sysProps = $msg->getSystemProperties();
        $this->assertTrue($sysProps->hasMessageGroup(), "Should have message group");
        $this->assertEquals('order-group-1', $sysProps->getMessageGroup(), "Message group should match");
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
        $this->assertTrue($sysProps->hasDeliveryTimestamp(), "Should have delivery timestamp");
    }

    public function testBuildPriorityMessage()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setPriority(1)
            ->build();

        $sysProps = $msg->getSystemProperties();
        $this->assertTrue($sysProps->hasPriority(), "Should have priority");
        $this->assertEquals(1, $sysProps->getPriority(), "Priority should be 1");
    }

    public function testBuildLiteMessage()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setLiteTopic('lite-subtopic')
            ->build();

        $sysProps = $msg->getSystemProperties();
        $this->assertTrue($sysProps->hasLiteTopic(), "Should have lite topic");
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
        $this->assertEquals('custom-value', $props['custom-key'], "User property should match");
        $this->assertEquals(2, count($props), "Should have 2 user properties");
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

        $this->assertEquals('full-topic', $msg->getTopic()->getName(), "Topic should match");
        $this->assertEquals('full body content', $msg->getBody(), "Body should match");
        $this->assertEquals('full-tag', $msg->getSystemProperties()->getTag(), "Tag should match");
    }

    public function testBuilderReturnsThisForChaining()
    {
        $builder = new MessageBuilder();
        $result = $builder->setTopic('test');
        $this->assertTrue($result === $builder, "setTopic should return \$this");
    }

    // --- Validation tests (mirrors Java MessageImplTest) ---

    public function testRejectsEmptyTopic()
    {
        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())->setTopic('')->setBody('body')->build();
    }

    public function testRejectsMissingTopic()
    {
        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())->setBody('body')->build();
    }

    public function testRejectsMissingBody()
    {
        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())->setTopic('test')->build();
    }

    public function testRejectsTagWithVerticalBar()
    {
        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())->setTopic('test')->setBody('body')->setTag('|')->build();
    }

    public function testRejectsTagWithWhitespace()
    {
        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())->setTopic('test')->setBody('body')->setTag("tag value")->build();
    }

    public function testRejectsBlankKey()
    {
        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())->setTopic('test')->setBody('body')->addKey('  ')->build();
    }

    public function testRejectsBlankLiteTopic()
    {
        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())->setTopic('test')->setBody('body')->setLiteTopic('   ')->build();
    }

    public function testRejectsInvalidPriority()
    {
        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())->setTopic('test')->setBody('body')->setPriority(0)->build();

        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())->setTopic('test')->setBody('body')->setPriority(10)->build();
    }

    public function testRejectsMessageTypeConflict()
    {
        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())
                ->setTopic('test')
                ->setBody('body')
                ->setDeliveryTimestamp(time() * 1000)
                ->setMessageGroup('group')
                ->build();

        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())
                ->setTopic('test')
                ->setBody('body')
                ->setMessageGroup('group')
                ->setLiteTopic('lite')
                ->build();

        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())
                ->setTopic('test')
                ->setBody('body')
                ->setPriority(1)
                ->setDeliveryTimestamp(time() * 1000)
                ->build();
    }

    public function testTopicSetterWithEmptyString()
    {
        $this->expectException(\InvalidArgumentException::class);
        (new MessageBuilder())->setTopic('  ')->build();
    }

    public function testTagSetterReturnsValidTag()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setTag('tagA')
            ->build();

        $this->assertTrue($msg->hasSystemProperties(), "Should have system properties");
        $this->assertEquals('tagA', $msg->getSystemProperties()->getTag(), "Tag should be tagA");
    }

    public function testKeySetterValidKey()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->setKeys(['keyA'])
            ->build();

        $keys = $msg->getSystemProperties()->getKeys();
        $this->assertTrue(count($keys) > 0, "Should have at least 1 key");
    }

    public function testBuildNoOptionalFields()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->build();

        $this->assertFalse($msg->hasSystemProperties(), "Should not have system properties");
    }

    public function testMultiplePropertiesAdd()
    {
        $msg = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('body')
            ->addProperty('foo', 'value')
            ->addProperty('bar', 'value2')
            ->build();

        $props = $msg->getUserProperties();
        $this->assertEquals(2, count($props), "Should have 2 user properties");
        $this->assertEquals('value', $props['foo'], "Property 'foo' should match");
        $this->assertEquals('value2', $props['bar'], "Property 'bar' should match");
    }
}
