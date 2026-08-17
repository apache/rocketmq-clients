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

use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Resource;

class MessageTest extends TestCase
{
    public function testBuildMessageWithTopicAndBody()
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $message = new Message();
        $message->setTopic($topic);
        $message->setBody('test body content');

        $this->assertEquals('test-topic', $message->getTopic()->getName(), "Topic name should match");
        $this->assertEquals('test body content', $message->getBody(), "Body should match");
    }

    public function testMessageWithTag()
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $sysProps = new SystemProperties();
        $sysProps->setTag('test-tag');

        $message = new Message();
        $message->setTopic($topic);
        $message->setBody('body');
        $message->setSystemProperties($sysProps);

        $props = $message->getSystemProperties();
        $this->assertTrue($props->hasTag(), "Should have tag set");
        $this->assertEquals('test-tag', $props->getTag(), "Tag should match");
    }

    public function testMessageWithKeys()
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $sysProps = new SystemProperties();
        $sysProps->setKeys(['key1', 'key2']);

        $message = new Message();
        $message->setTopic($topic);
        $message->setBody('body');
        $message->setSystemProperties($sysProps);

        $props = $message->getSystemProperties();
        $keys = $props->getKeys();
        $this->assertEquals(2, count($keys), "Should have 2 keys");
        $this->assertEquals('key1', $keys[0], "First key should match");
    }

    public function testMessageWithMessageGroup()
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $sysProps = new SystemProperties();
        $sysProps->setMessageGroup('group-A');

        $message = new Message();
        $message->setTopic($topic);
        $message->setBody('body');
        $message->setSystemProperties($sysProps);

        $props = $message->getSystemProperties();
        $this->assertTrue($props->hasMessageGroup(), "Should have message group");
        $this->assertEquals('group-A', $props->getMessageGroup(), "Message group should match");
    }

    public function testMessageWithUserProperties()
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $message = new Message();
        $message->setTopic($topic);
        $message->setBody('body');
        $message->getUserProperties()['custom-key'] = 'custom-value';

        $this->assertEquals(
            'custom-value',
            $message->getUserProperties()['custom-key'],
            "User property should match"
        );
    }

    public function testMessageBodyImmutability()
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $originalBody = 'original body';
        $message = new Message();
        $message->setTopic($topic);
        $message->setBody($originalBody);

        // Modify original variable
        $originalBody = 'modified';

        // Message body should remain unchanged
        $this->assertEquals('original body', $message->getBody(), "Message body should be immutable after set");
    }

    public function testMessageWithPriority()
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $sysProps = new SystemProperties();
        $sysProps->setPriority(1);

        $message = new Message();
        $message->setTopic($topic);
        $message->setBody('body');
        $message->setSystemProperties($sysProps);

        $props = $message->getSystemProperties();
        $this->assertTrue($props->hasPriority(), "Should have priority set");
        $this->assertEquals(1, $props->getPriority(), "Priority should be 1");
    }

    public function testMessageWithLiteTopic()
    {
        $topic = new Resource();
        $topic->setName('test-topic');

        $sysProps = new SystemProperties();
        $sysProps->setLiteTopic('lite-topic-A');

        $message = new Message();
        $message->setTopic($topic);
        $message->setBody('body');
        $message->setSystemProperties($sysProps);

        $props = $message->getSystemProperties();
        $this->assertTrue($props->hasLiteTopic(), "Should have lite topic");
        $this->assertEquals('lite-topic-A', $props->getLiteTopic(), "Lite topic should match");
    }
}
