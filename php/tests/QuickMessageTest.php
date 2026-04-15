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

namespace Apache\Rocketmq\Tests;

use PHPUnit\Framework\TestCase;
use Apache\Rocketmq\Builder\MessageBuilder;

/**
 * Quick message operations test class
 * 
 * Tests basic message building operations
 */
class QuickMessageTest extends TestCase
{
    /**
     * Test build simple message
     */
    public function testBuildSimpleMessage()
    {
        $message = (new MessageBuilder())
            ->setTopic('topic-normal')
            ->setBody('Test Message')
            ->build();
        
        $this->assertEquals('topic-normal', $message->getTopic());
        $this->assertEquals('Test Message', $message->getBody());
    }
    
    /**
     * Test build message with tag and key
     */
    public function testBuildMessageWithTagAndKey()
    {
        $message = (new MessageBuilder())
            ->setTopic('topic-normal')
            ->setBody('Test Message')
            ->setTag('test')
            ->setKeys('test-key')
            ->build();
        
        $this->assertEquals('test', $message->getTag());
        $keys = $message->getKeys();
        $this->assertIsArray($keys);
        $this->assertContains('test-key', $keys);
    }
    
    /**
     * Test build message with multiple keys
     */
    public function testBuildMessageWithMultipleKeys()
    {
        // setKeys accepts variadic string arguments
        $message = (new MessageBuilder())
            ->setTopic('topic-normal')
            ->setBody('Test Message')
            ->setKeys('key1', 'key2', 'key3')
            ->build();
        
        $keys = $message->getKeys();
        $this->assertIsArray($keys);
        $this->assertCount(3, $keys);
        $this->assertContains('key1', $keys);
        $this->assertContains('key2', $keys);
        $this->assertContains('key3', $keys);
    }
    
    /**
     * Test message builder chaining
     */
    public function testMessageBuilderChaining()
    {
        $builder = new MessageBuilder();
        
        $result = $builder
            ->setTopic('test-topic')
            ->setBody('test body')
            ->setTag('test-tag');
        
        $this->assertSame($builder, $result);
    }
}
