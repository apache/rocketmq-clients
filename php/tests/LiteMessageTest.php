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
use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Exception\LiteTopicQuotaExceededException;

/**
 * Lite message test class
 * 
 * Tests lite topic functionality including:
 * - Setting lite topic on messages
 * - Lite topic quota exception handling
 * - Message builder integration
 */
class LiteMessageTest extends TestCase
{
    /**
     * Test setting lite topic on message
     */
    public function testSetLiteTopic()
    {
        $message = (new MessageBuilder())
            ->setTopic('parent-topic')
            ->setBody('Test message')
            ->setLiteTopic('lite-topic-1')
            ->build();
        
        $this->assertEquals('lite-topic-1', $message->getLiteTopic());
        $this->assertEquals('parent-topic', $message->getTopic());
    }
    
    /**
     * Test lite topic is optional
     */
    public function testLiteTopicOptional()
    {
        $message = (new MessageBuilder())
            ->setTopic('parent-topic')
            ->setBody('Test message')
            ->build();
        
        $this->assertNull($message->getLiteTopic());
    }
    
    /**
     * Test LiteTopicQuotaExceededException
     */
    public function testLiteTopicQuotaException()
    {
        $exception = new LiteTopicQuotaExceededException(
            'Quota exceeded',
            'lite-topic-1'
        );
        
        $this->assertEquals('lite-topic-1', $exception->getLiteTopic());
        $this->assertStringContainsString('Quota exceeded', $exception->getMessage());
    }
    
    /**
     * Test LiteTopicQuotaExceededException with previous exception
     */
    public function testLiteTopicQuotaExceptionWithPrevious()
    {
        $previous = new \Exception('Original error');
        $exception = new LiteTopicQuotaExceededException(
            'Quota exceeded',
            'lite-topic-1',
            0,
            $previous
        );
        
        $this->assertSame($previous, $exception->getPrevious());
    }
    
    /**
     * Test message with keys and lite topic
     */
    public function testMessageWithKeysAndLiteTopic()
    {
        $message = (new MessageBuilder())
            ->setTopic('parent-topic')
            ->setBody('Test message')
            ->setKeys('key1', 'key2')
            ->setLiteTopic('lite-topic-1')
            ->setTag('test-tag')
            ->build();
        
        $this->assertEquals('lite-topic-1', $message->getLiteTopic());
        $keys = $message->getKeys();
        $this->assertIsArray($keys);
        $this->assertContains('key1', $keys);
        $this->assertContains('key2', $keys);
        $this->assertEquals('test-tag', $message->getTag());
    }
    
    /**
     * Test different lite topics
     */
    public function testMultipleLiteTopics()
    {
        $liteTopics = ['lite-topic-1', 'lite-topic-2', 'lite-topic-3'];
        
        foreach ($liteTopics as $index => $liteTopic) {
            $message = (new MessageBuilder())
                ->setTopic('parent-topic')
                ->setBody("Message {$index}")
                ->setLiteTopic($liteTopic)
                ->build();
            
            $this->assertEquals($liteTopic, $message->getLiteTopic());
        }
    }
}
