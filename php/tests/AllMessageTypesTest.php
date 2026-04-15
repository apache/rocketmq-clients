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
 * All message types test class
 * 
 * Tests different message types: normal, FIFO, delayed, transactional
 */
class AllMessageTypesTest extends TestCase
{
    /**
     * Test normal message
     */
    public function testNormalMessage()
    {
        $message = (new MessageBuilder())
            ->setTopic('topic-normal')
            ->setBody('Normal message')
            ->setTag('normal-tag')
            ->setKeys('key1')
            ->build();
        
        $this->assertEquals('topic-normal', $message->getTopic());
        $this->assertNull($message->getMessageGroup());
        $this->assertNull($message->getDeliveryTimestamp());
        $this->assertNull($message->getPriority());
        
        $keys = $message->getKeys();
        $this->assertIsArray($keys);
        $this->assertContains('key1', $keys);
    }
    
    /**
     * Test FIFO message
     */
    public function testFifoMessage()
    {
        $message = (new MessageBuilder())
            ->setTopic('topic-order')
            ->setBody('FIFO message')
            ->setMessageGroup('order-group')
            ->build();
        
        $this->assertEquals('order-group', $message->getMessageGroup());
    }
    
    /**
     * Test delayed message
     */
    public function testDelayedMessage()
    {
        $deliveryTimestamp = (time() + 60) * 1000;
        
        $message = (new MessageBuilder())
            ->setTopic('topic-delay')
            ->setBody('Delayed message')
            ->setDeliveryTimestamp($deliveryTimestamp)
            ->build();
        
        $this->assertEquals($deliveryTimestamp, $message->getDeliveryTimestamp());
    }
    
    /**
     * Test priority message
     */
    public function testPriorityMessage()
    {
        $message = (new MessageBuilder())
            ->setTopic('topic-priority')
            ->setBody('Priority message')
            ->setPriority(5)
            ->build();
        
        $this->assertEquals(5, $message->getPriority());
    }
    
    /**
     * Test lite topic message
     */
    public function testLiteTopicMessage()
    {
        $message = (new MessageBuilder())
            ->setTopic('parent-topic')
            ->setBody('Lite message')
            ->setLiteTopic('lite-topic-1')
            ->build();
        
        $this->assertEquals('lite-topic-1', $message->getLiteTopic());
    }
    
    /**
     * Test message type mutual exclusivity - FIFO vs Priority
     */
    public function testFifoVsPriorityExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test')
            ->setMessageGroup('group')
            ->setPriority(5)
            ->build();
    }
    
    /**
     * Test message type mutual exclusivity - FIFO vs Delayed
     */
    public function testFifoVsDelayedExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test')
            ->setMessageGroup('group')
            ->setDeliveryTimestamp((time() + 60) * 1000)
            ->build();
    }
    
    /**
     * Test message type mutual exclusivity - Priority vs Delayed
     */
    public function testPriorityVsDelayedExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test')
            ->setPriority(5)
            ->setDeliveryTimestamp((time() + 60) * 1000)
            ->build();
    }
    
    /**
     * Test message type mutual exclusivity - Lite vs FIFO
     */
    public function testLiteVsFifoExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test')
            ->setLiteTopic('lite-topic')
            ->setMessageGroup('group')
            ->build();
    }
}
