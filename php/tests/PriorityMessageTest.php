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
 * Priority message test class
 * 
 * Tests priority message functionality including:
 * - Setting priority values
 * - Validation rules (mutual exclusivity)
 * - Priority value range validation
 */
class PriorityMessageTest extends TestCase
{
    /**
     * Test setting valid priority values
     */
    public function testValidPriorityValues()
    {
        $validPriorities = [1, 5, 10, 50, 100];
        
        foreach ($validPriorities as $priority) {
            $message = (new MessageBuilder())
                ->setTopic('test-topic')
                ->setBody('Test message')
                ->setPriority($priority)
                ->build();
            
            $this->assertEquals($priority, $message->getPriority());
        }
    }
    
    /**
     * Test priority mutual exclusivity with delivery timestamp
     */
    public function testPriorityAndDeliveryTimestampMutualExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setDeliveryTimestamp(time() * 1000 + 60000)
            ->setPriority(5)
            ->build();
    }
    
    /**
     * Test priority mutual exclusivity with message group
     */
    public function testPriorityAndMessageGroupMutualExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setMessageGroup('test-group')
            ->setPriority(5)
            ->build();
    }
    
    /**
     * Test priority mutual exclusivity with lite topic
     */
    public function testPriorityAndLiteTopicMutualExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setLiteTopic('lite-topic')
            ->setPriority(5)
            ->build();
    }
    
    /**
     * Test negative priority validation
     */
    public function testNegativePriorityValidation()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setPriority(-1)
            ->build();
    }
    
    /**
     * Test zero priority
     */
    public function testZeroPriority()
    {
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setPriority(0)
            ->build();
        
        $this->assertEquals(0, $message->getPriority());
    }
    
    /**
     * Test priority with other message properties
     */
    public function testPriorityWithOtherProperties()
    {
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setTag('test-tag')
            ->setKeys('key1', 'key2')
            ->setPriority(5)
            ->build();
        
        $this->assertEquals(5, $message->getPriority());
        $this->assertEquals('test-tag', $message->getTag());
        $keys = $message->getKeys();
        $this->assertIsArray($keys);
        $this->assertContains('key1', $keys);
        $this->assertContains('key2', $keys);
    }
    
    /**
     * Test default priority is null
     */
    public function testDefaultPriorityIsNull()
    {
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        $this->assertNull($message->getPriority());
    }
}
