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
 * PushConsumer FIFO consumption test class
 * 
 * Tests FIFO message building and message group functionality
 */
class PushConsumerFifoTest extends TestCase
{
    /**
     * Test build FIFO message with message group
     */
    public function testBuildFifoMessage()
    {
        $message = (new MessageBuilder())
            ->setTopic('topic-order')
            ->setBody('Message #1 from group-A')
            ->setMessageGroup('group-A')
            ->build();
        
        $this->assertEquals('group-A', $message->getMessageGroup());
        $this->assertEquals('topic-order', $message->getTopic());
    }
    
    /**
     * Test FIFO message mutual exclusivity with priority
     */
    public function testFifoAndPriorityMutualExclusivity()
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
     * Test FIFO message mutual exclusivity with delivery timestamp
     */
    public function testFifoAndDeliveryTimestampMutualExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setMessageGroup('test-group')
            ->setDeliveryTimestamp((time() + 60) * 1000)
            ->build();
    }
    
    /**
     * Test FIFO message mutual exclusivity with lite topic
     */
    public function testFifoAndLiteTopicMutualExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setMessageGroup('test-group')
            ->setLiteTopic('lite-topic')
            ->build();
    }
    
    /**
     * Test multiple message groups
     */
    public function testMultipleMessageGroups()
    {
        $groups = ['group-A', 'group-B', 'group-C'];
        
        foreach ($groups as $index => $group) {
            $message = (new MessageBuilder())
                ->setTopic('topic-order')
                ->setBody("Message from {$group}")
                ->setMessageGroup($group)
                ->build();
            
            $this->assertEquals($group, $message->getMessageGroup());
        }
    }
    
    /**
     * Test FIFO message with tag and keys
     */
    public function testFifoMessageWithProperties()
    {
        $message = (new MessageBuilder())
            ->setTopic('topic-order')
            ->setBody('FIFO message')
            ->setMessageGroup('group-A')
            ->setTag('fifo-tag')
            ->setKeys('key-group-A-1')
            ->build();
        
        $this->assertEquals('group-A', $message->getMessageGroup());
        $this->assertEquals('fifo-tag', $message->getTag());
        // getKeys() returns array, setKeys() accepts variadic string
        $keys = $message->getKeys();
        $this->assertIsArray($keys);
        $this->assertContains('key-group-A-1', $keys);
    }
    
    /**
     * Test message group ordering verification
     */
    public function testMessageGroupOrdering()
    {
        $messages = [];
        
        // Simulate sending messages in order
        for ($i = 1; $i <= 5; $i++) {
            $message = (new MessageBuilder())
                ->setTopic('topic-order')
                ->setBody("Message #{$i} from group-A")
                ->setMessageGroup('group-A')
                ->build();
            
            $messages[] = [
                'order' => $i,
                'body' => $message->getBody(),
                'group' => $message->getMessageGroup(),
            ];
        }
        
        // Verify order
        foreach ($messages as $index => $msg) {
            $expectedOrder = $index + 1;
            $this->assertEquals($expectedOrder, $msg['order']);
            $this->assertEquals('group-A', $msg['group']);
        }
    }
}
