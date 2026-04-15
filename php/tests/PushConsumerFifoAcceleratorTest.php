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
 * FIFO consume accelerator test class
 * 
 * Tests FIFO message group processing and ordering verification
 */
class PushConsumerFifoAcceleratorTest extends TestCase
{
    /**
     * Test build messages for multiple groups
     */
    public function testBuildMessagesForMultipleGroups()
    {
        $groups = ['order-group-A', 'order-group-B', 'order-group-C'];
        
        foreach ($groups as $group) {
            $message = (new MessageBuilder())
                ->setTopic('topic-order')
                ->setBody("Msg-1-from-{$group}")
                ->setMessageGroup($group)
                ->build();
            
            $this->assertEquals($group, $message->getMessageGroup());
        }
    }
    
    /**
     * Test message order tracking
     */
    public function testMessageOrderTracking()
    {
        $groupLastOrder = [];
        $messages = [];
        
        // Simulate receiving messages in order
        for ($i = 1; $i <= 5; $i++) {
            $message = (new MessageBuilder())
                ->setTopic('topic-order')
                ->setBody("Msg-{$i}-from-group-A")
                ->setMessageGroup('group-A')
                ->build();
            
            $groupName = $message->getMessageGroup();
            
            if (!isset($groupLastOrder[$groupName])) {
                $groupLastOrder[$groupName] = 0;
            }
            
            // Extract order from body
            preg_match('/Msg-(\d+)-from-(.+)/', $message->getBody(), $matches);
            $orderNum = intval($matches[1]);
            
            $isInOrder = ($orderNum == $groupLastOrder[$groupName] + 1);
            $groupLastOrder[$groupName] = $orderNum;
            
            $messages[] = [
                'order' => $orderNum,
                'inOrder' => $isInOrder,
            ];
        }
        
        // Verify all messages are in order
        foreach ($messages as $msg) {
            $this->assertTrue($msg['inOrder']);
        }
    }
    
    /**
     * Test parallel group consumption simulation
     */
    public function testParallelGroupConsumption()
    {
        $groups = ['group-A', 'group-B', 'group-C'];
        $receivedMessages = [];
        
        // Simulate concurrent consumption of different groups
        foreach ($groups as $group) {
            $receivedMessages[$group] = [];
            
            for ($i = 1; $i <= 3; $i++) {
                $message = (new MessageBuilder())
                    ->setTopic('topic-order')
                    ->setBody("Msg-{$i}-from-{$group}")
                    ->setMessageGroup($group)
                    ->build();
                
                $receivedMessages[$group][] = [
                    'order' => $i,
                    'body' => $message->getBody(),
                ];
            }
        }
        
        // Verify each group has correct number of messages
        foreach ($groups as $group) {
            $this->assertCount(3, $receivedMessages[$group]);
            
            // Verify order within group
            for ($i = 0; $i < 3; $i++) {
                $expectedOrder = $i + 1;
                $this->assertEquals($expectedOrder, $receivedMessages[$group][$i]['order']);
            }
        }
    }
    
    /**
     * Test FIFO message with properties
     */
    public function testFifoMessageWithProperties()
    {
        $message = (new MessageBuilder())
            ->setTopic('topic-order')
            ->setBody('Msg-1-from-group-A')
            ->setMessageGroup('group-A')
            ->setTag('fifo-test')
            ->setKeys('key-group-A-1')
            ->build();
        
        $this->assertEquals('group-A', $message->getMessageGroup());
        $this->assertEquals('fifo-test', $message->getTag());
        $keys = $message->getKeys();
        $this->assertIsArray($keys);
        $this->assertContains('key-group-A-1', $keys);
    }
    
    /**
     * Test order violation detection
     */
    public function testOrderViolationDetection()
    {
        $groupLastOrder = ['group-A' => 0];
        
        // Simulate out-of-order messages
        $testOrders = [1, 2, 4, 3, 5]; // 4 comes before 3 - violation!
        $violations = [];
        
        foreach ($testOrders as $order) {
            $isInOrder = ($order == $groupLastOrder['group-A'] + 1);
            
            if (!$isInOrder) {
                $violations[] = $order;
            }
            
            $groupLastOrder['group-A'] = $order;
        }
        
        // Should detect violations
        $this->assertNotEmpty($violations);
        $this->assertContains(4, $violations);
    }
    
    /**
     * Test message group completion tracking
     */
    public function testGroupCompletionTracking()
    {
        $expectedMessagesPerGroup = 5;
        $receivedMessages = [
            'group-A' => array_fill(0, 5, ['order' => 1]),
            'group-B' => array_fill(0, 3, ['order' => 1]), // Incomplete
            'group-C' => array_fill(0, 5, ['order' => 1]),
        ];
        
        $completedGroups = [];
        
        foreach ($receivedMessages as $group => $messages) {
            if (count($messages) == $expectedMessagesPerGroup) {
                $completedGroups[] = $group;
            }
        }
        
        $this->assertCount(2, $completedGroups);
        $this->assertContains('group-A', $completedGroups);
        $this->assertContains('group-C', $completedGroups);
        $this->assertNotContains('group-B', $completedGroups);
    }
}
