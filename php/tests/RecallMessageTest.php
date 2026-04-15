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
 * Message recall functionality test class
 * 
 * Tests message building with delivery timestamp for recall scenarios
 */
class RecallMessageTest extends TestCase
{
    /**
     * Test build delay message
     */
    public function testBuildDelayMessage()
    {
        $deliveryTimestamp = (time() + 60) * 1000; // 60 seconds from now in milliseconds
        
        $message = (new MessageBuilder())
            ->setTopic('topic-delay')
            ->setBody('Delay message')
            ->setDeliveryTimestamp($deliveryTimestamp)
            ->build();
        
        $this->assertEquals($deliveryTimestamp, $message->getDeliveryTimestamp());
    }
    
    /**
     * Test delay message mutual exclusivity with priority
     */
    public function testDelayAndPriorityMutualExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setDeliveryTimestamp((time() + 60) * 1000)
            ->setPriority(5)
            ->build();
    }
    
    /**
     * Test delay message mutual exclusivity with message group
     */
    public function testDelayAndMessageGroupMutualExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setDeliveryTimestamp((time() + 60) * 1000)
            ->setMessageGroup('test-group')
            ->build();
    }
    
    /**
     * Test delay message mutual exclusivity with lite topic
     */
    public function testDelayAndLiteTopicMutualExclusivity()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setDeliveryTimestamp((time() + 60) * 1000)
            ->setLiteTopic('lite-topic')
            ->build();
    }
    
    /**
     * Test delay message with tag and keys
     */
    public function testDelayMessageWithProperties()
    {
        $deliveryTimestamp = (time() + 30) * 1000;
        
        $message = (new MessageBuilder())
            ->setTopic('topic-delay')
            ->setBody('Delay message')
            ->setTag('recall-test')
            ->setKeys('recall-test-key')
            ->setDeliveryTimestamp($deliveryTimestamp)
            ->build();
        
        $this->assertEquals($deliveryTimestamp, $message->getDeliveryTimestamp());
        $this->assertEquals('recall-test', $message->getTag());
        $keys = $message->getKeys();
        $this->assertIsArray($keys);
        $this->assertContains('recall-test-key', $keys);
    }
    
    /**
     * Test past delivery timestamp validation
     */
    public function testPastDeliveryTimestamp()
    {
        $this->expectException(\Apache\Rocketmq\Exception\MessageException::class);
        $this->expectExceptionMessage('Delivery timestamp must be in the future');
        
        $pastTimestamp = (time() - 60) * 1000; // 60 seconds ago
        
        // Should throw exception at client side
        (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->setDeliveryTimestamp($pastTimestamp)
            ->build();
    }
}
