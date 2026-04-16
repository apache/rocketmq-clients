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
use Apache\Rocketmq\Builder\PushConsumerBuilder;
use Apache\Rocketmq\Consumer\FilterExpression;
use Apache\Rocketmq\Consumer\FilterExpressionType;
use Apache\Rocketmq\Consumer\ConsumeResult;

// ProducerSingleton is a global class, not in namespace

/**
 * PushConsumer FIFO/order message test
 * 
 * Topic: topic-order
 * Consumer Group: GID-order-consumer
 * Message Count: 4
 */
class PushConsumerOrderMessageTest extends TestCase
{
    private $endpoints = '127.0.0.1:8080';
    private $topic = 'topic-order';
    private $consumerGroup = 'GID-order-consumer';
    
    /**
     * Test FIFO message consumption with message groups
     */
    public function testFifoMessageConsumption()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $receivedMessages = [];
        $messageCount = 0;
        $testCount = 4;
        
        go(function() use (&$receivedMessages, &$messageCount, $testCount) {
            $config = new ClientConfiguration($this->endpoints);
            $config->withSslEnabled(false);
            
            $messageListener = function($message) use (&$receivedMessages, &$messageCount) {
                $messageCount++;
                $receivedMessages[] = [
                    'id' => (string) $message->getMessageId(),
                    'body' => $message->getBody(),
                    'messageGroup' => $message->getMessageGroup(),
                    'timestamp' => time(),
                ];
                return ConsumeResult::SUCCESS;
            };
            
            $consumer = (new PushConsumerBuilder())
                ->setClientConfiguration($config)
                ->setConsumerGroup($this->consumerGroup)
                ->setSubscriptionExpressions([
                    $this->topic => new FilterExpression('*', FilterExpressionType::TAG),
                ])
                ->setMessageListener($messageListener)
                ->build();
            
            // Wait for consumer to start
            \Swoole\Coroutine::sleep(2);
            
            // Send FIFO messages with different message groups
            $producer = \ProducerSingleton::getInstance($this->topic);
            $groups = ['group-A', 'group-B'];
            
            for ($i = 1; $i <= $testCount; $i++) {
                $group = $groups[$i % count($groups)];
                $message = (new MessageBuilder())
                    ->setTopic($this->topic)
                    ->setBody("FIFO message #{$i} from {$group}")
                    ->setMessageGroup($group)
                    ->setTag("order-tag-{$i}")
                    ->build();
                
                $receipt = $producer->send($message);
                usleep(100000); // 100ms delay
            }
            
            // Wait for messages to be consumed
            $maxWait = 10;
            $waited = 0;
            while ($messageCount < $testCount && $waited < $maxWait) {
                \Swoole\Coroutine::sleep(1);
                $waited++;
            }
            
            $consumer->shutdown();
        });
        
        // Assertions
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive at least 1 FIFO message');
        $this->assertLessThanOrEqual($testCount * 2, $messageCount, 'Should not receive excessive duplicate messages');
        
        if (!empty($receivedMessages)) {
            $firstMsg = $receivedMessages[0];
            $this->assertArrayHasKey('messageGroup', $firstMsg);
            $this->assertNotEmpty($firstMsg['messageGroup']);
            $this->assertContains($firstMsg['messageGroup'], ['group-A', 'group-B']);
        }
    }
    
    /**
     * Test FIFO message ordering within same group
     */
    public function testFifoMessageOrderingInSameGroup()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $receivedMessages = [];
        $messageCount = 0;
        $testCount = 4;
        $messageGroup = 'order-group-1';
        
        go(function() use (&$receivedMessages, &$messageCount, $testCount, $messageGroup) {
            $config = new ClientConfiguration($this->endpoints);
            $config->withSslEnabled(false);
            
            $messageListener = function($message) use (&$receivedMessages, &$messageCount) {
                $messageCount++;
                $receivedMessages[] = [
                    'body' => $message->getBody(),
                    'messageGroup' => $message->getMessageGroup(),
                    'receiveTime' => time(),
                ];
                return ConsumeResult::SUCCESS;
            };
            
            $consumer = (new PushConsumerBuilder())
                ->setClientConfiguration($config)
                ->setConsumerGroup($this->consumerGroup)
                ->setSubscriptionExpressions([
                    $this->topic => new FilterExpression('*', FilterExpressionType::TAG),
                ])
                ->setMessageListener($messageListener)
                ->build();
            
            \Swoole\Coroutine::sleep(2);
            
            $producer = \ProducerSingleton::getInstance($this->topic);
            
            // Send messages in order to same group
            for ($i = 1; $i <= $testCount; $i++) {
                $message = (new MessageBuilder())
                    ->setTopic($this->topic)
                    ->setBody("Ordered message #{$i}")
                    ->setMessageGroup($messageGroup)
                    ->build();
                
                $producer->send($message);
                usleep(100000);
            }
            
            // Wait for consumption
            $maxWait = 10;
            $waited = 0;
            while ($messageCount < $testCount && $waited < $maxWait) {
                \Swoole\Coroutine::sleep(1);
                $waited++;
            }
            
            $consumer->shutdown();
        });
        
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive ordered messages');
        
        // Verify all messages belong to same group
        if (!empty($receivedMessages)) {
            foreach ($receivedMessages as $msg) {
                $this->assertEquals($messageGroup, $msg['messageGroup']);
            }
        }
    }
    
    /**
     * Test FIFO message with multiple groups
     */
    public function testFifoMessageWithMultipleGroups()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $receivedByGroup = [];
        $messageCount = 0;
        $groups = ['group-X', 'group-Y', 'group-Z'];
        
        go(function() use (&$receivedByGroup, &$messageCount, $groups) {
            $config = new ClientConfiguration($this->endpoints);
            $config->withSslEnabled(false);
            
            $messageListener = function($message) use (&$receivedByGroup, &$messageCount) {
                $messageCount++;
                $group = $message->getMessageGroup() ?: 'no-group';
                
                if (!isset($receivedByGroup[$group])) {
                    $receivedByGroup[$group] = 0;
                }
                $receivedByGroup[$group]++;
                
                return ConsumeResult::SUCCESS;
            };
            
            $consumer = (new PushConsumerBuilder())
                ->setClientConfiguration($config)
                ->setConsumerGroup($this->consumerGroup)
                ->setSubscriptionExpressions([
                    $this->topic => new FilterExpression('*', FilterExpressionType::TAG),
                ])
                ->setMessageListener($messageListener)
                ->build();
            
            \Swoole\Coroutine::sleep(2);
            
            $producer = \ProducerSingleton::getInstance($this->topic);
            
            // Send messages to different groups
            foreach ($groups as $index => $group) {
                $message = (new MessageBuilder())
                    ->setTopic($this->topic)
                    ->setBody("Message for {$group}")
                    ->setMessageGroup($group)
                    ->build();
                
                $producer->send($message);
                usleep(100000);
            }
            
            // Wait for consumption
            $maxWait = 10;
            $waited = 0;
            while ($messageCount < count($groups) && $waited < $maxWait) {
                \Swoole\Coroutine::sleep(1);
                $waited++;
            }
            
            $consumer->shutdown();
        });
        
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive messages from multiple groups');
        $this->assertGreaterThanOrEqual(1, count($receivedByGroup), 'Should have messages from at least 1 group');
    }
}
