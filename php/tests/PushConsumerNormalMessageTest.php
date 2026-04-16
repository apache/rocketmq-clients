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
 * PushConsumer normal message test
 * 
 * Topic: topic-normal
 * Consumer Group: GID-normal-consumer
 * Message Count: 8
 */
class PushConsumerNormalMessageTest extends TestCase
{
    private $endpoints = '127.0.0.1:8080';
    private $topic = 'topic-normal';
    private $consumerGroup = 'GID-normal-consumer';
    
    /**
     * Test normal message consumption with PushConsumer
     */
    public function testNormalMessageConsumption()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $receivedMessages = [];
        $messageCount = 0;
        $testCount = 8;
        
        go(function() use (&$receivedMessages, &$messageCount, $testCount) {
            $config = new ClientConfiguration($this->endpoints);
            $config->withSslEnabled(false);
            
            $messageListener = function($message) use (&$receivedMessages, &$messageCount) {
                $messageCount++;
                $receivedMessages[] = [
                    'id' => (string) $message->getMessageId(),
                    'body' => $message->getBody(),
                    'tag' => $message->getTag(),
                    'keys' => $message->getKeys(),
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
            
            // Send test messages
            $producer = \ProducerSingleton::getInstance($this->topic);
            
            for ($i = 1; $i <= $testCount; $i++) {
                $message = (new MessageBuilder())
                    ->setTopic($this->topic)
                    ->setBody("Normal message #{$i}")
                    ->setTag("normal-tag-{$i}")
                    ->setKeys("key-normal-{$i}")
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
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive at least 1 normal message');
        $this->assertLessThanOrEqual($testCount * 2, $messageCount, 'Should not receive excessive duplicate messages');
        
        if (!empty($receivedMessages)) {
            $firstMsg = $receivedMessages[0];
            $this->assertArrayHasKey('id', $firstMsg);
            $this->assertArrayHasKey('body', $firstMsg);
            $this->assertNotEmpty($firstMsg['id']);
            $this->assertNotEmpty($firstMsg['body']);
        }
    }
    
    /**
     * Test normal message with different tags
     */
    public function testNormalMessageWithDifferentTags()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $tags = ['tag-A', 'tag-B', 'tag-C'];
        $receivedByTag = [];
        $messageCount = 0;
        
        go(function() use ($tags, &$receivedByTag, &$messageCount) {
            $config = new ClientConfiguration($this->endpoints);
            $config->withSslEnabled(false);
            
            $messageListener = function($message) use (&$receivedByTag, &$messageCount) {
                $messageCount++;
                $tag = $message->getTag() ?: 'no-tag';
                
                if (!isset($receivedByTag[$tag])) {
                    $receivedByTag[$tag] = 0;
                }
                $receivedByTag[$tag]++;
                
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
            
            foreach ($tags as $index => $tag) {
                $message = (new MessageBuilder())
                    ->setTopic($this->topic)
                    ->setBody("Message with tag {$tag}")
                    ->setTag($tag)
                    ->build();
                
                $producer->send($message);
                usleep(100000);
            }
            
            // Wait for consumption
            $maxWait = 10;
            $waited = 0;
            while ($messageCount < count($tags) && $waited < $maxWait) {
                \Swoole\Coroutine::sleep(1);
                $waited++;
            }
            
            $consumer->shutdown();
        });
        
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive messages with different tags');
    }
    
    /**
     * Test normal message with keys
     */
    public function testNormalMessageWithKeys()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $receivedMessages = [];
        $messageCount = 0;
        $testCount = 5;
        
        go(function() use (&$receivedMessages, &$messageCount, $testCount) {
            $config = new ClientConfiguration($this->endpoints);
            $config->withSslEnabled(false);
            
            $messageListener = function($message) use (&$receivedMessages, &$messageCount) {
                $messageCount++;
                $receivedMessages[] = [
                    'keys' => $message->getKeys(),
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
            
            for ($i = 1; $i <= $testCount; $i++) {
                $message = (new MessageBuilder())
                    ->setTopic($this->topic)
                    ->setBody("Message with keys #{$i}")
                    ->setKeys("key-{$i},order-{$i}")
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
        
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive messages with keys');
        
        if (!empty($receivedMessages)) {
            $this->assertIsArray($receivedMessages[0]['keys']);
        }
    }
}
