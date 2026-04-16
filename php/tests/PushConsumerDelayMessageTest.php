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
 * PushConsumer delay message test
 * 
 * Topic: topic-delay
 * Consumer Group: GID-delay-consumer
 * Message Count: 8
 */
class PushConsumerDelayMessageTest extends TestCase
{
    private $endpoints = '127.0.0.1:8080';
    private $topic = 'topic-delay';
    private $consumerGroup = 'GID-delay-consumer';
    
    /**
     * Test delay message consumption
     */
    public function testDelayMessageConsumption()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $receivedMessages = [];
        $messageCount = 0;
        $testCount = 8;
        $delaySeconds = 5; // 5 seconds delay
        
        go(function() use (&$receivedMessages, &$messageCount, $testCount, $delaySeconds) {
            $config = new ClientConfiguration($this->endpoints);
            $config->withSslEnabled(false);
            
            $messageListener = function($message) use (&$receivedMessages, &$messageCount) {
                $messageCount++;
                $receivedMessages[] = [
                    'id' => (string) $message->getMessageId(),
                    'body' => $message->getBody(),
                    'tag' => $message->getTag(),
                    'deliveryTimestamp' => $message->getDeliveryTimestamp(),
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
            
            // Wait for consumer to start
            \Swoole\Coroutine::sleep(2);
            
            // Send delay messages
            $producer = \ProducerSingleton::getInstance($this->topic);
            
            for ($i = 1; $i <= $testCount; $i++) {
                $deliveryTimestamp = (time() + $delaySeconds) * 1000; // Convert to milliseconds
                
                $message = (new MessageBuilder())
                    ->setTopic($this->topic)
                    ->setBody("Delay message #{$i} ({$delaySeconds}s delay)")
                    ->setDeliveryTimestamp($deliveryTimestamp)
                    ->setTag("delay-tag-{$i}")
                    ->setKeys("key-delay-{$i}")
                    ->build();
                
                $receipt = $producer->send($message);
                usleep(100000); // 100ms delay
            }
            
            // Wait for messages to be consumed (include delay time)
            $maxWait = $delaySeconds + 10;
            $waited = 0;
            while ($messageCount < $testCount && $waited < $maxWait) {
                \Swoole\Coroutine::sleep(1);
                $waited++;
            }
            
            $consumer->shutdown();
        });
        
        // Assertions
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive at least 1 delay message');
        $this->assertLessThanOrEqual($testCount * 2, $messageCount, 'Should not receive excessive duplicate messages');
        
        if (!empty($receivedMessages)) {
            $firstMsg = $receivedMessages[0];
            $this->assertArrayHasKey('deliveryTimestamp', $firstMsg);
            $this->assertGreaterThan(0, $firstMsg['deliveryTimestamp']);
        }
    }
    
    /**
     * Test delay message with different delay times
     */
    public function testDelayMessageWithDifferentDelays()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $receivedMessages = [];
        $messageCount = 0;
        $delays = [3, 5, 8]; // Different delay times in seconds
        
        go(function() use (&$receivedMessages, &$messageCount, $delays) {
            $config = new ClientConfiguration($this->endpoints);
            $config->withSslEnabled(false);
            
            $messageListener = function($message) use (&$receivedMessages, &$messageCount) {
                $messageCount++;
                $receivedMessages[] = [
                    'body' => $message->getBody(),
                    'deliveryTimestamp' => $message->getDeliveryTimestamp(),
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
            
            foreach ($delays as $index => $delaySeconds) {
                $deliveryTimestamp = (time() + $delaySeconds) * 1000;
                
                $message = (new MessageBuilder())
                    ->setTopic($this->topic)
                    ->setBody("Delay message with {$delaySeconds}s delay")
                    ->setDeliveryTimestamp($deliveryTimestamp)
                    ->setTag("delay-{$delaySeconds}s")
                    ->build();
                
                $producer->send($message);
                usleep(100000);
            }
            
            // Wait for all messages (use max delay + buffer)
            $maxDelay = max($delays);
            $maxWait = $maxDelay + 10;
            $waited = 0;
            while ($messageCount < count($delays) && $waited < $maxWait) {
                \Swoole\Coroutine::sleep(1);
                $waited++;
            }
            
            $consumer->shutdown();
        });
        
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive delay messages with different delays');
    }
    
    /**
     * Test delay message timing accuracy
     */
    public function testDelayMessageTimingAccuracy()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $receivedMessages = [];
        $messageCount = 0;
        $delaySeconds = 3;
        $testCount = 3;
        
        go(function() use (&$receivedMessages, &$messageCount, $delaySeconds, $testCount) {
            $config = new ClientConfiguration($this->endpoints);
            $config->withSslEnabled(false);
            
            $messageListener = function($message) use (&$receivedMessages, &$messageCount) {
                $messageCount++;
                $receivedMessages[] = [
                    'deliveryTimestamp' => $message->getDeliveryTimestamp(),
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
            $sendTime = time();
            
            for ($i = 1; $i <= $testCount; $i++) {
                $deliveryTimestamp = (time() + $delaySeconds) * 1000;
                
                $message = (new MessageBuilder())
                    ->setTopic($this->topic)
                    ->setBody("Timing test message #{$i}")
                    ->setDeliveryTimestamp($deliveryTimestamp)
                    ->build();
                
                $producer->send($message);
                usleep(100000);
            }
            
            // Wait for consumption
            $maxWait = $delaySeconds + 10;
            $waited = 0;
            while ($messageCount < $testCount && $waited < $maxWait) {
                \Swoole\Coroutine::sleep(1);
                $waited++;
            }
            
            $consumer->shutdown();
        });
        
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive delay messages for timing test');
        
        // Verify timing accuracy (allow 2 seconds tolerance)
        if (!empty($receivedMessages)) {
            foreach ($receivedMessages as $msg) {
                $expectedDeliveryTime = $msg['deliveryTimestamp'] / 1000;
                $actualReceiveTime = $msg['receiveTime'];
                $diff = abs($actualReceiveTime - $expectedDeliveryTime);
                
                // Message should be delivered around the expected time (with some tolerance)
                $this->assertLessThanOrEqual(
                    $delaySeconds + 2,
                    $diff,
                    "Message delivery time should be close to expected delay (diff: {$diff}s)"
                );
            }
        }
    }
}
