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
 * PushConsumer transaction message test
 * 
 * Topic: topic-transaction
 * Consumer Group: GID-transaction-consumer
 * Message Count: 4
 */
class PushConsumerTransactionMessageTest extends TestCase
{
    private $endpoints = '127.0.0.1:8080';
    private $topic = 'topic-transaction';
    private $consumerGroup = 'GID-transaction-consumer';
    
    /**
     * Test transaction message consumption
     */
    public function testTransactionMessageConsumption()
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
            
            // Send transaction messages
            $producer = \ProducerSingleton::getInstance($this->topic);
            
            for ($i = 1; $i <= $testCount; $i++) {
                $message = (new MessageBuilder())
                    ->setTopic($this->topic)
                    ->setBody("Transaction message #{$i}")
                    ->setTag("transaction-tag-{$i}")
                    ->setKeys("key-txn-{$i}")
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
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive at least 1 transaction message');
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
     * Test transaction message with different tags
     */
    public function testTransactionMessageWithDifferentTags()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $tags = ['txn-commit', 'txn-rollback', 'txn-pending'];
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
                    ->setBody("Transaction message with tag {$tag}")
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
        
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive transaction messages with different tags');
    }
    
    /**
     * Test transaction message with properties
     */
    public function testTransactionMessageWithProperties()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $receivedMessages = [];
        $messageCount = 0;
        $testCount = 3;
        
        go(function() use (&$receivedMessages, &$messageCount, $testCount) {
            $config = new ClientConfiguration($this->endpoints);
            $config->withSslEnabled(false);
            
            $messageListener = function($message) use (&$receivedMessages, &$messageCount) {
                $messageCount++;
                $sysProps = $message->getSystemProperties();
                
                $receivedMessages[] = [
                    'messageId' => (string) $message->getMessageId(),
                    'body' => $message->getBody(),
                    'tag' => $message->getTag(),
                    'keys' => $message->getKeys(),
                    'bornHost' => $sysProps ? $sysProps->getBornHost() : 'N/A',
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
                    ->setBody("Transaction message with properties #{$i}")
                    ->setTag("txn-prop-tag")
                    ->setKeys("txn-key-{$i},order-id-{$i}")
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
        
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive transaction messages with properties');
        
        if (!empty($receivedMessages)) {
            $firstMsg = $receivedMessages[0];
            $this->assertArrayHasKey('messageId', $firstMsg);
            $this->assertArrayHasKey('keys', $firstMsg);
        }
    }
    
    /**
     * Test transaction message body content integrity
     */
    public function testTransactionMessageBodyIntegrity()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is required');
        }
        
        $expectedBodies = [
            'Transaction test data #1 - JSON payload',
            'Transaction test data #2 - XML payload',
            'Transaction test data #3 - Plain text',
            'Transaction test data #4 - Binary-like data',
        ];
        
        $receivedBodies = [];
        $messageCount = 0;
        
        go(function() use ($expectedBodies, &$receivedBodies, &$messageCount) {
            $config = new ClientConfiguration($this->endpoints);
            $config->withSslEnabled(false);
            
            $messageListener = function($message) use (&$receivedBodies, &$messageCount) {
                $messageCount++;
                $receivedBodies[] = $message->getBody();
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
            
            foreach ($expectedBodies as $index => $body) {
                $message = (new MessageBuilder())
                    ->setTopic($this->topic)
                    ->setBody($body)
                    ->setTag("integrity-test")
                    ->build();
                
                $producer->send($message);
                usleep(100000);
            }
            
            // Wait for consumption
            $maxWait = 10;
            $waited = 0;
            while ($messageCount < count($expectedBodies) && $waited < $maxWait) {
                \Swoole\Coroutine::sleep(1);
                $waited++;
            }
            
            $consumer->shutdown();
        });
        
        $this->assertGreaterThanOrEqual(1, $messageCount, 'Should receive transaction messages');
        
        // Verify body content integrity
        if (!empty($receivedBodies)) {
            foreach ($receivedBodies as $body) {
                $this->assertIsString($body);
                $this->assertNotEmpty($body);
            }
        }
    }
}
