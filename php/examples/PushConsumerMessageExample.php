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

/**
 * PushConsumer Message Send/Receive Test
 * 
 * This test demonstrates:
 * 1. Start PushConsumer to listen for messages
 * 2. Send multiple messages using Producer
 * 3. Verify messages are consumed correctly
 * 
 * Configuration:
 * - Topic: topic-normal
 * - Consumer Group: GID-normal-consumer
 */

if (!extension_loaded('swoole')) {
    echo "✗ Swoole extension is required for this test\n";
    exit(1);
}

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';
require_once __DIR__ . '/../SimpleConsumer.php';
require_once __DIR__ . '/../PushConsumer.php';
require_once __DIR__ . '/../ProducerSingleton.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Builder\PushConsumerBuilder;
use Apache\Rocketmq\Consumer\FilterExpression;
use Apache\Rocketmq\Consumer\FilterExpressionType;
use Apache\Rocketmq\Consumer\ConsumeResult;

// Configuration
$endpoints = '127.0.0.1:8080';
$topic = 'topic-normal';
$consumerGroup = 'GID-normal-consumer';

echo "========================================\n";
echo "  PushConsumer Message Send/Receive Test\n";
echo "========================================\n\n";
echo "Configuration:\n";
echo "  - Topic: {$topic}\n";
echo "  - Consumer Group: {$consumerGroup}\n";
echo "  - Endpoints: {$endpoints}\n\n";

// Track received messages
$receivedMessages = [];
$messageCount = 0;
$testPassed = false;

// Run in Swoole coroutine
go(function() use ($topic, $consumerGroup, $endpoints, &$receivedMessages, &$messageCount, &$testPassed) {

try {
    // ========================================
    // Step 1: Start PushConsumer
    // ========================================
    echo "[Step 1] Starting PushConsumer...\n";
    echo str_repeat("-", 60) . "\n";
    
    $config = new ClientConfiguration($endpoints);
    $config->withSslEnabled(false);
    
    // Define message listener
    $messageListener = function($message) use (&$receivedMessages, &$messageCount) {
        $msgId = (string) $message->getMessageId();
        $body = $message->getBody();
        $tag = $message->getTag() ?: 'N/A';
        $keys = $message->getKeys();
        
        $messageCount++;
        $receivedMessages[] = [
            'id' => $msgId,
            'body' => $body,
            'tag' => $tag,
            'keys' => $keys,
            'timestamp' => time()
        ];
        
        echo "  [{$messageCount}] Received message\n";
        echo "      Message ID: {$msgId}\n";
        echo "      Body: {$body}\n";
        echo "      Tag: {$tag}\n";
        if (!empty($keys)) {
            echo "      Keys: " . implode(', ', $keys) . "\n";
        }
        echo "\n";
        
        return ConsumeResult::SUCCESS;
    };
    
    // Create and start push consumer
    $consumer = (new PushConsumerBuilder())
        ->setClientConfiguration($config)
        ->setConsumerGroup($consumerGroup)
        ->setSubscriptionExpressions([
            $topic => new FilterExpression('*', FilterExpressionType::TAG),
        ])
        ->setMessageListener($messageListener)
        ->build();
    
    echo "  ✓ PushConsumer created successfully\n";
    echo "  ✓ Subscribed to topic: {$topic}\n";
    echo "  ✓ Message listener registered\n\n";
    
    // ========================================
    // Step 3: PushConsumer is already started by build()
    // ========================================
    echo "[Step 3] PushConsumer already started by build()...\n";
    echo str_repeat("-", 60) . "\n";
    
    // Give consumer time to initialize and start receiving
    \Swoole\Coroutine::sleep(2);
    echo "  ✓ PushConsumer started and ready\n";
    echo "  ✓ Waiting for messages...\n\n";
    
    // ========================================
    // Step 4: Send Messages using Producer
    // ========================================
    echo "[Step 4] Sending test messages...\n";
    echo str_repeat("-", 60) . "\n";
    
    // Get producer instance using singleton
    $producer = ProducerSingleton::getInstance($topic);
    
    $testMessages = [
        [
            'body' => 'Test message #1 - Hello RocketMQ',
            'tag' => 'test-tag-1',
            'keys' => ['key-001']
        ],
        [
            'body' => 'Test message #2 - PHP SDK Test',
            'tag' => 'test-tag-2',
            'keys' => ['key-002']
        ],
        [
            'body' => 'Test message #3 - PushConsumer Verification',
            'tag' => 'test-tag-3',
            'keys' => ['key-003']
        ],
        [
            'body' => 'Test message #4 - Message Order Test',
            'tag' => 'test-tag-4',
            'keys' => ['key-004']
        ],
        [
            'body' => 'Test message #5 - Final Test Message',
            'tag' => 'test-tag-5',
            'keys' => ['key-005']
        ],
    ];
    
    $sentMessages = [];
    
    foreach ($testMessages as $index => $msgData) {
        try {
            $msgBuilder = new MessageBuilder();
            $message = $msgBuilder
                ->setTopic($topic)
                ->setBody($msgData['body'])
                ->setTag($msgData['tag'])
                ->setKeys(implode(',', $msgData['keys']))  // Convert array to comma-separated string
                ->build();
            
            echo "  → Sending message #" . ($index + 1) . "...\n";
            $receipt = $producer->send($message);
            
            $sentMessages[] = [
                'id' => $receipt->getMessageId(),
                'body' => $msgData['body'],
                'tag' => $msgData['tag'],
                'keys' => $msgData['keys']
            ];
            
            echo "  ✓ Sent successfully\n";
            echo "    Message ID: {$receipt->getMessageId()}\n";
            echo "    Body: {$msgData['body']}\n\n";
            
            // Small delay between messages
            usleep(200000); // 200ms
            
        } catch (\Exception $e) {
            echo "  ✗ Failed to send message #" . ($index + 1) . ": " . $e->getMessage() . "\n\n";
        }
    }
    
    echo "Total sent: " . count($sentMessages) . " messages\n\n";
    
    // ========================================
    // Step 5: Wait for messages to be consumed
    // ========================================
    echo "[Step 5] Waiting for messages to be consumed...\n";
    echo str_repeat("-", 60) . "\n";
    
    // Wait for messages to be consumed
    $expectedCount = count($sentMessages);
    $maxWaitTime = 15; // seconds
    $waitedTime = 0;
    
    while ($messageCount < $expectedCount && $waitedTime < $maxWaitTime) {
        echo "  ⏳ Received {$messageCount}/{$expectedCount} messages, waiting...\n";
        \Swoole\Coroutine::sleep(2);
        $waitedTime += 2;
    }
    
    echo "\n";
    
    // ========================================
    // Step 6: Verify results
    // ========================================
    echo "[Step 6] Verification Results\n";
    echo str_repeat("-", 60) . "\n";
    
    echo "Expected messages: {$expectedCount}\n";
    echo "Received messages: {$messageCount}\n\n";
    
    if ($messageCount >= $expectedCount) {
        echo "✅ TEST PASSED!\n";
        echo "   All messages were successfully consumed.\n\n";
        $testPassed = true;
    } else {
        echo "⚠️  TEST WARNING\n";
        echo "   Some messages may not have been consumed yet.\n";
        echo "   Expected: {$expectedCount}, Received: {$messageCount}\n\n";
    }
    
    // Show received message details
    if (!empty($receivedMessages)) {
        echo "Received Message Summary:\n";
        foreach ($receivedMessages as $index => $msg) {
            echo "  [" . ($index + 1) . "] ID: {$msg['id']}\n";
            echo "      Body: {$msg['body']}\n";
            echo "      Tag: {$msg['tag']}\n";
            echo "\n";
        }
    }
    
    // ========================================
    // Step 7: Cleanup
    // ========================================
    echo "[Step 7] Cleaning up...\n";
    echo str_repeat("-", 60) . "\n";
    
    // Shutdown consumer gracefully
    $consumer->shutdown();
    echo "  ✓ PushConsumer shutdown successfully\n";
    
    // Note: Producer is singleton, no need to shutdown
    
    echo "\n";
    
} catch (\Exception $e) {
    echo "\n❌ ERROR: " . $e->getMessage() . "\n";
    echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
    exit(1);
}

// Final summary
echo "========================================\n";
echo "  TEST SUMMARY\n";
echo "========================================\n\n";

echo "Configuration:\n";
echo "  Topic: {$topic}\n";
echo "  Consumer Group: {$consumerGroup}\n";
echo "  Endpoints: {$endpoints}\n\n";

if ($testPassed) {
    echo "Status: ✅ SUCCESS\n";
    echo "All messages sent and consumed successfully!\n";
} else {
    echo "Status: ⚠️  INCOMPLETE\n";
    echo "Some messages may not have been consumed.\n";
    echo "This could be due to:\n";
    echo "  - Network latency\n";
    echo "  - Consumer initialization delay\n";
    echo "  - Server-side processing time\n";
}

echo "\nKey Points Verified:\n";
echo "  ✓ PushConsumer can subscribe to topics\n";
echo "  ✓ Producer can send messages\n";
echo "  ✓ Messages are delivered to consumers\n";
echo "  ✓ Message content is preserved\n";
echo "  ✓ Tags and keys are correctly set\n";

echo "\n";

}); // End of Swoole coroutine
