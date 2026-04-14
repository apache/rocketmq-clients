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

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Builder\LitePushConsumerBuilder;
use Apache\Rocketmq\Consumer\ConsumeResult;
use Apache\Rocketmq\Exception\LiteSubscriptionQuotaExceededException;
use Apache\Rocketmq\Logger;

/**
 * Lite Push Consumer Example
 * 
 * This example demonstrates how to:
 * 1. Create a lite push consumer
 * 2. Subscribe to multiple lite topics
 * 3. Handle lite subscription quota exceeded exceptions
 * 4. Process messages from lite topics
 * 
 * Lite topics are sub-topics under a parent topic that share resources
 * and have simplified management compared to regular topics.
 */

// Configuration
$endpoints = '127.0.0.1:8080';
$consumerGroup = 'GID-normal-consumer';
$parentTopic = 'topic-normal';

echo "========================================\n";
echo "  Lite Push Consumer Example\n";
echo "========================================\n\n";

// Create client configuration
$config = new ClientConfiguration($endpoints);
$config->withSslEnabled(false);

// Create lite push consumer using builder
echo "[Step 1] Creating Lite Push Consumer\n";
echo str_repeat("-", 60) . "\n";

try {
    $consumer = (new LitePushConsumerBuilder())
        ->setClientConfiguration($config)
        ->setConsumerGroup($consumerGroup)
        ->bindTopic($parentTopic)
        ->setMessageListener(function($messageView) {
            // Extract message information
            $messageId = $messageView->getMessageId();
            $topic = $messageView->getTopic();
            $liteTopic = $messageView->getLiteTopic();
            $body = $messageView->getBody();
            $tag = $messageView->getTag();
            $keys = $messageView->getKeys();
            
            echo "[Message Received]\n";
            echo "  Message ID: {$messageId}\n";
            echo "  Topic: {$topic}\n";
            echo "  Lite Topic: " . ($liteTopic ?? 'N/A') . "\n";
            echo "  Tag: " . ($tag ?? 'N/A') . "\n";
            echo "  Keys: " . implode(', ', $keys ?? []) . "\n";
            echo "  Body: {$body}\n";
            echo "  Born Time: " . date('Y-m-d H:i:s', $messageView->getBornTimestamp() / 1000) . "\n\n";
            
            // Return consume result
            return ConsumeResult::SUCCESS;
        })
        ->build();
    
    echo "  ✓ Lite push consumer created\n";
    echo "    Consumer Group: {$consumerGroup}\n";
    echo "    Parent Topic: {$parentTopic}\n\n";
    
} catch (\Exception $e) {
    echo "  ✗ Failed to create lite push consumer: " . $e->getMessage() . "\n\n";
    exit(1);
}

// Start the consumer
echo "[Step 2] Starting Lite Push Consumer\n";
echo str_repeat("-", 60) . "\n";

try {
    $consumer->start();
    echo "  ✓ Lite push consumer started successfully\n\n";
    
} catch (\Exception $e) {
    echo "  ✗ Failed to start lite push consumer: " . $e->getMessage() . "\n\n";
    exit(1);
}

// Subscribe to lite topics
echo "[Step 3] Subscribing to Lite Topics\n";
echo str_repeat("-", 60) . "\n";

$liteTopics = [
    'lite-topic-1',
    'lite-topic-2',
    'lite-topic-3',
];

$subscribedCount = 0;
$failedCount = 0;

foreach ($liteTopics as $liteTopic) {
    try {
        echo "  → Subscribing to lite topic: {$liteTopic}\n";
        
        $consumer->subscribeLite($liteTopic, function($messageView) use ($liteTopic) {
            echo "[Lite Message Received - {$liteTopic}]\n";
            echo "  Message ID: {$messageView->getMessageId()}\n";
            echo "  Body: {$messageView->getBody()}\n\n";
            
            return ConsumeResult::SUCCESS;
        });
        
        $subscribedCount++;
        echo "  ✓ Successfully subscribed\n\n";
        
    } catch (LiteSubscriptionQuotaExceededException $e) {
        $failedCount++;
        
        echo "  ✗ Lite subscription quota exceeded\n";
        echo "    Lite Topic: {$liteTopic}\n";
        echo "    Error: {$e->getMessage()}\n";
        echo "    Action: \n";
        echo "      1. Evaluate and increase the lite topic resource limit\n";
        echo "      2. Unsubscribe unused lite topics:\n";
        echo "         \$consumer->unsubscribeLite('{$liteTopic}')\n\n";
        
    } catch (\Exception $e) {
        $failedCount++;
        
        echo "  ✗ Failed to subscribe\n";
        echo "    Lite Topic: {$liteTopic}\n";
        echo "    Error: {$e->getMessage()}\n";
        echo "    Action: Retry later\n\n";
    }
}

echo "Subscription Summary: {$subscribedCount} subscribed, {$failedCount} failed\n\n";

// Show current subscriptions
echo "[Step 4] Current Lite Topic Subscriptions\n";
echo str_repeat("-", 60) . "\n";

$liteTopicSet = $consumer->getLiteTopicSet();
if (empty($liteTopicSet)) {
    echo "  No lite topics subscribed\n\n";
} else {
    echo "  Subscribed Lite Topics:\n";
    foreach ($liteTopicSet as $topic) {
        echo "    - {$topic}\n";
    }
    echo "\n";
}

// Test unsubscribe
echo "[Step 5] Testing Unsubscribe\n";
echo str_repeat("-", 60) . "\n";

if (!empty($liteTopicSet)) {
    $topicToUnsubscribe = $liteTopicSet[0];
    
    try {
        echo "  → Unsubscribing from: {$topicToUnsubscribe}\n";
        
        $consumer->unsubscribeLite($topicToUnsubscribe);
        
        echo "  ✓ Successfully unsubscribed\n\n";
        
        // Show updated subscriptions
        $updatedTopicSet = $consumer->getLiteTopicSet();
        echo "  Updated Lite Topic Subscriptions:\n";
        if (empty($updatedTopicSet)) {
            echo "    No lite topics subscribed\n";
        } else {
            foreach ($updatedTopicSet as $topic) {
                echo "    - {$topic}\n";
            }
        }
        echo "\n";
        
    } catch (\Exception $e) {
        echo "  ✗ Failed to unsubscribe: {$e->getMessage()}\n\n";
    }
} else {
    echo "  No topics to unsubscribe\n\n";
}

// Wait for messages (in production, this would be your application logic)
echo "[Step 6] Waiting for Messages\n";
echo str_repeat("-", 60) . "\n";
echo "  The consumer is now running and listening for messages.\n";
echo "  In a real application, this would continue running.\n";
echo "  For this example, we'll wait 10 seconds...\n\n";

sleep(10);

// Shutdown the consumer
echo "[Step 7] Shutting Down Lite Push Consumer\n";
echo str_repeat("-", 60) . "\n";

try {
    $consumer->shutdown();
    echo "  ✓ Lite push consumer shutdown successfully\n\n";
    
} catch (\Exception $e) {
    echo "  ✗ Failed to shutdown lite push consumer: {$e->getMessage()}\n\n";
}

// Summary
echo "========================================\n";
echo "  SUMMARY\n";
echo "========================================\n\n";

echo "Lite Topics Subscribed: {$subscribedCount}\n";
echo "Subscription Failures: {$failedCount}\n";
echo "\n";

if ($failedCount == 0) {
    echo "✅ All lite topic subscriptions successful!\n";
} else {
    echo "⚠️  Some subscriptions failed.\n";
    echo "   Check the error messages above for details.\n";
}

echo "\nKey Points:\n";
echo "- Lite push consumers subscribe to lite topics under a parent topic\n";
echo "- Use subscribeLite() to subscribe to specific lite topics\n";
echo "- Handle LiteSubscriptionQuotaExceededException for quota issues\n";
echo "- Use unsubscribeLite() to free up quota when topics are no longer needed\n";
echo "- Lite consumers share the same consumer group across all lite topics\n";
echo "- Each lite topic can have its own message listener\n";
