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
use Apache\Rocketmq\Builder\LiteProducerBuilder;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Exception\LiteTopicQuotaExceededException;
use Apache\Rocketmq\Logger;

/**
 * Lite Producer Example
 * 
 * This example demonstrates how to:
 * 1. Create a lite producer for a parent topic
 * 2. Send messages to different lite topics
 * 3. Handle lite topic quota exceeded exceptions
 * 
 * Lite topics are sub-topics under a parent topic that share resources
 * and have simplified management compared to regular topics.
 */

// Configuration
$endpoints = '127.0.0.1:8080';
$parentTopic = 'topic-normal';  // Parent topic

echo "========================================\n";
echo "  Lite Producer Example\n";
echo "========================================\n\n";

// Create client configuration
$config = new ClientConfiguration($endpoints);
$config->withSslEnabled(false);

// Create lite producer using builder
echo "[Step 1] Creating Lite Producer\n";
echo str_repeat("-", 60) . "\n";

try {
    $producer = (new LiteProducerBuilder())
        ->setClientConfiguration($config)
        ->setParentTopic($parentTopic)
        ->build();
    
    echo "  ✓ Lite producer created for parent topic: {$parentTopic}\n\n";
    
} catch (\Exception $e) {
    echo "  ✗ Failed to create lite producer: " . $e->getMessage() . "\n\n";
    exit(1);
}

// Start the producer
echo "[Step 2] Starting Lite Producer\n";
echo str_repeat("-", 60) . "\n";

try {
    $producer->start();
    echo "  ✓ Lite producer started successfully\n";
    echo "    Client ID: " . $producer->getClientId() . "\n\n";
    
} catch (\Exception $e) {
    echo "  ✗ Failed to start lite producer: " . $e->getMessage() . "\n\n";
    exit(1);
}

// Send lite messages
echo "[Step 3] Sending Lite Messages\n";
echo str_repeat("-", 60) . "\n";

$liteTopics = [
    'lite-topic-1',
    'lite-topic-2',
    'lite-topic-3',
];

$sentCount = 0;
$failedCount = 0;

foreach ($liteTopics as $index => $liteTopic) {
    try {
        // Build message with lite topic
        $msgBuilder = new MessageBuilder();
        $message = $msgBuilder
            ->setTopic($parentTopic)
            ->setBody("This is a lite message for Apache RocketMQ - Topic #{$index}")
            ->setKeys(["lite-key-{$index}"])
            ->setLiteTopic($liteTopic)
            ->build();
        
        echo "  → Sending message to lite topic: {$liteTopic}\n";
        
        $receipt = $producer->send($message);
        $sentCount++;
        
        echo "  ✓ Sent successfully\n";
        echo "    Message ID: {$receipt->getMessageId()}\n";
        echo "    Lite Topic: {$liteTopic}\n\n";
        
    } catch (LiteTopicQuotaExceededException $e) {
        $failedCount++;
        
        echo "  ✗ Lite topic quota exceeded\n";
        echo "    Lite Topic: {$e->getLiteTopic()}\n";
        echo "    Error: {$e->getMessage()}\n";
        echo "    Action: Evaluate and increase the lite topic resource limit\n\n";
        
    } catch (\Exception $e) {
        $failedCount++;
        
        echo "  ✗ Failed to send message\n";
        echo "    Lite Topic: {$liteTopic}\n";
        echo "    Error: {$e->getMessage()}\n\n";
    }
}

echo "Send Summary: {$sentCount} sent, {$failedCount} failed\n\n";

// Test async sending
echo "[Step 4] Testing Async Lite Message Sending\n";
echo str_repeat("-", 60) . "\n";

try {
    $msgBuilder = new MessageBuilder();
    $asyncMessage = $msgBuilder
        ->setTopic($parentTopic)
        ->setBody("Async lite message")
        ->setKeys(["async-lite-key"])
        ->setLiteTopic('lite-topic-async')
        ->build();
    
    echo "  → Sending async message...\n";
    
    $producer->sendAsync($asyncMessage, function($receipt, $error) {
        if ($error !== null) {
            if ($error instanceof LiteTopicQuotaExceededException) {
                echo "  ✗ Async send failed - Quota exceeded: {$error->getLiteTopic()}\n";
            } else {
                echo "  ✗ Async send failed: {$error->getMessage()}\n";
            }
        } else {
            echo "  ✓ Async send successful\n";
            echo "    Message ID: {$receipt->getMessageId()}\n";
        }
    });
    
    // Wait a bit for async operation
    usleep(500000); // 500ms
    echo "\n";
    
} catch (\Exception $e) {
    echo "  ✗ Failed to send async message: {$e->getMessage()}\n\n";
}

// Shutdown the producer
echo "[Step 5] Shutting Down Lite Producer\n";
echo str_repeat("-", 60) . "\n";

try {
    $producer->shutdown();
    echo "  ✓ Lite producer shutdown successfully\n\n";
    
} catch (\Exception $e) {
    echo "  ✗ Failed to shutdown lite producer: {$e->getMessage()}\n\n";
}

// Summary
echo "========================================\n";
echo "  SUMMARY\n";
echo "========================================\n\n";

echo "Messages Sent: {$sentCount}\n";
echo "Messages Failed: {$failedCount}\n";
echo "\n";

if ($failedCount == 0) {
    echo "✅ All lite messages sent successfully!\n";
} else {
    echo "⚠️  Some messages failed to send.\n";
    echo "   Check the error messages above for details.\n";
}

echo "\nKey Points:\n";
echo "- Lite topics are sub-topics under a parent topic\n";
echo "- They share resources with the parent topic\n";
echo "- Use MessageBuilder::setLiteTopic() to specify lite topic\n";
echo "- Handle LiteTopicQuotaExceededException for quota issues\n";
echo "- Lite producers are simpler than regular producers\n";
