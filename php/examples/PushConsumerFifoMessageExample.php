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
 * PushConsumer FIFO Message Example
 * 
 * This example demonstrates how to consume FIFO (ordered) messages using PushConsumer.
 * Messages in the same message group will be consumed in strict order.
 * Different message groups can be consumed in parallel.
 */

if (!extension_loaded('swoole')) {
    echo "✗ Swoole extension is required for this example\n";
    exit(1);
}

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../SimpleConsumer.php';
require_once __DIR__ . '/../PushConsumer.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\PushConsumer;
use Apache\Rocketmq\Consumer\ConsumeResult;

// Configuration
$endpoints = '127.0.0.1:8080';
$topic = 'topic-order';
$consumerGroup = 'GID-order-consumer';

echo "=== PushConsumer FIFO Message Example ===\n\n";
echo "Topic: {$topic}\n";
echo "Consumer Group: {$consumerGroup}\n";
echo "Note: Messages in the same message group will be consumed in strict order.\n";
echo "      Different message groups can be consumed in parallel.\n\n";

try {
    // Create client configuration
    $config = new ClientConfiguration($endpoints);
    $config->withSslEnabled(false);
    
    // Create PushConsumer
    $consumer = PushConsumer::getInstance($config, $consumerGroup, $topic);
    
    // Configure FIFO consumption
    $consumer->setEnableFifoConsumeAccelerator(true);  // Enable parallel consumption of different groups
    $consumer->setConsumptionThreadCount(10);          // Max 10 concurrent workers for different groups
    $consumer->setMaxCacheMessageCount(512);           // Flow control: max 512 cached messages
    $consumer->setMaxCacheMessageSizeInBytes(32 * 1024 * 1024); // 32MB
    
    // Set message listener
    $consumer->setMessageListener(function($messageView) {
        $messageId = $messageView->getMessageId();
        $body = $messageView->getBody();
        $messageGroup = $messageView->getMessageGroup() ?? 'N/A';
        $tag = $messageView->getTag() ?? 'N/A';
        $keys = implode(',', $messageView->getKeys());
        
        echo "[FIFO Consumer] Received message:\n";
        echo "  - Message ID: {$messageId}\n";
        echo "  - Message Group: {$messageGroup}\n";
        echo "  - Tag: {$tag}\n";
        echo "  - Keys: {$keys}\n";
        echo "  - Body: {$body}\n";
        
        // Simulate processing time
        usleep(100000); // 100ms
        
        // Return ConsumeResult::SUCCESS to ACK the message
        return ConsumeResult::SUCCESS;
    });
    
    echo "✓ PushConsumer configured for FIFO consumption\n";
    echo "  - FIFO Accelerator: Enabled\n";
    echo "  - Consumption Threads: 10\n";
    echo "  - Max Cache Count: 512\n";
    echo "  - Max Cache Size: 32MB\n\n";
    
    // Start consumer (blocking)
    echo "Starting PushConsumer...\n";
    echo "Press Ctrl+C to stop\n\n";
    
    $consumer->start();
    
} catch (\Exception $e) {
    echo "✗ Error: " . $e->getMessage() . "\n";
    echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
    exit(1);
}

echo "\n=== Example Complete ===\n";
