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
 * Producer Delay Message Example
 * 
 * This example demonstrates how to send scheduled/delayed messages using RocketMQ PHP SDK.
 */

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Builder\MessageBuilder;

// Configuration
$endpoints = '127.0.0.1:8080';
$topic = 'topic-delay';
$producerGroup = 'GID-delay-consumer';

echo "=== Producer Delay Message Example ===\n\n";

try {
    // Create client configuration
    $config = new ClientConfiguration($endpoints);
    $config->withSslEnabled(false);
    
    // Create and start producer (second parameter is topic, not group)
    $producer = Producer::getInstance($config, $topic);
    $producer->start();
    echo "✓ Producer started\n";
    echo "  - Topic: {$topic}\n";
    echo "  - Producer Group: {$producerGroup}\n\n";
    
    // Send delay message (deliver after 10 seconds)
    $delaySeconds = 10;
    $body = 'This is a delay message, will be delivered after ' . $delaySeconds . ' seconds';
    $tag = 'delay-tag';
    $keys = 'delay-key-' . time();
    
    echo "Sending delay message ({$delaySeconds} seconds delay)...\n";
    
    // Send delay message
    $receipt = $producer->sendDelayMessage($body, $delaySeconds, $tag, $keys);
    echo "✓ Delay message sent successfully\n";
    echo "  - Message ID: {$receipt->getMessageId()}\n";
    echo "  - Delay: {$delaySeconds} seconds\n";
    echo "  - Will be delivered at: " . date('Y-m-d H:i:s', time() + $delaySeconds) . "\n\n";
    
    // Shutdown producer
    $producer->shutdown();
    echo "✓ Producer shutdown\n";
    
} catch (\Exception $e) {
    echo "✗ Error: " . $e->getMessage() . "\n";
    exit(1);
}

echo "\n=== Example Complete ===\n";
