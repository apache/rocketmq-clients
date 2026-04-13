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
 * Producer Normal Message Example
 * 
 * This example demonstrates how to send normal messages using RocketMQ PHP SDK.
 */

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Builder\MessageBuilder;

// Configuration
$endpoints = '127.0.0.1:8080';
$topic = 'topic-php';
$producerGroup = 'GID-php';

echo "=== Producer Normal Message Example ===\n\n";

try {
    // Create client configuration
    $config = new ClientConfiguration($endpoints);
    $config->withSslEnabled(false);
    
    // Create and start producer
    $producer = Producer::getInstance($config, $producerGroup);
    $producer->start();
    echo "✓ Producer started\n\n";
    
    // Define message body
    $body = 'This is a normal message for Apache RocketMQ';
    $tag = 'yourMessageTagA';
    $keys = 'yourMessageKey-' . uniqid();
    
    // Build message
    $msgBuilder = new MessageBuilder();
    $message = $msgBuilder
        ->setTopic($topic)
        ->setBody($body)
        ->setTag($tag)
        ->setKeys([$keys])
        ->build();
    
    // Send message
    echo "Sending message...\n";
    $receipt = $producer->send($message);
    echo "✓ Message sent successfully\n";
    echo "  - Message ID: {$receipt->getMessageId()}\n";
    echo "  - Topic: {$topic}\n";
    echo "  - Tag: {$tag}\n";
    echo "  - Keys: {$keys}\n\n";
    
    // Shutdown producer
    $producer->shutdown();
    echo "✓ Producer shutdown\n";
    
} catch (\Exception $e) {
    echo "✗ Error: " . $e->getMessage() . "\n";
    echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
    exit(1);
}

echo "\n=== Example Complete ===\n";
