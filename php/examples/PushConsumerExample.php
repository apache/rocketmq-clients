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
 * Push Consumer Example
 * 
 * This example demonstrates how to consume messages using PushConsumer pattern.
 * PushConsumer automatically receives messages and calls your message listener.
 */

if (!extension_loaded('swoole')) {
    echo "✗ Swoole extension is required for this example\n";
    exit(1);
}

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Consumer.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\SimpleConsumer;

// Configuration
$endpoints = '127.0.0.1:8080';
$topic = 'topic-php';
$consumerGroup = 'GID-php';

echo "=== Push Consumer Example ===\n\n";

try {
    // Create client configuration
    $config = new ClientConfiguration($endpoints);
    $config->withSslEnabled(false);
    
    // Create consumer
    $consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
    $consumer->start();
    echo "✓ Consumer started\n\n";
    
    // Define message listener (callback function)
    $messageListener = function($message) {
        $sysProps = $message->getSystemProperties();
        $msgId = $sysProps->getMessageId();
        $body = $message->getBody();
        $tag = $sysProps->getTag() ?: 'N/A';
        
        echo "\n📨 Received message\n";
        echo "   Message ID: {$msgId}\n";
        echo "   Body: {$body}\n";
        echo "   Tag: {$tag}\n";
        
        // Process your business logic here
        // ...
        
        return true; // Return true for success
    };
    
    echo "Start consuming messages with listener (press Ctrl+C to stop)...\n\n";
    
    // Start consuming in coroutine (simulating PushConsumer)
    go(function() use ($consumer, $messageListener) {
        while (true) {
            try {
                // Poll for messages
                $messages = $consumer->receive(10, 30);
                
                if (!empty($messages)) {
                    foreach ($messages as $message) {
                        // Call message listener
                        $result = $messageListener($message);
                        
                        // Auto ACK on success
                        if ($result === true) {
                            try {
                                $consumer->ack($message);
                                echo "   ✓ ACK: Success\n";
                            } catch (\Exception $e) {
                                echo "   ✗ ACK: Failed - " . $e->getMessage() . "\n";
                            }
                        }
                    }
                } else {
                    // No messages, wait a bit
                    \Swoole\Coroutine::sleep(1);
                }
            } catch (\Exception $e) {
                echo "✗ Error: " . $e->getMessage() . "\n";
                \Swoole\Coroutine::sleep(1);
            }
        }
    });
    
    // Start the event loop
    \Swoole\Event::wait();
    
} catch (\Exception $e) {
    echo "✗ Error: " . $e->getMessage() . "\n";
    exit(1);
}
