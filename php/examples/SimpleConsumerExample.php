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
 * Simple Consumer Example
 * 
 * This example demonstrates how to consume messages using SimpleConsumer (pull mode).
 * SimpleConsumer gives you full control over message consumption.
 */

if (!extension_loaded('swoole')) {
    echo "✗ Swoole extension is required for this example\n";
    exit(1);
}

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../SimpleConsumer.php';
require_once __DIR__ . '/../PushConsumer.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\SimpleConsumer;

// Configuration
$endpoints = '127.0.0.1:8080';
$topic = 'topic-php';
$consumerGroup = 'GID-php';

echo "=== Simple Consumer Example ===\n\n";

try {
    // Create client configuration
    $config = new ClientConfiguration($endpoints);
    $config->withSslEnabled(false);
    
    // Create and start consumer
    $consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
    $consumer->start();
    echo "✓ Consumer started\n\n";
    
    // Consume messages in a loop
    echo "Start consuming messages (press Ctrl+C to stop)...\n\n";
    
    go(function() use ($consumer) {
        $messageCount = 0;
        
        while (true) {
            try {
                // Receive messages (max 10, invisible for 30 seconds)
                $messages = $consumer->receive(10, 30);
                
                if (!empty($messages)) {
                    foreach ($messages as $message) {
                        $messageCount++;
                        $sysProps = $message->getSystemProperties();
                        $msgId = $sysProps->getMessageId();
                        $body = $message->getBody();
                        
                        echo "✓ Received message #{$messageCount}\n";
                        echo "  - Message ID: {$msgId}\n";
                        echo "  - Body: {$body}\n";
                        
                        // Acknowledge the message
                        try {
                            $consumer->ack($message);
                            echo "  - ACK: Success\n\n";
                        } catch (\Exception $e) {
                            echo "  - ACK: Failed - " . $e->getMessage() . "\n\n";
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
