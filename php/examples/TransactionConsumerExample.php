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
 * Transaction Message Consumer Example
 * 
 * This example demonstrates how to consume transactional messages using SimpleConsumer.
 * Only committed transaction messages will be delivered to consumers.
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
$topic = 'topic-php-transcation';
$consumerGroup = 'GID-php-transcation';

echo "=== Transaction Message Consumer Example ===\n\n";
echo "Topic: {$topic}\n";
echo "Consumer Group: {$consumerGroup}\n";
echo "Note: Only committed transaction messages will be consumed.\n\n";

try {
    // Create client configuration
    $config = new ClientConfiguration($endpoints);
    $config->withSslEnabled(false);
    
    // Create and start consumer
    $consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
    $consumer->start();
    echo "✓ Consumer started\n\n";
    
    // Consume messages
    echo "Start consuming transaction messages (timeout: 30 seconds)...\n\n";
    
    go(function() use ($consumer) {
        $startTime = time();
        $maxWaitTime = 30;
        $messageCount = 0;
        $receivedMessages = [];
        
        while ((time() - $startTime) < $maxWaitTime) {
            try {
                // Receive messages (max 10, invisible for 30 seconds)
                $messages = $consumer->receive(10, 30);
                
                if (!empty($messages)) {
                    foreach ($messages as $message) {
                        $messageCount++;
                        $sysProps = $message->getSystemProperties();
                        $msgId = $sysProps->getMessageId();
                        $body = $message->getBody();
                        $tag = $sysProps->getTag() ?: 'N/A';
                        
                        echo "✓ Received transaction message #{$messageCount}\n";
                        echo "  - Message ID: {$msgId}\n";
                        echo "  - Body: {$body}\n";
                        echo "  - Tag: {$tag}\n";
                        
                        // Store message info
                        $receivedMessages[] = [
                            'id' => $msgId,
                            'body' => $body,
                            'tag' => $tag
                        ];
                        
                        // Acknowledge the message
                        try {
                            $consumer->ack($message);
                            echo "  - ACK: Success\n\n";
                        } catch (\Exception $e) {
                            echo "  - ACK: Failed - " . $e->getMessage() . "\n\n";
                        }
                    }
                } else {
                    echo ".";
                    flush();
                    \Swoole\Coroutine::sleep(1);
                }
            } catch (\Exception $e) {
                echo "\n✗ Error: " . $e->getMessage() . "\n";
                \Swoole\Coroutine::sleep(1);
            }
        }
        
        echo "\n\n=== Test Summary ===\n";
        echo "Total transaction messages received: {$messageCount}\n";
        
        if ($messageCount > 0) {
            echo "\nReceived messages:\n";
            foreach ($receivedMessages as $idx => $msg) {
                echo "  " . ($idx + 1) . ". [{$msg['id']}] {$msg['body']}\n";
            }
            echo "\n✓ Test PASSED - Transaction messages consumed successfully!\n";
            echo "Note: These messages were only delivered after being committed.\n";
        } else {
            echo "⚠ No transaction messages received\n";
            echo "Note: Make sure to send transaction messages first using ProducerTransactionMessageExample.php\n";
            echo "      And ensure transactions are committed (not rolled back).\n";
        }
        
        echo "\n=== Example Complete ===\n";
        
        // Close consumer
        $consumer->close();
        echo "✓ Consumer closed\n";
        
        // Exit event loop
        \Swoole\Event::exit();
    });
    
    // Start the event loop
    \Swoole\Event::wait();
    
} catch (\Exception $e) {
    echo "✗ Error: " . $e->getMessage() . "\n";
    exit(1);
}
