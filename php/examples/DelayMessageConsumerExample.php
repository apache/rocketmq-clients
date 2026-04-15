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
 * Delay Message Consumer Example
 * 
 * This example demonstrates how to consume delay messages using SimpleConsumer.
 * Delay messages will only be delivered after the specified delay time.
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
$topic = 'topic-delay';
$consumerGroup = 'GID-delay-consumer';

echo "=== Delay Message Consumer Example ===\n\n";
echo "Topic: {$topic}\n";
echo "Consumer Group: {$consumerGroup}\n";
echo "Note: Delay messages will be consumed after their scheduled delivery time.\n\n";

try {
    // Create client configuration
    $config = new ClientConfiguration($endpoints);
    $config->withSslEnabled(false);
    
    // Create and start consumer
    $consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
    $consumer->start();
    echo "✓ Consumer started\n\n";
    
    // Consume messages
    echo "Start consuming delay messages (timeout: 30 seconds)...\n\n";
    
    go(function() use ($consumer) {
        $startTime = time();
        $maxWaitTime = 30;
        $messageCount = 0;
        
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
                        $deliveryTime = $sysProps->getDeliveryTimestamp();
                        
                        echo "✓ Received delay message #{$messageCount}\n";
                        echo "  - Message ID: {$msgId}\n";
                        echo "  - Body: {$body}\n";
                        
                        if ($deliveryTime) {
                            $scheduledTime = $deliveryTime->getSeconds();
                            echo "  - Scheduled Delivery: " . date('Y-m-d H:i:s', $scheduledTime) . "\n";
                            echo "  - Actual Delivery: " . date('Y-m-d H:i:s', time()) . "\n";
                            
                            $delay = time() - $scheduledTime;
                            echo "  - Delay Accuracy: {$delay} seconds\n";
                        }
                        
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
        echo "Total delay messages received: {$messageCount}\n";
        
        if ($messageCount > 0) {
            echo "✓ Test PASSED - Delay messages consumed successfully!\n";
        } else {
            echo "⚠ No delay messages received\n";
            echo "Note: Make sure to send delay messages first using ProducerDelayMessageExample.php\n";
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
