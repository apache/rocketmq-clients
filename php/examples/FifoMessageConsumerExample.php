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
 * FIFO Message Consumer Example
 * 
 * This example demonstrates how to consume FIFO (ordered) messages using SimpleConsumer.
 * Messages in the same message group will be consumed in strict order.
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
$topic = 'topic-order';
$consumerGroup = 'GID-order-consumer';

echo "=== FIFO Message Consumer Example ===\n\n";
echo "Topic: {$topic}\n";
echo "Consumer Group: {$consumerGroup}\n";
echo "Note: Messages in the same message group will be consumed in strict order.\n\n";

try {
    // Create client configuration
    $config = new ClientConfiguration($endpoints);
    $config->withSslEnabled(false);
    
    // Create and start consumer
    $consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
    $consumer->start();
    echo "✓ Consumer started\n\n";
    
    // Consume messages
    echo "Start consuming FIFO messages (timeout: 30 seconds)...\n\n";
    
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
                        $messageGroup = $sysProps->getMessageGroup() ?: 'N/A';
                        
                        echo "✓ Received FIFO message #{$messageCount}\n";
                        echo "  - Message ID: {$msgId}\n";
                        echo "  - Body: {$body}\n";
                        echo "  - Message Group: {$messageGroup}\n";
                        
                        // Store message info for order verification
                        $receivedMessages[] = [
                            'id' => $msgId,
                            'body' => $body,
                            'group' => $messageGroup,
                            'sequence' => $messageCount
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
        echo "Total FIFO messages received: {$messageCount}\n";
        
        if ($messageCount > 0) {
            echo "\nReceived messages in order:\n";
            foreach ($receivedMessages as $msg) {
                echo "  {$msg['sequence']}. [{$msg['id']}] Group: {$msg['group']} - {$msg['body']}\n";
            }
            
            // Verify order within same message group
            $groups = [];
            foreach ($receivedMessages as $msg) {
                if ($msg['group'] !== 'N/A') {
                    if (!isset($groups[$msg['group']])) {
                        $groups[$msg['group']] = [];
                    }
                    $groups[$msg['group']][] = $msg;
                }
            }
            
            echo "\nOrder verification by message group:\n";
            $orderCorrect = true;
            foreach ($groups as $group => $msgs) {
                echo "  Group '{$group}': ";
                if (count($msgs) > 1) {
                    // Check if sequences are in order
                    $sequences = array_column($msgs, 'sequence');
                    $isOrdered = true;
                    for ($i = 1; $i < count($sequences); $i++) {
                        if ($sequences[$i] <= $sequences[$i - 1]) {
                            $isOrdered = false;
                            break;
                        }
                    }
                    
                    if ($isOrdered) {
                        echo "✓ Order preserved (" . count($msgs) . " messages)\n";
                    } else {
                        echo "✗ Order NOT preserved!\n";
                        $orderCorrect = false;
                    }
                } else {
                    echo "✓ Single message\n";
                }
            }
            
            if ($orderCorrect) {
                echo "\n✓ Test PASSED - FIFO messages consumed in correct order!\n";
            } else {
                echo "\n✗ Test FAILED - Message order was not preserved!\n";
            }
        } else {
            echo "⚠ No FIFO messages received\n";
            echo "Note: Make sure to send FIFO messages first using ProducerFifoMessageExample.php\n";
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
