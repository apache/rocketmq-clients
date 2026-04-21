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
 * Priority Message Test Example
 * 
 * Tests priority message send and receive using:
 * - Topic: topic-priority
 * - Consumer Group: GID-priority-consumer
 * 
 * Priority messages are delivered based on their priority level:
 * - Higher priority value = higher priority (e.g., priority 10 > priority 1)
 * - Priority cannot be combined with: deliveryTimestamp, messageGroup, or liteTopic
 * - Priority is only meaningful when the topic is configured as a priority topic
 */

if (!extension_loaded('swoole')) {
    echo "Swoole extension is required for this test\n";
    exit(1);
}

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';
require_once __DIR__ . '/../SimpleConsumer.php';
require_once __DIR__ . '/../ProducerSingleton.php';
require_once __DIR__ . '/../Builder/MessageBuilder.php';
require_once __DIR__ . '/../Builder/SimpleConsumerBuilder.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Builder\SimpleConsumerBuilder;
use Apache\Rocketmq\Consumer\FilterExpression;
use Apache\Rocketmq\Consumer\FilterExpressionType;

// Configuration
$endpoints = '127.0.0.1:8080';
$topic = 'topic-priority';
$consumerGroup = 'GID-priority-consumer';

echo "========================================\n";
echo "  Priority Message Test\n";
echo "========================================\n\n";
echo "Configuration:\n";
echo "  - Topic: {$topic}\n";
echo "  - Consumer Group: {$consumerGroup}\n";
echo "  - Endpoints: {$endpoints}\n\n";

go(function () use ($topic, $consumerGroup, $endpoints) {

    try {
        // ========================================
        // Step 1: Create SimpleConsumer
        // ========================================
        echo "[Step 1] Creating SimpleConsumer...\n";
        echo str_repeat("-", 60) . "\n";

        $config = new ClientConfiguration($endpoints);
        $config->withSslEnabled(false);
        
        // Set subscription expression
        $filterExpression = new FilterExpression('*', FilterExpressionType::TAG);

        // Use Builder pattern
        $consumer = (new SimpleConsumerBuilder())
            ->setClientConfiguration($config)
            ->setConsumerGroup($consumerGroup)
            ->setTopic($topic)
            ->setSubscriptionExpressions([$topic => $filterExpression])
            ->build();
        
        echo "  SimpleConsumer created and started\n";
        echo "  Subscribed to topic: {$topic}\n\n";

        // ========================================
        // Step 2: Send priority messages with different levels
        // ========================================
        echo "[Step 2] Sending priority messages...\n";
        echo str_repeat("-", 60) . "\n";

        $producer = ProducerSingleton::getInstance($topic);

        // Define test messages with different priorities
        $testMessages = [
            ['body' => 'Low priority task', 'priority' => 1, 'tag' => 'low'],
            ['body' => 'Medium priority task', 'priority' => 5, 'tag' => 'medium'],
            ['body' => 'High priority task', 'priority' => 8, 'tag' => 'high'],
            ['body' => 'Critical priority task', 'priority' => 10, 'tag' => 'critical'],
            ['body' => 'Normal priority task', 'priority' => 3, 'tag' => 'normal'],
        ];

        $sentCount = 0;
        foreach ($testMessages as $index => $msgData) {
            try {
                $msgBuilder = new MessageBuilder();
                $message = $msgBuilder
                    ->setTopic($topic)
                    ->setBody($msgData['body'])
                    ->setTag($msgData['tag'])
                    ->setKeys("priority-key-" . ($index + 1))
                    ->setPriority($msgData['priority'])
                    ->build();

                $receipt = $producer->send($message);
                $sentCount++;
                
                echo "  Sent #{$sentCount}: Priority {$msgData['priority']} - {$msgData['body']}\n";
                echo "    Message ID: {$receipt->getMessageId()}\n";
                
                // Small delay between sends
                usleep(100000); // 100ms
            } catch (\Exception $e) {
                echo "  Failed to send #{$sentCount}: " . $e->getMessage() . "\n";
            }
        }
        
        echo "\n  Total sent: {$sentCount} messages\n\n";

        // Wait for messages to be available on broker
        \Swoole\Coroutine::sleep(2);

        // ========================================
        // Step 3: Receive messages
        // ========================================
        echo "[Step 3] Receiving messages via SimpleConsumer.receive()...\n";
        echo str_repeat("-", 60) . "\n";

        $receivedMessages = [];
        $receivedCount = 0;
        $ackedCount = 0;
        $maxAttempts = 5;

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            echo "  Attempt #{$attempt}: pulling messages...\n";

            try {
                $messages = $consumer->receive(32, 15);

                if (empty($messages)) {
                    echo "    No messages received, waiting...\n";
                    \Swoole\Coroutine::sleep(2);
                    continue;
                }

                echo "    Got " . count($messages) . " messages\n";

                foreach ($messages as $message) {
                    $receivedCount++;
                    $sysProps = $message->getSystemProperties();
                    $msgId = $sysProps->getMessageId();
                    $body = $message->getBody();
                    if (!is_string($body)) {
                        $body = method_exists($body, 'toString') ? $body->toString() : (string)$body;
                    }
                    
                    // Get priority from message
                    $priority = $message->getPriority();
                    $priorityStr = ($priority !== null) ? (string)$priority : 'N/A';

                    echo "    [{$receivedCount}] Priority: {$priorityStr}\n";
                    echo "         ID: {$msgId}\n";
                    echo "         Body: {$body}\n";

                    // Store for analysis
                    $receivedMessages[] = [
                        'priority' => $priority,
                        'body' => $body,
                        'messageId' => $msgId
                    ];

                    // ACK message
                    try {
                        $consumer->ack($message);
                        $ackedCount++;
                        echo "         ACK: OK\n\n";
                    } catch (\Exception $e) {
                        echo "         ACK: FAILED - " . $e->getMessage() . "\n\n";
                    }
                }

                // If we got enough messages, stop early
                if ($receivedCount >= $sentCount) {
                    break;
                }
            } catch (\Exception $e) {
                echo "    Error: " . $e->getMessage() . "\n";
                \Swoole\Coroutine::sleep(1);
            }
        }

        echo "\n";

        // ========================================
        // Step 4: Analyze results
        // ========================================
        echo "[Step 4] Results Analysis\n";
        echo str_repeat("-", 60) . "\n";
        echo "  Sent:     {$sentCount}\n";
        echo "  Received: {$receivedCount}\n";
        echo "  ACKed:    {$ackedCount}\n\n";

        // Display received messages in order
        if (!empty($receivedMessages)) {
            echo "  Received Messages Order:\n";
            foreach ($receivedMessages as $index => $msg) {
                echo "    " . ($index + 1) . ". Priority: {$msg['priority']} - {$msg['body']}\n";
            }
            echo "\n";
        }

        // Verify test results
        if ($receivedCount >= $sentCount) {
            echo "TEST PASSED - Priority messages received successfully!\n";
            
            // Check if all priorities were received
            $priorities = array_column($receivedMessages, 'priority');
            $uniquePriorities = array_unique($priorities);
            echo "  Unique priorities received: " . implode(', ', $uniquePriorities) . "\n";
        } elseif ($receivedCount > 0) {
            echo "TEST PARTIAL - Received some messages ({$receivedCount}/{$sentCount})\n";
        } else {
            echo "TEST FAILED - No messages received\n";
        }

        echo "\n";

        // Cleanup
        echo "[Cleanup] Shutting down...\n";
        $consumer->close();
        echo "  SimpleConsumer shutdown\n";

    } catch (\Exception $e) {
        echo "\nERROR: " . $e->getMessage() . "\n";
        echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
        exit(1);
    }

    echo "\n========================================\n";
    echo "  TEST COMPLETE\n";
    echo "========================================\n";
});
