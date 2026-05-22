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
 * Async Producer Example with Swoole Coroutine
 * 
 * This example demonstrates how to use Swoole coroutines for async message sending:
 * - Multiple messages sent concurrently using coroutines
 * - Non-blocking I/O operations
 * - Improved throughput compared to synchronous sending
 * 
 * Requirements:
 * - Swoole extension installed
 * - PHP 7.1+
 * 
 * Usage:
 *   php examples/AsyncProducerExample.php
 */

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';
require_once __DIR__ . '/../Logger.php';
require_once __DIR__ . '/ExampleConfig.php';

use Apache\Rocketmq\Producer;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;

// Load configuration
$config = ExampleConfig::getInstance();
$endpoints = $config->getEndpoints();
$topic = $config->getTopic('normal');
$credentials = $config->getCredentials();

// Display configuration
$config->display();

echo "\n[INFO] Starting Async Producer with Swoole Coroutines...\n\n";

// Check if Swoole is available
if (!class_exists('\Swoole\Coroutine')) {
    echo "[ERROR] Swoole extension is not installed!\n";
    echo "Please install Swoole: pecl install swoole\n";
    exit(1);
}

echo "[INFO] Swoole version: " . SWOOLE_VERSION . "\n";
echo "[INFO] Sending 5 messages concurrently using coroutines...\n\n";

// Use Swoole coroutine context
\Swoole\Coroutine\run(function() use ($endpoints, $topic, $credentials) {
    // Create producer
    $producer = new Producer($endpoints, [
        'topics' => [$topic],
        'credentials' => $credentials,
        'maxAttempts' => 3,
        'requestTimeout' => 3000,
    ]);
    
    $producer->start();
    echo "[SUCCESS] Producer started\n\n";
    
    // Track results
    $results = [];
    $channel = new \Swoole\Coroutine\Channel(10);
    
    // Send multiple messages concurrently using coroutines
    $messageCount = 5;
    for ($i = 1; $i <= $messageCount; $i++) {
        \Swoole\Coroutine::create(function() use ($producer, $topic, $i, $channel) {
            try {
                $startTime = microtime(true);
                
                // Build message
                $topicResource = new Resource();
                $topicResource->setName($topic);
                
                $sysProps = new SystemProperties();
                $sysProps->setTag('async-test');
                $sysProps->setKeys(["async-msg-{$i}"]);
                
                $message = new Message();
                $message->setTopic($topicResource);
                $message->setBody("Async message #{$i} - Sent at " . date('Y-m-d H:i:s'));
                $message->setSystemProperties($sysProps);
                
                echo "[Coroutine " . \Swoole\Coroutine::getCid() . "] Sending message #{$i}...\n";
                
                // Send message asynchronously
                $result = $producer->send($message);
                
                $elapsed = round((microtime(true) - $startTime) * 1000, 2);
                
                echo "[Coroutine " . \Swoole\Coroutine::getCid() . "] ✓ Message #{$i} sent successfully\n";
                echo "    Message ID: " . substr($result['messageId'], 0, 30) . "...\n";
                echo "    Time: {$elapsed}ms\n\n";
                
                $channel->push([
                    'index' => $i,
                    'success' => true,
                    'messageId' => $result['messageId'],
                    'elapsed' => $elapsed,
                ]);
                
            } catch (\Throwable $e) {
                echo "[Coroutine " . \Swoole\Coroutine::getCid() . "] ✗ Failed to send message #{$i}: " . $e->getMessage() . "\n\n";
                
                $channel->push([
                    'index' => $i,
                    'success' => false,
                    'error' => $e->getMessage(),
                ]);
            }
        });
    }
    
    // Collect results from all coroutines
    echo "[INFO] Waiting for all coroutines to complete...\n\n";
    
    for ($i = 0; $i < $messageCount; $i++) {
        $result = $channel->pop(5); // 5 second timeout
        if ($result !== false) {
            $results[] = $result;
        }
    }
    
    // Display summary
    echo str_repeat("=", 80) . "\n";
    echo "ASYNC PRODUCER SUMMARY\n";
    echo str_repeat("=", 80) . "\n\n";
    
    $successCount = count(array_filter($results, function($r) { return $r['success']; }));
    $failCount = $messageCount - $successCount;
    
    echo "Total messages: {$messageCount}\n";
    echo "Successful: {$successCount}\n";
    echo "Failed: {$failCount}\n\n";
    
    if (!empty($results)) {
        $avgTime = array_sum(array_column($results, 'elapsed')) / count($results);
        echo "Average time: " . round($avgTime, 2) . "ms per message\n";
    }
    
    echo "\n[NOTE] All messages were sent concurrently using Swoole coroutines\n";
    echo "[NOTE] This provides better throughput than sequential sending\n";
    
    echo "\n" . str_repeat("=", 80) . "\n";
    
    // Shutdown producer
    $producer->shutdown();
    echo "[INFO] Producer shutdown complete\n";
});
